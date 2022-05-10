package sarama

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrClosedConsumerGroup is the error returned when a method is called on a consumer group that has been closed.
var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")

// FIXME: BY SUN
// 1. 编程模式中的"依赖倒置"原则：
// 这里，所有的类都是用interface接口的形式对外提供服务的；
// 所有的结构体，都是小写的，对外不可见，需要对外可见的，提供的是 New方法。

// 2. 这个文件中ConsumerGroup的核心代码逻辑:
// 2.1 消费者加入到一个给定topics的组，并启动一个阻塞的Session，贯穿整个ConsumerGroupHandler的处理过程
// 2.2 会话的生命周期：
//      (a) 消费者加入 组中，之后会被公平分配给他 partitions，此时，消费者被称作 "claims" ==> 貌似可以理解为一个 “小型的应用”;
//      (b) 开始启动处理前，会调用用户自定义的Setup()函数；
//      (c) 对于每一个消费者“claims”，都会在一个单独的协程中，调用线程安全的ConsumeClaim函数；
//      (d) session退出的时候，通常是某一个ConsumeClaim退出的时候，通常是由于被父协程终止或者 是服务端重新负载均衡导致的;
//      (e) 所有的ConsumeClaim退出之后，Cleanup会被调用，用于业务自定义清理操作;
//      (f) session被释放之前，偏移量最后会被提交一次。

// 3.核心对象
// 3.1 ConsumerGroupHandler
//     这个是消费者处理函数，是用于给业务留的核心的，业务自定义的 处理接口。主要留了三个接口，session构建之前的预处理函数Setup，建立之后的处理函数，和清理之前的析构函数Cleanup。
//     这种设计方式以后可以使用，通过接口的形式给业务提供接入方式！！
// 3.2 ConsumerGroupSession
//     这个是Session消费组中，单个成员的会话现场保存。
//     貌似主要提供了 消费者在消费过程中，对偏移量的管理细节。
// 3.3 ConsumerGroupClaim
//     这个貌似就是提供真实的消息消费的方法。
// 3.4 ConsumerGroup
//     这个貌似是真的消费者组，对用于提供的接口调用。

// ConsumerGroup is responsible for dividing up processing of topics and partitions
// over a collection of processes (the members of the consumer group).
type ConsumerGroup interface {
	// Consume joins a cluster of consumers for a given list of topics and
	// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
	//
	// The life-cycle of a session is represented by the following steps:
	//
	// 1. The consumers join the group (as explained in https://kafka.apache.org/documentation/#intro_consumers)
	//    and is assigned their "fair share" of partitions, aka 'claims'.
	// 2. Before processing starts, the handler's Setup() hook is called to notify the user
	//    of the claims and allow any necessary preparation or alteration of state.
	// 3. For each of the assigned claims the handler's ConsumeClaim() function is then called
	//    in a separate goroutine which requires it to be thread-safe. Any state must be carefully protected
	//    from concurrent reads/writes.
	// 4. The session will persist until one of the ConsumeClaim() functions exits. This can be either when the
	//    parent context is cancelled or when a server-side rebalance cycle is initiated.
	// 5. Once all the ConsumeClaim() loops have exited, the handler's Cleanup() hook is called
	//    to allow the user to perform any final tasks before a rebalance.
	// 6. Finally, marked offsets are committed one last time before claims are released.
	//
	// Please note, that once a rebalance is triggered, sessions must be completed within
	// Config.Consumer.Group.Rebalance.Timeout. This means that ConsumeClaim() functions must exit
	// as quickly as possible to allow time for Cleanup() and the final offset commit. If the timeout
	// is exceeded, the consumer will be removed from the group by Kafka, which will cause offset
	// commit failures.
	// This method should be called inside an infinite loop, when a
	// server-side rebalance happens, the consumer session will need to be
	// recreated to get the new claims.
	Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error

	// Errors returns a read channel of errors that occurred during the consumer life-cycle.
	// By default, errors are logged and not returned over this channel.
	// If you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan error

	// Close stops the ConsumerGroup and detaches any running sessions. It is required to call
	// this function before the object passes out of scope, as it will otherwise leak memory.
	Close() error
}

type consumerGroup struct {
	client Client

	config   *Config
	consumer Consumer
	groupID  string // 这个是topic的名字
	memberID string // 这个是成员id，这个是动态的，每次动态分配，重启后，协调者不用关注每个动态加入的消费者的唯一ID到底是啥！！
	errors   chan error

	lock      sync.Mutex
	closed    chan none
	closeOnce sync.Once

	userData []byte
}

// NewConsumerGroup creates a new consumer group the given broker addresses and configuration.
func NewConsumerGroup(addrs []string, groupID string, config *Config) (ConsumerGroup, error) {
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	c, err := newConsumerGroup(groupID, client)
	if err != nil {
		_ = client.Close()
	}
	return c, err
}

// NewConsumerGroupFromClient creates a new consumer group using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
// PLEASE NOTE: consumer groups can only re-use but not share clients.
func NewConsumerGroupFromClient(groupID string, client Client) (ConsumerGroup, error) {
	// For clients passed in by the client, ensure we don't
	// call Close() on it.
	cli := &nopCloserClient{client}
	return newConsumerGroup(groupID, cli)
}

func newConsumerGroup(groupID string, client Client) (ConsumerGroup, error) {
	config := client.Config()
	if !config.Version.IsAtLeast(V0_10_2_0) {
		return nil, ConfigurationError("consumer groups require Version to be >= V0_10_2_0")
	}

	consumer, err := NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &consumerGroup{
		client:   client,
		consumer: consumer,
		config:   config,
		groupID:  groupID,
		errors:   make(chan error, config.ChannelBufferSize),
		closed:   make(chan none),
	}, nil
}

// Errors implements ConsumerGroup.
func (c *consumerGroup) Errors() <-chan error { return c.errors }

// Close implements ConsumerGroup.
func (c *consumerGroup) Close() (err error) {
	c.closeOnce.Do(func() {
		close(c.closed)

		// leave group
		if e := c.leave(); e != nil {
			err = e
		}

		// drain errors
		go func() {
			close(c.errors)
		}()
		for e := range c.errors {
			err = e
		}

		if e := c.client.Close(); e != nil {
			err = e
		}
	})
	return
}

// 消费的核心入口逻辑：// 业务调用方会卡在这个函数中
// （1）更新所有的topics的元数据信息
// （2）初始化session会话
// （3）异步监测&校验分区数目变化
// （4）等待session结束，结束后清理session

// Consume implements ConsumerGroup.
func (c *consumerGroup) Consume(ctx context.Context, topics []string, handler ConsumerGroupHandler) error {
	// Ensure group is not closed
	select {
	case <-c.closed:
		return ErrClosedConsumerGroup
	default:
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Quick exit when no topics are provided
	if len(topics) == 0 {
		return fmt.Errorf("no topics provided")
	}

	// 在初始化Client的参数的时候，实际上就已经开启了一个协程，会周期性对账检查topics的元数据更新的情况；
	// 核心代码在client中，提供与服务端broker的各种操作能力！！

	// Refresh metadata for requested topics
	if err := c.client.RefreshMetadata(topics...); err != nil {
		return err
	}

	// 初始化session是整个消费的核心流程：

	// Init session
	sess, err := c.newSession(ctx, topics, handler, c.config.Consumer.Group.Rebalance.Retry.Max)
	if err == ErrClosedClient {
		return ErrClosedConsumerGroup
	} else if err != nil {
		return err
	}

	// loop check topic partition numbers changed
	// will trigger rebalance when any topic partitions number had changed
	// avoid Consume function called again that will generate more than loopCheckPartitionNumbers coroutine
	go c.loopCheckPartitionNumbers(topics, sess)

	// Wait for session exit signal
	<-sess.ctx.Done()

	// Gracefully release session claims
	return sess.release(true)
}

func (c *consumerGroup) retryNewSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int, refreshCoordinator bool) (*consumerGroupSession, error) {
	select {
	case <-c.closed:
		return nil, ErrClosedConsumerGroup
	case <-time.After(c.config.Consumer.Group.Rebalance.Retry.Backoff):
	}

	if refreshCoordinator {
		err := c.client.RefreshCoordinator(c.groupID)
		if err != nil {
			return c.retryNewSession(ctx, topics, handler, retries, true)
		}
	}

	return c.newSession(ctx, topics, handler, retries-1)
}

func (c *consumerGroup) newSession(ctx context.Context, topics []string, handler ConsumerGroupHandler, retries int) (*consumerGroupSession, error) {
	// 协调组文档：https://blog.csdn.net/liyiming2017/article/details/82867765
	// 查找协调者broker，也分为组协调器(协调功能svr，位于broker端) & 消费者协调器(协调功能client，位于client端)
	// 关于协调者，实际上是代理以前zookeeper的，用来保存的是 整个kafka集群中，这个topic的元数据信息：
	// （1）有哪些消费者： 消费者的加入、消费者的退出、消费者的心跳保持
	// （2）每个消费者消费的偏移量等
	// （3）leader消费者的分配，leader消费者主要的职责，就是给其他的分配者分配拥有的分区，并将结构汇报给 协调者 【不知道为啥有leader消费者这个代理？】貌似说是会减小协调组的压力！！
	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		if retries <= 0 {
			return nil, err
		}

		return c.retryNewSession(ctx, topics, handler, retries, true)
	}

	// 这个就是消费者协调器的一个功能，就是加入组协调器，这样，kafka系统才知道有哪些消费者连着他，才能为后续的各种分配工作搞进展！！
	// Join.MemberId

	// Join consumer group
	join, err := c.joinGroupRequest(coordinator, topics)
	if err != nil {
		_ = coordinator.Close()
		return nil, err
	}
	switch join.Err {
	case ErrNoError:
		c.memberID = join.MemberId // 记住协调组给自己编码的唯一的ID (重要)
	case ErrUnknownMemberId, ErrIllegalGeneration: // reset member ID and retry immediately
		c.memberID = ""
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer: // retry after backoff with coordinator refresh
		if retries <= 0 {
			return nil, join.Err
		}

		return c.retryNewSession(ctx, topics, handler, retries, true)
	case ErrRebalanceInProgress: // retry after backoff
		if retries <= 0 {
			return nil, join.Err
		}

		return c.retryNewSession(ctx, topics, handler, retries, false)
	default:
		return nil, join.Err
	}

	// 到这里，消费者就已经成功加入到了协调者了，此时之后，就需要进行 消费者进行分配了，
	// 分配计划是 leader分配的，说的是为了避免 负载有问题！！

	// by sun: 已经加入成功这个group，并且，成为了这个group中的一个leader;于是，需要制定消费方案  ==> 轮询？hash？
	// ==> 也就是，如何将这些消费topic分配给各个消费组使用？
	// 各个消费组的使用计划，是由消费组中的leader进行指定的。指定的依赖 {topic的partiotion分布，group的consumer分布, 用户指定的消费策略}
	// Prepare distribution plan if we joined as the leader
	var plan BalanceStrategyPlan
	if join.LeaderId == join.MemberId {
		members, err := join.GetMembers()
		if err != nil {
			return nil, err
		}

		// plan 本身也是 接口开放与封闭的原则!!
		plan, err = c.balance(members)
		if err != nil {
			return nil, err
		}
	}

	// 同步生成的计划信息给 协调者。
	// (1) leader 同步的是 有值 的 plan
	// (2) 非leader 同步的则是空的 plan
	// Sync consumer group
	groupRequest, err := c.syncGroupRequest(coordinator, plan, join.GenerationId)
	if err != nil {
		_ = coordinator.Close()
		return nil, err
	}
	switch groupRequest.Err {
	case ErrNoError:
	case ErrUnknownMemberId, ErrIllegalGeneration: // reset member ID and retry immediately
		c.memberID = ""
		return c.newSession(ctx, topics, handler, retries)
	case ErrNotCoordinatorForConsumer: // retry after backoff with coordinator refresh
		if retries <= 0 {
			return nil, groupRequest.Err
		}

		return c.retryNewSession(ctx, topics, handler, retries, true)
	case ErrRebalanceInProgress: // retry after backoff
		if retries <= 0 {
			return nil, groupRequest.Err
		}

		return c.retryNewSession(ctx, topics, handler, retries, false)
	default:
		return nil, groupRequest.Err
	}

	// 协调者拉取到对应的 partition的分配之后，将这些partition分配给每一个消费者，这个就是分配的结果
	// Retrieve and sort claims
	var claims map[string][]int32
	if len(groupRequest.MemberAssignment) > 0 {
		members, err := groupRequest.GetMemberAssignment()
		if err != nil {
			return nil, err
		}
		claims = members.Topics
		c.userData = members.UserData

		for _, partitions := range claims {
			sort.Sort(int32Slice(partitions))
		}
	}

	// 到此，用户就拿到了自己的 分配的所有的topic对应的partition值了！！
	// 之后，就需要创建消费单元，来消费每一个partition中的数据！！
	return newConsumerGroupSession(ctx, c, claims, join.MemberId, join.GenerationId, handler)
}

func (c *consumerGroup) joinGroupRequest(coordinator *Broker, topics []string) (*JoinGroupResponse, error) {
	req := &JoinGroupRequest{
		GroupId:        c.groupID,
		MemberId:       c.memberID,
		SessionTimeout: int32(c.config.Consumer.Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}
	if c.config.Version.IsAtLeast(V0_10_1_0) {
		req.Version = 1
		req.RebalanceTimeout = int32(c.config.Consumer.Group.Rebalance.Timeout / time.Millisecond)
	}

	// use static user-data if configured, otherwise use consumer-group userdata from the last sync
	userData := c.config.Consumer.Group.Member.UserData
	if len(userData) == 0 {
		userData = c.userData
	}
	meta := &ConsumerGroupMemberMetadata{
		Topics:   topics,
		UserData: userData,
	}
	strategy := c.config.Consumer.Group.Rebalance.Strategy
	if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
		return nil, err
	}

	return coordinator.JoinGroup(req)
}

func (c *consumerGroup) syncGroupRequest(coordinator *Broker, plan BalanceStrategyPlan, generationID int32) (*SyncGroupResponse, error) {
	req := &SyncGroupRequest{
		GroupId:      c.groupID,
		MemberId:     c.memberID,
		GenerationId: generationID,
	}
	strategy := c.config.Consumer.Group.Rebalance.Strategy
	for memberID, topics := range plan {
		assignment := &ConsumerGroupMemberAssignment{Topics: topics}
		userDataBytes, err := strategy.AssignmentData(memberID, topics, generationID)
		if err != nil {
			return nil, err
		}
		assignment.UserData = userDataBytes
		if err := req.AddGroupAssignmentMember(memberID, assignment); err != nil {
			return nil, err
		}
	}
	return coordinator.SyncGroup(req)
}

func (c *consumerGroup) heartbeatRequest(coordinator *Broker, memberID string, generationID int32) (*HeartbeatResponse, error) {
	req := &HeartbeatRequest{
		GroupId:      c.groupID,
		MemberId:     memberID,
		GenerationId: generationID,
	}

	return coordinator.Heartbeat(req)
}

func (c *consumerGroup) balance(members map[string]ConsumerGroupMemberMetadata) (BalanceStrategyPlan, error) {
	topics := make(map[string][]int32)
	for _, meta := range members {
		for _, topic := range meta.Topics {
			topics[topic] = nil
		}
	}

	for topic := range topics {
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		topics[topic] = partitions
	}

	strategy := c.config.Consumer.Group.Rebalance.Strategy
	return strategy.Plan(members, topics)
}

// Leaves the cluster, called by Close.
func (c *consumerGroup) leave() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.memberID == "" {
		return nil
	}

	coordinator, err := c.client.Coordinator(c.groupID)
	if err != nil {
		return err
	}

	resp, err := coordinator.LeaveGroup(&LeaveGroupRequest{
		GroupId:  c.groupID,
		MemberId: c.memberID,
	})
	if err != nil {
		_ = coordinator.Close()
		return err
	}

	// Unset memberID
	c.memberID = ""

	// Check response
	switch resp.Err {
	case ErrRebalanceInProgress, ErrUnknownMemberId, ErrNoError:
		return nil
	default:
		return resp.Err
	}
}

func (c *consumerGroup) handleError(err error, topic string, partition int32) {
	if _, ok := err.(*ConsumerError); !ok && topic != "" && partition > -1 {
		err = &ConsumerError{
			Topic:     topic,
			Partition: partition,
			Err:       err,
		}
	}

	if !c.config.Consumer.Return.Errors {
		Logger.Println(err)
		return
	}

	select {
	case <-c.closed:
		//consumer is closed
		return
	default:
	}

	select {
	case c.errors <- err:
	default:
		// no error listener
	}
}

func (c *consumerGroup) loopCheckPartitionNumbers(topics []string, session *consumerGroupSession) {
	pause := time.NewTicker(c.config.Metadata.RefreshFrequency)
	defer session.cancel()
	defer pause.Stop()
	var oldTopicToPartitionNum map[string]int
	var err error
	if oldTopicToPartitionNum, err = c.topicToPartitionNumbers(topics); err != nil {
		return
	}
	for {
		if newTopicToPartitionNum, err := c.topicToPartitionNumbers(topics); err != nil {
			return
		} else {
			for topic, num := range oldTopicToPartitionNum {
				if newTopicToPartitionNum[topic] != num {
					return // trigger the end of the session on exit
				}
			}
		}
		select {
		case <-pause.C:
		case <-session.ctx.Done():
			Logger.Printf("loop check partition number coroutine will exit, topics %s", topics)
			// if session closed by other, should be exited
			return
		case <-c.closed:
			return
		}
	}
}

func (c *consumerGroup) topicToPartitionNumbers(topics []string) (map[string]int, error) {
	topicToPartitionNum := make(map[string]int, len(topics))
	for _, topic := range topics {
		if partitionNum, err := c.client.Partitions(topic); err != nil {
			Logger.Printf("Consumer Group topic %s get partition number failed %v", topic, err)
			return nil, err
		} else {
			topicToPartitionNum[topic] = len(partitionNum)
		}
	}
	return topicToPartitionNum, nil
}

// --------------------------------------------------------------------

// ConsumerGroupSession represents a consumer group member session.
type ConsumerGroupSession interface {
	// Claims returns information about the claimed partitions by topic.
	Claims() map[string][]int32

	// MemberID returns the cluster member ID.
	MemberID() string

	// GenerationID returns the current generation ID.
	GenerationID() int32

	// MarkOffset marks the provided offset, alongside a metadata string
	// that represents the state of the partition consumer at that point in time. The
	// metadata string can be used by another consumer to restore that state, so it
	// can resume consumption.
	//
	// To follow upstream conventions, you are expected to mark the offset of the
	// next message to read, not the last message read. Thus, when calling `MarkOffset`
	// you should typically add one to the offset of the last consumed message.
	//
	// Note: calling MarkOffset does not necessarily commit the offset to the backend
	// store immediately for efficiency reasons, and it may never be committed if
	// your application crashes. This means that you may end up processing the same
	// message twice, and your processing should ideally be idempotent.
	MarkOffset(topic string, partition int32, offset int64, metadata string)

	// Commit the offset to the backend
	//
	// Note: calling Commit performs a blocking synchronous operation.
	Commit()

	// ResetOffset resets to the provided offset, alongside a metadata string that
	// represents the state of the partition consumer at that point in time. Reset
	// acts as a counterpart to MarkOffset, the difference being that it allows to
	// reset an offset to an earlier or smaller value, where MarkOffset only
	// allows incrementing the offset. cf MarkOffset for more details.
	ResetOffset(topic string, partition int32, offset int64, metadata string)

	// MarkMessage marks a message as consumed.
	MarkMessage(msg *ConsumerMessage, metadata string)

	// Context returns the session context.
	Context() context.Context
}

type consumerGroupSession struct {
	parent       *consumerGroup
	memberID     string
	generationID int32
	handler      ConsumerGroupHandler

	claims  map[string][]int32
	offsets *offsetManager
	ctx     context.Context
	cancel  func()

	waitGroup       sync.WaitGroup
	releaseOnce     sync.Once
	hbDying, hbDead chan none
}

// 对于每一个topic分配的partition，构建对应的协程去消费他!!
func newConsumerGroupSession(ctx context.Context, parent *consumerGroup, claims map[string][]int32, memberID string, generationID int32, handler ConsumerGroupHandler) (*consumerGroupSession, error) {
	// init offset manager
	offsets, err := newOffsetManagerFromClient(parent.groupID, memberID, generationID, parent.client)
	if err != nil {
		return nil, err
	}

	// init context
	ctx, cancel := context.WithCancel(ctx)

	// init session
	sess := &consumerGroupSession{
		parent:       parent,
		memberID:     memberID,
		generationID: generationID,
		handler:      handler,
		offsets:      offsets,
		claims:       claims,
		ctx:          ctx,
		cancel:       cancel,
		hbDying:      make(chan none),
		hbDead:       make(chan none),
	}

	// 同一个topic一定治好啊对应的一个协调者； 对于每一个topic，每个客户端和协调者保持心跳，这样才能让别人了解掉这个消费者还活着！！
	// 一个group可能好几个topic，这样就能对应一个协调者了！！【注意理解这句话！！】
	// start heartbeat loop
	go sess.heartbeatLoop()

	//
	// create a POM for each claim
	for topic, partitions := range claims {
		for _, partition := range partitions {

			// 计算，并获取每一topic的每一个partition的偏移量
			pom, err := offsets.ManagePartition(topic, partition)
			if err != nil {
				_ = sess.release(false)
				return nil, err
			}

			// 处理偏移量的错误 [TODO: 搞清楚为啥这个代码中这么热衷于这样捕获错误？？] ==> 这种错误处理方式是否有可以借鉴之处！！
			// handle POM errors
			go func(topic string, partition int32) {
				for err := range pom.Errors() {
					sess.parent.handleError(err, topic, partition)
				}
			}(topic, partition)
		}
	}

	// 这个就是具体的接收消息前，进行的操作，就是用户自定义的setup函数
	// perform setup
	if err := handler.Setup(sess); err != nil {
		_ = sess.release(true)
		return nil, err
	}

	// 这里，就开始消费了，对于每一个topic的每一个partition，开启一个协程进行消费了！！
	// start consuming
	for topic, partitions := range claims {
		for _, partition := range partitions {
			sess.waitGroup.Add(1)

			go func(topic string, partition int32) {
				defer sess.waitGroup.Done()

				// cancel the as session as soon as the first
				// goroutine exits
				defer sess.cancel()

				// 每一个partition，开启一个协程进行消费!!
				// consume a single topic/partition, blocking
				sess.consume(topic, partition)
			}(topic, partition)
		}
	}
	return sess, nil
}

func (s *consumerGroupSession) Claims() map[string][]int32 { return s.claims }
func (s *consumerGroupSession) MemberID() string           { return s.memberID }
func (s *consumerGroupSession) GenerationID() int32        { return s.generationID }

func (s *consumerGroupSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.MarkOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) Commit() {
	s.offsets.Commit()
}

func (s *consumerGroupSession) ResetOffset(topic string, partition int32, offset int64, metadata string) {
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		pom.ResetOffset(offset, metadata)
	}
}

func (s *consumerGroupSession) MarkMessage(msg *ConsumerMessage, metadata string) {
	s.MarkOffset(msg.Topic, msg.Partition, msg.Offset+1, metadata)
}

func (s *consumerGroupSession) Context() context.Context {
	return s.ctx
}

func (s *consumerGroupSession) consume(topic string, partition int32) {
	// quick exit if rebalance is due
	select {
	case <-s.ctx.Done():
		return
	case <-s.parent.closed:
		return
	default:
	}

	// 实际上就是从前面 通过rpc拿到的偏移量中，获取这个partition自己的偏移来那个.
	// by sun: 能找到，就强制使用最新的offset
	// get next offset
	offset := s.parent.config.Consumer.Offsets.Initial
	if pom := s.offsets.findPOM(topic, partition); pom != nil {
		offset, _ = pom.NextOffset()
	}

	// 这里也是相当重要的代码逻辑。
	// create new claim
	// ==> by sun:  这里面，会针对partition开单独的协程，每一个协程对应一个partion去进行消费。消费的内容放到一个chan中，对外提供队列式消费功能!!
	// ==> fixme:  需要注意的是，这里是chan的单通道，也就是，这里看起来，没有所谓的批量消费多少的功能。一次就是取一个。没有消费就会阻塞起来！！
	claim, err := newConsumerGroupClaim(s, topic, partition, offset)
	if err != nil {
		s.parent.handleError(err, topic, partition)
		return
	}

	// handle errors
	go func() {
		for err := range claim.Errors() {
			s.parent.handleError(err, topic, partition)
		}
	}()

	// trigger close when session is done
	go func() {
		select {
		case <-s.ctx.Done():
		case <-s.parent.closed:
		}
		claim.AsyncClose()
	}()

	// 这里是用户自定义逻辑的消费的核心：
	// 用户通常会自定义 ConsumeClaim函数， 这个函数中，通常会消费消息，并进行处理；之后上报偏移来那个。 【很重要的逻辑】

	// start processing  // 消费者的一个协程将数据放入到了 Messages中，这里只用从中消费！！ chan队列！！
	if err := s.handler.ConsumeClaim(s, claim); err != nil { // by sun 这个hander就是需要处理的内容！！，用户自定义实现的函数
		s.parent.handleError(err, topic, partition)
	}

	// ensure consumer is closed & drained
	claim.AsyncClose()
	for _, err := range claim.waitClosed() {
		s.parent.handleError(err, topic, partition)
	}
}

func (s *consumerGroupSession) release(withCleanup bool) (err error) {
	// signal release, stop heartbeat
	s.cancel()

	// wait for consumers to exit
	s.waitGroup.Wait()

	// perform release
	s.releaseOnce.Do(func() {
		if withCleanup {
			if e := s.handler.Cleanup(s); e != nil {
				s.parent.handleError(e, "", -1)
				err = e
			}
		}

		if e := s.offsets.Close(); e != nil {
			err = e
		}

		close(s.hbDying)
		<-s.hbDead
	})

	return
}

func (s *consumerGroupSession) heartbeatLoop() {
	defer close(s.hbDead)
	defer s.cancel() // trigger the end of the session on exit

	pause := time.NewTicker(s.parent.config.Consumer.Group.Heartbeat.Interval)
	defer pause.Stop()

	retries := s.parent.config.Metadata.Retry.Max
	for {
		coordinator, err := s.parent.client.Coordinator(s.parent.groupID)
		if err != nil {
			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}

			select {
			case <-s.hbDying:
				return
			case <-time.After(s.parent.config.Metadata.Retry.Backoff):
				retries--
			}
			continue
		}

		resp, err := s.parent.heartbeatRequest(coordinator, s.memberID, s.generationID)
		if err != nil {
			_ = coordinator.Close()

			if retries <= 0 {
				s.parent.handleError(err, "", -1)
				return
			}

			retries--
			continue
		}

		switch resp.Err {
		case ErrNoError:
			retries = s.parent.config.Metadata.Retry.Max
		case ErrRebalanceInProgress, ErrUnknownMemberId, ErrIllegalGeneration:
			return
		default:
			s.parent.handleError(resp.Err, "", -1)
			return
		}

		select {
		case <-pause.C:
		case <-s.hbDying:
			return
		}
	}
}

// --------------------------------------------------------------------

// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for your consumer group session life-cycle and allow you to
// trigger logic before or after the consume loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.
type ConsumerGroupHandler interface {
	// Setup is run at the beginning of a new session, before ConsumeClaim.
	Setup(ConsumerGroupSession) error

	// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
	// but before the offsets are committed for the very last time.
	Cleanup(ConsumerGroupSession) error

	// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
	// Once the Messages() channel is closed, the Handler must finish its processing
	// loop and exit.
	// by sun: 整个消费的重载器!! ==> 需要用户自己实现
	ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error
}

// 实际上，就是对应一个分区的消费者。 的一个 消费句柄！！ ==> 的抽象!!
// ConsumerGroupClaim processes Kafka messages from a given topic and partition within a consumer group.
type ConsumerGroupClaim interface {
	// Topic returns the consumed topic name.
	Topic() string

	// Partition returns the consumed partition.
	Partition() int32

	// InitialOffset returns the initial offset that was used as a starting point for this claim.
	InitialOffset() int64

	// HighWaterMarkOffset returns the high water mark offset of the partition,
	// i.e. the offset that will be used for the next message that will be produced.
	// You can use this to determine how far behind the processing is.
	HighWaterMarkOffset() int64

	// Messages returns the read channel for the messages that are returned by
	// the broker. The messages channel will be closed when a new rebalance cycle
	// is due. You must finish processing and mark offsets within
	// Config.Consumer.Group.Session.Timeout before the topic/partition is eventually
	// re-assigned to another group member.
	Messages() <-chan *ConsumerMessage
}

type consumerGroupClaim struct {
	topic     string
	partition int32
	offset    int64
	PartitionConsumer
}

func newConsumerGroupClaim(sess *consumerGroupSession, topic string, partition int32, offset int64) (*consumerGroupClaim, error) {
	pcm, err := sess.parent.consumer.ConsumePartition(topic, partition, offset)
	if err == ErrOffsetOutOfRange {
		offset = sess.parent.config.Consumer.Offsets.Initial
		pcm, err = sess.parent.consumer.ConsumePartition(topic, partition, offset)
	}
	if err != nil {
		return nil, err
	}

	// 这种写法感觉挺大胆的，很自信；如果阻塞了，那岂不是就gg在中间了！！
	go func() {
		for err := range pcm.Errors() {
			sess.parent.handleError(err, topic, partition)
		}
	}()

	return &consumerGroupClaim{
		topic:             topic,
		partition:         partition,
		offset:            offset,
		PartitionConsumer: pcm,
	}, nil
}

func (c *consumerGroupClaim) Topic() string        { return c.topic }
func (c *consumerGroupClaim) Partition() int32     { return c.partition }
func (c *consumerGroupClaim) InitialOffset() int64 { return c.offset }

// Drains messages and errors, ensures the claim is fully closed.
func (c *consumerGroupClaim) waitClosed() (errs ConsumerErrors) {
	go func() {
		for range c.Messages() {
		}
	}()

	for err := range c.Errors() {
		errs = append(errs, err)
	}
	return
}

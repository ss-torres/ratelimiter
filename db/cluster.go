package db

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const (
	clusterKey = "{}" // 用于集群之间协商谁成为"加令牌节点"
	nodeReq    = "PING"
	nodeRsp    = "PONG"
)

// =====================================================================================
/*
 * 作用: 判断这个节点是否用于新增键令牌(以下称为: 加令牌节点),
 * 从而实现每个redis(或者redis集群)总是有N个节点(例如2个或者3个)用于添加令牌操作
 * 我们可能采用多种方式获取所有的key, 然后向对应的key增加令牌, 例如
 * (1) 通过数据库获取所有键值
 * (2) 通过遍历redis获取所有键值
 * (3) 直接读取配置文件
 * (4) 通过远程调用设置
 * 判断方式通过如下实现:
 * (1) 实现这个判断需要在redis中使用一个键值存储信息, 这里使用"{}"的键值, 这个键存储所有的加令牌节点
 *		1) 这个键使用类型为list
 *		2) 这个list中存储的值为是否可用标示+":"+节点标识符(可用时, 为"1:节点标识符")
 * (2) 程序启动时, 监听这个键的修改操作
 *		1) "加令牌节点"监听插入类型的消息(可能是LINSERT, 也可能给是PUSH类型的消息), "加令牌节点"还需要监听LSET
 *		2) 非"加令牌节点"监听类型的消息(可能是LREM)
 * (3) 然后, 读取redis中键名为"{}"的键的值, 然后判断当前"加令牌节点"的个数, 如果这个个数小于配置值,
 *  则修改redis中这个键的值,将自己设为"加令牌节点", 否则, 不做处理
 * (4) 对于每个设置为"加令牌节点"的应用, 会在这个list中排在后面的M个节点(例如2个或者3个)建立tcp连接,
 *	然后通过ping和pong消息, 来判断这个节点是否可以连接.
 * (5) 如果A节点发现B节点不可连接(假设发送一条消息,经过20s没有接收到回应值), 向redis发送一条修改请求,
 *   请求中先查看B节点是否可用,如果B节点当前显示可用, 那么修改"{}"的值,设置1+":"+节点标识符为
 *   0+":"+节点标识符(B节点), 如果显示B节点已经不可用, 则继续进行tcp通信, 如果B节点已经不存在,
 *   则断开与这个节点的tcp连接
 * (6) 节点继续等待若干时间(例如15s), 然后再次判断这个节点是否已经恢复正常, 如果没有恢复正常,
 *  从这个list中清除这个节点
 * (7) 那些非"加令牌节点"接收到清楚的消息之后, 会申请自己成为"加令牌节点",会检测当前"加令牌节点"的个数,
 *   如果条件满足, 则将这个节点信息插入, 让这个节点成为"加令牌节点", 然后, 每个当前"加令牌节点"
 *   读取所有的"加令牌节点", 重新更新与哪些节点建立tcp连接.
 * (8) 当前标识符为Ip:port
 */

// InitConfig 用来初始化Maintainer
type InitConfig struct {
	maxTargetNum    int    // "加令牌节点"主动联系的其他"加令牌节点"最大的个数
	maxFailNum      int    // "加令牌节点"标记其他"加令牌节点"不可用需要尝试的发送消息的次数，当前每秒发送一次
	maxFailResetNum int    // 失败重设节点的状态之后等待的时间
	nodeAddr        string // 监听的地址
	maxAddNodeNum   int    // 集群中最大的"加令牌节点"的个数
	redisAddr       string // redis的地址
	redisPWD        string // redis的密码
}

// NewInitConfig 用来创建InitConfig
func NewInitConfig(maxTargetNum int, maxFailNum int, nodeAddr string,
	maxAddNodeNum int, redisAddr string, redisPWD string) *InitConfig {
	return &InitConfig{
		maxTargetNum:  maxTargetNum,
		maxFailNum:    maxFailNum,
		nodeAddr:      nodeAddr,
		maxAddNodeNum: maxAddNodeNum,
		redisAddr:     redisAddr,
		redisPWD:      redisPWD,
	}
}

// Maintainer 用来维持这个集群始终存在若干"加令牌节点"
type Maintainer struct {
	client          redis.UniversalClient
	pwg             *sync.WaitGroup
	toNodes         *SendNodes    // 主动通信的"加令牌节点"
	fromNode        *ListenNode   // 监听节点
	maxFailResetNum int           // 标识"加令牌节点"无效之后，等待的时间
	pubsub          *redis.PubSub // 用来监听redis的键空间事件
	nodeAddr        string        // 用来设置这个节点的标识, 这个标识的监听通信的端口
	maxAddNodeNum   int           // 设置这个集群需要的"加令牌节点"个数
	isAddNode       bool          // 是否为"加令牌节点"
	// 通信相关
	keyMsgChan <-chan *redis.Message
}

// NewMaintainer 用来创建Maintainer
func NewMaintainer(client redis.UniversalClient, config *InitConfig) (m *Maintainer, err error) {
	toNodes, err := NewSendNodes(config.maxTargetNum, config.maxFailNum)
	if err != nil {
		return nil, err
	}
	// 清理发送消息相关的tcp连接
	defer func() {
		if err != nil {
			toNodes.closeNodes()
		}
	}()

	var wg sync.WaitGroup
	fromNode, err := NewListenNode(&wg, config.nodeAddr)
	if err != nil {
		return nil, err
	}
	// 清理监听消息相关的网络
	defer func() {
		if err != nil && fromNode != nil {
			fromNode.Close()
		}
	}()

	m = &Maintainer{
		client:          client,
		pwg:             &wg,
		toNodes:         toNodes,
		fromNode:        fromNode,
		maxFailResetNum: config.maxFailResetNum,
		nodeAddr:        config.nodeAddr,
		maxAddNodeNum:   config.maxAddNodeNum,
		isAddNode:       false,
	}

	err = m.subscribeClusterNotify(config.redisAddr, config.redisPWD)
	if err != nil {
		return nil, err
	}
	// 出错时，关闭键空间提醒, 按照测试，这个defer，就算没有执行上面的语句，也会生效
	defer func() {
		if err != nil && m != nil {
			m.closeNotify()
		}
	}()

	err = m.startInit()
	if err != nil {
		return nil, err
	}

	return m, nil
}

// closeNotify 关闭键空间的监听
func (m *Maintainer) closeNotify() {
	if m.pubsub != nil {
		m.pubsub.Close()
	}
}

// Close 用于清理相关的信息
func (m *Maintainer) Close() {
	if m.toNodes != nil {
		m.toNodes.closeNodes()
	}
	if m.fromNode != nil {
		m.fromNode.Close()
	}
	m.closeNotify()

	m.Wait()
}

// 获取有效的节点在redis中如何存储
func getValidNodeAddr(nodeAddr string) string {
	return "1:" + nodeAddr
}

// 获取无效的节点在redis中如何存储
func getInvalidNodeAddr(nodeAddr string) string {
	return "0:" + nodeAddr
}

// 根据有效或者无效节点获取实际节点
func getRealNodeAddr(nodeAddr string) (string, error) {
	idx := strings.IndexByte(nodeAddr, ':')
	if idx == -1 && idx < len(nodeAddr)-1 {
		return "", errors.New(`nodeAddr should contains ':'`)
	}

	return nodeAddr[idx+1:], nil
}

// subscribeClusterNotify 用来监听"加令牌节点"变化情况
func (m *Maintainer) subscribeClusterNotify(redisAddr string, redisPWD string) error {
	client, err := newClient(redisAddr, redisPWD)
	if err != nil {
		return err
	}
	clusterKeySpace := "__keyspace@0__:" + clusterKey
	pubsub := client.Subscribe(clusterKeySpace)
	// Wait for confirmation that subscription is created before publishing anything
	iface, err := pubsub.Receive()
	if err != nil {
		return err
	}

	switch iface.(type) {
	case *redis.Subscription:
	case *redis.Message:
		return errors.New("iface.(type) is *Message")
	case *redis.Pong:
		return errors.New("iface.(type) is *Pong")
	default:
		return errors.New("iface.(type) is default")
	}

	// Go channels which receives messages.
	m.keyMsgChan = pubsub.Channel()
	return nil
}

// startInit 进行完整的初始化操作
func (m *Maintainer) startInit() error {
	ok, err := m.tryBeAddNode()
	if err != nil {
		return err
	}
	// 如果出现错误，则将这个节点从"加令牌节点"移除
	defer func() {
		if err != nil {
			m.removeValidAddNode(m.nodeAddr)
		}
	}()

	// 如果没有成为"加令牌节点", 那么监听消息就可以了
	if !ok {
		return nil
	}
	m.isAddNode = true

	// 如果成为"加令牌节点"，则需要和其他"加令牌节点"进行通信
	tcpNodeAddrs, err := m.getTCPNodeAddrs()
	if err != nil {
		return err
	}
	// 建立tcp连接，如果连接失败，则在真正的事件循环时，
	// 申请移除这个节点
	for _, nodeAddr := range tcpNodeAddrs {
		m.toNodes.insertNodeIgnoreErr(nodeAddr)
	}

	return nil
}

// handlePushMsg 用来处理插入的消息
func (m *Maintainer) handlePushMsg() error {
	if !m.isAddNode {
		return nil
	}
	tcpNodeAddrs, err := m.getTCPNodeAddrs()
	if err != nil {
		return err
	}
	// 移除非"加令牌节点", 添加"加令牌节点"
	origTCPNodeAddrs := m.toNodes.getNodeAddrs()
	addAddrs := difference(tcpNodeAddrs, origTCPNodeAddrs)
	for _, addAddr := range addAddrs {
		m.toNodes.insertNodeIgnoreErr(addAddr)
	}
	return nil
}

// handleRemMsg 用来处理移除消息
func (m *Maintainer) handleRemMsg() error {
	if !m.isAddNode {
		// 非"加令牌节点"申请成为"加令牌节点"
		ok, err := m.tryBeAddNode()
		if err != nil {
			return err
		}
		m.isAddNode = ok
		return nil
	}
	tcpNodeAddrs, err := m.getTCPNodeAddrs()
	if err != nil {
		return err
	}
	// 移除非"加令牌节点", 这里理论上也应该处理增加节点，
	// 不过，如果没有新节点出现，处理意义不大
	origTCPNodeAddrs := m.toNodes.getNodeAddrs()
	addAddrs := difference(origTCPNodeAddrs, tcpNodeAddrs)
	for _, addAddr := range addAddrs {
		m.toNodes.deleteNode(addAddr)
	}
	return nil
}

// handleSetMsg 用来处理设置消息
func (m *Maintainer) handleSetMsg(failNodeAddr *string) error {
	if !m.isAddNode {
		return nil
	}
	// 这里需要处理两种情况，
	// (1) 有节点被设置为无效
	// (2) 有节点又被设置为有效

	// 处理(1)，如果自身被设置无效，则恢复有效
	_, err := m.resetAddNode(getInvalidNodeAddr(m.nodeAddr), getValidNodeAddr(m.nodeAddr))
	if err != nil {
		return err
	}
	// 处理（2)，如果有节点被设置有效，查看是否为自己监听的失败接口
	ok, err := m.findAddNode(getValidNodeAddr(*failNodeAddr))
	if err != nil {
		return err
	}
	if ok {
		*failNodeAddr = ""
	}
	return nil
}

// handleMsg 用来处理消息
func (m *Maintainer) handleMsg(msg *redis.Message, failNodeAddr *string) (int, error) {
	// 如果这个节点是加令牌节点
	payload := msg.Payload
	// 处理消息
	switch payload {
	case "lpush":
		fallthrough
	case "rpush":
		return 0, m.handlePushMsg()
	case "lrem":
		return 1, m.handleRemMsg()
	case "lset":
		return 2, m.handleSetMsg(failNodeAddr)
	}

	return -1, nil
}

// RunLoop 是主事件循环
func (m *Maintainer) RunLoop() error {
	var realFailNodeAddr string
	failResetCount := 0
	var wg sync.WaitGroup
	timeOut := time.Second
	timeOutThres := time.Duration(0.99 * float64(timeOut))
	for {
		// 处理redis键监听消息
		var msg *redis.Message
		select {
		case msg = <-m.keyMsgChan:
		default:
			msg = nil
		}
		if msg != nil {
			m.handleMsg(msg, &realFailNodeAddr)
			// 如果已经清除
			if len(realFailNodeAddr) == 0 {
				failResetCount = 0
				m.toNodes.resetFailNode(realFailNodeAddr)
			}
		}

		// 如果没有失败的项，则进行必要的tcp通信
		if len(realFailNodeAddr) == 0 {
			// 查看是否存在失败的节点
			failNodeAddr, ok := m.toNodes.getFailNode()
			if ok {
				var err error
				ok, err = m.resetAddNode(getValidNodeAddr(failNodeAddr),
					getInvalidNodeAddr(failNodeAddr))
				if err != nil {
					return err
				}
				if !ok {
					// 如果没有成功，则说明有其他节点这么做
					m.toNodes.resetFailNode(failNodeAddr)
				} else {
					realFailNodeAddr = failNodeAddr
					failResetCount = 0
				}
			}
			// 如果当前这个节点不存在监听的失败节点
			if !ok {
				cur := time.Now()
				m.toNodes.SendMsg(&wg, timeOut)
				wg.Wait()
				duration := time.Now().Sub(cur)
				if duration < timeOutThres {
					time.Sleep(timeOut - duration)
				}
			}
		} else {
			failResetCount++
			// 如果已经等待了足够的时间，则清理这个无效的节点
			if failResetCount > m.maxFailResetNum {
				m.removeInvalidAddNode(realFailNodeAddr)
				failResetCount = 0
				realFailNodeAddr = ""
			}
			time.Sleep(timeOut)
		}
	}
}

// Wait 用来等待消息的处理
func (m *Maintainer) Wait() {
	m.pwg.Wait()
}

// tryBeAddNode 用来尝试成为"加令牌节点"
func (m *Maintainer) tryBeAddNode() (bool, error) {
	/*
	 * KEYS[1] 表示这个需要操作的键
	 * ARGV[1] 表示需要的"加令牌节点"个数
	 * ARGV[2] 表示当前节点标识符, 传参时就在前面+"1:"
	 */
	// 实现思路, 如果节点已经够了,则返回结束, 如果节点不够, 则插入成为新的节点
	tryBeNodeScript :=
		`--[[测试显示, 通过call, 可以将error返回给客户端, 即使没有使用return]]--
		local nodeNum = redis.call("LLEN", KEYS[1])
		local maxNum = tonumber(ARGV[1])
		if (nodeNum >= maxNum) 
		then 
			return false
		end
		redis.call("RPUSH", KEYS[1], ARGV[2])
		return true
		`

	tryBeNodeCmd := redis.NewScript(tryBeNodeScript)
	res, err := tryBeNodeCmd.Run(m.client, []string{clusterKey},
		m.maxAddNodeNum, getValidNodeAddr(m.nodeAddr)).Result()
	if err != nil && err != redis.Nil {
		return false, err
	}

	if res == nil {
		return false, nil
	}

	if isAdd, ok := res.(int64); ok {
		return (isAdd == 1), nil
	}

	return false, errors.New("res should be int64")
}

// resetAddNode 用来设置一个加令牌节点的有效和无效状态
func (m *Maintainer) resetAddNode(curNodeAddr string, setNodeAddr string) (bool, error) {
	/*
	 * KEYS[1] 表示这个需要操作的键
	 * ARGV[1] 表示节点当前的值
	 * ARGV[2] 表示节点设置的值
	 */
	// 实现思路, 如果节点已经够了,则返回结束, 如果节点不够, 则插入成为新的节点
	resetAddNodeScript :=
		`--[[测试显示, 通过call, 可以将error返回给客户端, 即使没有使用return]]--
		local nodeAddrs = redis.call("LRANGE", KEYS[1], 0, -1)
		for key, value in pairs(nodeAddrs) do
		if (value == ARGV[1])
			redis.call("LSET", KEYS[1], key-1, ARGV[2])
			return true
		then
		end
		end
		return false
		`

	resetAddNodeCmd := redis.NewScript(resetAddNodeScript)
	res, err := resetAddNodeCmd.Run(m.client, []string{clusterKey},
		curNodeAddr, setNodeAddr).Result()
	if err != nil && err != redis.Nil {
		return false, err
	}

	if res == nil {
		return false, nil
	}

	if isAdd, ok := res.(int64); ok {
		return (isAdd == 1), nil
	}

	return false, errors.New("res should be int64")
}

// 查看是否存在某个"加令牌节点"
func (m *Maintainer) findAddNode(realNodeAddr string) (bool, error) {
	/*
	 * KEYS[1] 表示这个需要操作的键
	 * ARGV[1] 表示节点当前的值
	 * ARGV[2] 表示节点设置的值
	 */
	// 实现思路, 如果节点已经够了,则返回结束, 如果节点不够, 则插入成为新的节点
	findAddNodeScript :=
		`--[[测试显示, 通过call, 可以将error返回给客户端, 即使没有使用return]]--
		local nodeAddrs = redis.call("LRANGE", KEYS[1], 0, -1)
		for key, value in pairs(nodeAddrs) do
		if (value == ARGV[1])
			return true
		then
		end
		end
		return false
		`

	findAddNodeCmd := redis.NewScript(findAddNodeScript)
	res, err := findAddNodeCmd.Run(m.client, []string{clusterKey},
		realNodeAddr).Result()
	if err != nil && err != redis.Nil {
		return false, err
	}

	if res == nil {
		return false, nil
	}

	if isAdd, ok := res.(int64); ok {
		return (isAdd == 1), nil
	}

	return false, errors.New("res should be int64")

}

// removeValidAddNode 用来从redis中移除一个有效的"加令牌节点"
func (m *Maintainer) removeValidAddNode(nodeAddr string) error {
	realNodeAddr := getValidNodeAddr(nodeAddr)
	return m.removeAddNode(realNodeAddr)
}

// removeInvalidAddNode 用来从redis中移除一个无效的"加令牌节点"
func (m *Maintainer) removeInvalidAddNode(nodeAddr string) error {
	realNodeAddr := getInvalidNodeAddr(nodeAddr)
	return m.removeAddNode(realNodeAddr)
}

// removeAddNode 用来从redis中移除某个"加令牌节点"
func (m *Maintainer) removeAddNode(realNodeAddr string) error {
	_, err := m.client.LRem(clusterKey, 1, realNodeAddr).Result()
	return err
}

// 获取tcp通信的节点名
func (m *Maintainer) getTCPNodeAddrs() ([]string, error) {
	allNodeAddrs, err := m.getAllNodeAddrs()
	if err != nil {
		return nil, err
	}

	nodeAddrs, ok := m.getNextNNodeAddrs(allNodeAddrs)
	if !ok {
		return nil, errors.New("Couldn't find self in all add node addrs")
	}

	return nodeAddrs, nil
}

// 获取所有节点名
func (m *Maintainer) getAllNodeAddrs() ([]string, error) {
	storeNodeAddrs, err := m.client.LRange(clusterKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var nodeAddrs []string
	for _, storeID := range storeNodeAddrs {
		nodeAddr, err := getRealNodeAddr(storeID)
		if err != nil {
			return nil, err
		}
		nodeAddrs = append(nodeAddrs, nodeAddr)
	}

	return nodeAddrs, nil
}

// 获取数组中后面的N个节点名, 可以回环,但是不可以包含自己
func (m *Maintainer) getNextNNodeAddrs(nodeAddrs []string) ([]string, bool) {
	var idx int
	// 先查找是否存在
	for ; idx < len(nodeAddrs); idx++ {
		if nodeAddrs[idx] == m.nodeAddr {
			break
		}
	}
	if idx == len(nodeAddrs) {
		return nil, false
	}

	// 插入之后的最多N个节点
	var targetNodeAddrs []string
	next := idx + 1
	for len(targetNodeAddrs) < m.toNodes.getMaxToNum() {
		if next == len(nodeAddrs) {
			next = 0
		}
		// 如果循环之后,开始找到自己,则说明已经遍历结束
		if next == idx {
			break
		}
		targetNodeAddrs = append(targetNodeAddrs, nodeAddrs[next])
		next++
	}
	return targetNodeAddrs, true
}

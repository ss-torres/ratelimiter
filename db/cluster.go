package db

import (
	"errors"
	"strings"

	"github.com/go-redis/redis"
)

const (
	clusterKey = "{}" // 用于集群之间协商谁成为"加令牌节点"
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

// Maintainer 用来维持这个集群始终存在若干"加令牌节点"
type Maintainer struct {
	client        redis.UniversalClient
	targetNodes   []string      // 主动通信的"加令牌节点"
	targetMaxNum  int           // 最多主动通信的"加令牌节点"
	pubsub        *redis.PubSub // 用来监听redis的键空间事件
	nodeID        string        // 用来设置这个节点的标识, 这个标识的监听通信的端口
	addNodeMaxNum int           // 设置这个集群需要的"加令牌节点"个数
	isAddNode     bool          // 是否为"加令牌节点"
	// 通信相关
	keyMsgChan <-chan *redis.Message
}

// 获取有效的节点在redis中如何存储
func getValidNodeStr(nodeID string) string {
	return "1:" + nodeID
}

// 获取无效的节点在redis中如何存储
func getInvalidNodeStr(nodeID string) string {
	return "0:" + nodeID
}

// 根据有效或者无效节点获取实际节点
func getRealNodeStr(nodeID string) (string, error) {
	idx := strings.IndexByte(nodeID, ':')
	if idx == -1 && idx < len(nodeID)-1 {
		return "", errors.New(`nodeID should contains ':'`)
	}

	return nodeID[idx+1:], nil
}

// SubscribeClusterNotify 用来监听"加令牌节点"变化情况
func (m *Maintainer) SubscribeClusterNotify(nodeName string) error {
	clusterKeySpace := "__keyspace@0__:" + clusterKey
	pubsub := m.client.Subscribe(clusterKeySpace)
	// Wait for confirmation that subscription is created before publishing anything
	_, err := pubsub.Receive()
	if err != nil {
		return err
	}

	// Go channels which receives messages.
	m.keyMsgChan = pubsub.Channel()
	return nil
}

// tryBeAddNodes 用来获取当前所有"加令牌节点"的信息
func (m *Maintainer) tryBeAddNodes() (bool, error) {
	/*
	 * KEYS[1] 表示这个需要操作的键
	 * ARGS[1] 表示需要的"加令牌节点"个数
	 * ARGS[2] 表示当前节点标识符, 传参时就在前面+"1:"
	 */
	// 实现思路, 如果节点已经够了,则返回结束, 如果节点不够, 则插入成为新的节点
	tryBeNodesScript :=
		`--[[测试显示, 通过call, 可以将error返回给客户端, 即使没有使用return]]--
		local nodeNum = redis.call("LLEN", KEYS[1])
		if (nodeNum >= ARGS[1]) 
		then 
			return false
		end
		redis.call("RPUSH", KEYS[1], ARGS[2])
		return true
		`

	tryBeNodesCmd := redis.NewScript(tryBeNodesScript)
	res, err := tryBeNodesCmd.Run(m.client, []string{clusterKey},
		m.addNodeMaxNum, getValidNodeStr(m.nodeID)).Result()
	if err != nil && err != redis.Nil {
		return false, err
	}

	if isAdd, ok := res.(int64); ok {
		return (isAdd == 1), nil
	}

	return false, errors.New("res should be int64")
}

// 获取所有节点名
func (m *Maintainer) getAllNodeNames() ([]string, error) {
	storeNodeIDs, err := m.client.LRange(clusterKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	var nodeIDs []string
	for _, storeID := range storeNodeIDs {
		nodeID, err := getRealNodeStr(storeID)
		if err != nil {
			return nil, err
		}
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

// 获取数组中后面的N个节点名, 可以回环,但是不可以包含自己
func (m *Maintainer) getNextNNodeNames(nodeIDs []string) ([]string, bool) {
	var idx int
	// 先查找是否存在
	for ; idx < len(nodeIDs); idx++ {
		if nodeIDs[idx] == m.nodeID {
			break
		}
	}
	if idx == len(nodeIDs) {
		return nil, false
	}

	// 插入之后的最多N个节点
	var targetNodeIDs []string
	next := idx + 1
	for len(targetNodeIDs) < m.targetMaxNum {
		if next == len(nodeIDs) {
			next = 0
		}
		// 如果循环之后,开始找到自己,则说明已经遍历结束
		if next == idx {
			break
		}
		targetNodeIDs = append(targetNodeIDs, nodeIDs[next])
		next++
	}
	return targetNodeIDs, true
}

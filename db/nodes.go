package db

import (
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// ListenNode 用来管理其他节点发送来的消息
type ListenNode struct {
	ln    net.Listener // 监听端口
	conns sync.Map     // 记录所有的连接端口
}

// NewListenNode 用来创建ListenNode
func NewListenNode(wg *sync.WaitGroup, addr string) (*ListenNode, error) {
	lnode := &ListenNode{}
	err := lnode.startListen(wg, addr)
	if err != nil {
		log.Printf("lnode.startListen error: %v\n", err)
		return nil, err
	}
	return lnode, nil
}

// startListen 用来启用监听
func (lnode *ListenNode) startListen(wg *sync.WaitGroup, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	lnode.ln = ln
	wg.Add(1)
	// 回复其他节点发送来的消息
	go func() {
		// 退出清理
		defer func() {
			ln.Close()
			wg.Done()
		}()
		for {
			conn, err := ln.Accept()
			if err != nil {
				// 这里先打印log， 可能有些错误不需要退出， 这里先不做区分
				log.Printf("ln.Accept error: %v\n", err)
				break
			}
			lnode.handleConnection(wg, conn)
		}
	}()

	return nil
}

// handleConnection 用来回复连接消息
func (lnode *ListenNode) handleConnection(wg *sync.WaitGroup, conn net.Conn) {
	lnode.conns.Store(conn, true)
	wg.Add(1)
	// 读取和发送回应消息
	go func() {
		// 退出清理
		defer func() {
			err := conn.Close()
			if err != nil {
				log.Printf("conn.Close error: %v\n", err)
			}
			lnode.conns.Delete(conn)
			wg.Done()
		}()
		buf := make([]byte, len(nodeReq))
		for {
			_, err := io.ReadFull(conn, buf)
			if err != nil {
				if err != io.EOF {
					log.Printf("io.ReadFull error: %v\n", err)
				}
				return
			}
			if string(buf) != nodeReq {
				log.Printf("request sent by nodes isn't invalid")
				// 部分发送不知道在这里会不会产生错误影响
				return
			}
			_, err = conn.Write([]byte(nodeRsp))
			if err != nil {
				log.Printf("io.Write error: %v\n", err)
				return
			}
		}
	}()
}

// Close 用来关闭网络通信
func (lnode *ListenNode) Close() {
	if lnode.ln != nil {
		err := lnode.ln.Close()
		if err != nil {
			log.Printf("ln.Close error: %v\n", err)
		}
	}
	lnode.conns.Range(func(key, value interface{}) bool {
		conn, ok := key.(net.Conn)
		if ok {
			err := conn.Close()
			if err != nil {
				log.Printf("conn.Close error: %v\n", err)
			}
		}
		log.Printf("key.(net.Conn) failed\n")
		return true
	})
}

// NodeInfo 用来存储节点信息
type NodeInfo struct {
	Addr      string
	FailCount int // 用来统计连续发送失败次数
	TCPConn   net.Conn
}

// SendNodes 用来管理向其他节点发送消息
type SendNodes struct {
	nodes        map[string]*NodeInfo
	maxTargetNum int // 最多主动通信的"加令牌节点"
	maxFailNum   int // 允许的最大失败次数，当前默认每秒尝试一次
	dialer       net.Dialer
}

// NewSendNodes 用来创建SendNodes
func NewSendNodes(maxTargetNum, maxFailNum int) (*SendNodes, error) {
	return &SendNodes{
		nodes:        make(map[string]*NodeInfo),
		maxTargetNum: maxTargetNum,
		maxFailNum:   maxFailNum,
		dialer:       net.Dialer{},
	}, nil
}

// getMaxToNum 用来获取进行通信的tcp个数
func (t *SendNodes) getMaxToNum() int {
	return t.maxTargetNum
}

// dialTimeOut 用来返回dial最长超时时间
func (t *SendNodes) dialTimeOut() time.Duration {
	return time.Duration(t.maxFailNum) * time.Second
}

// getAllNodeAddrs 用来获取当前监听的所有tcp连接
func (t *SendNodes) getNodeAddrs() []string {
	nodeAddrs := make([]string, len(t.nodes))
	for key := range t.nodes {
		nodeAddrs = append(nodeAddrs, key)
	}
	return nodeAddrs
}

// getFailNode只返回一个失败节点，而不是多个，是因为这样可以简化逻辑, 虽然这样可能使"加令牌节点"数恢复的更慢
func (t *SendNodes) getFailNode() (string, bool) {
	for _, val := range t.nodes {
		if val.FailCount > t.maxFailNum {
			return val.Addr, true
		}
	}

	return "", false
}

// resetFailNode 表示重新进行这个失败节点计数
func (t *SendNodes) resetFailNode(failNodeAddr string) {
	node, ok := t.nodes[failNodeAddr]
	if ok {
		node.FailCount = 0
	}
}

// insertNodeIgnoreErr 用于插入节点，插入节点的同时，建立tcp连接
func (t *SendNodes) insertNodeIgnoreErr(nodeAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), t.dialTimeOut())
	defer cancel()

	conn, err := t.dialer.DialContext(ctx, "tcp", nodeAddr)
	node := t.nodes[nodeAddr]
	// 这个不应该出现，如果出现，则关闭连接
	if node != nil {
		node.TCPConn.Close()
	}
	// 设置新值
	failCount := 0
	if err != nil {
		failCount = t.maxFailNum
	}
	node = &NodeInfo{
		Addr:      nodeAddr,
		FailCount: failCount,
		TCPConn:   conn,
	}
}

// deleteNode 用来移除节点， 移除节点的同时，断开tcp连接
func (t *SendNodes) deleteNode(nodeAddr string) {
	node, ok := t.nodes[nodeAddr]
	if ok {
		node.TCPConn.Close()
		delete(t.nodes, nodeAddr)
	}
}

// SendMsg 用来向其他节点发送消息
func (t *SendNodes) SendMsg(wg *sync.WaitGroup, timeOut time.Duration) {
	for _, node := range t.nodes {
		wg.Add(1)
		go func(node *NodeInfo) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), timeOut)
			defer cancel()

			var err error
			// 如果没有建立连接，则尝试使用1s建立连接
			if node.TCPConn == nil {
				node.TCPConn, err = t.dialer.DialContext(ctx, "tcp", node.Addr)
				// 如果失败，则增加一个新的失败计数，然后退出
				if err != nil {
					node.FailCount++
					return
				}
			}
			// 尝试发送数据，如果失败，则失败计数加1，如果成功，则清除计数，这里发送部分数据不知道会不会使其一直失败
			node.TCPConn.SetWriteDeadline(time.Now().Add(timeOut))
			_, err = node.TCPConn.Write([]byte("PING"))
			node.TCPConn.SetWriteDeadline(time.Time{})
			if err != nil {
				log.Printf("node.TCPConn.Write error: %v\n", err)
				node.FailCount++
				return
			}
			node.FailCount = 0
		}(node)
	}
}

// closeNodes 用来关闭所有的tcp连接
func (t *SendNodes) closeNodes() {
	for _, node := range t.nodes {
		node.TCPConn.Close()
	}
}

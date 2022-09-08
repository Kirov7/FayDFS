package raft

import (
	"faydfs/namenode/service"
	"faydfs/proto"
	"faydfs/public"
	"log"
	"sync"
	"time"
)

type RAFTROLE uint8

const None int64 = -1

const (
	FOLLOWER RAFTROLE = iota
	CANDIDATE
	LEADER
)

func RoleToString(role RAFTROLE) string {
	switch role {
	case CANDIDATE:
		return "Candidate"
	case FOLLOWER:
		return "Follower"
	case LEADER:
		return "Leader"
	}
	return "unknow"
}

type Raft struct {
	mu             sync.RWMutex
	peers          []*RaftClientEnd     // rpc 客户端
	me             int                  // 自己的 id
	dead           int32                // 节点的状态
	applyCh        chan *proto.ApplyMsg // apply 协程通道
	applyCond      *sync.Cond           // apply 流程控制的信号量
	replicatorCond []*sync.Cond         // 复制操作控制的信号量
	role           RAFTROLE             // 节点当前的状态

	curTerm      int64    // 当前的任期
	votedFor     int64    // 为谁投票
	grantedVotes int      // 已经获得的票数
	logs         *RaftLog // 日志信息
	persister    *RaftLog

	commitIdx     int64 // 已经提交的最大的日志 id
	lastApplied   int64 // 已经 apply 的最大日志的 id
	nextIdx       []int // 到其他节点下一个匹配的日志 id 信息
	matchIdx      []int // 到其他节点当前匹配的日志 id 信息
	isSnapshoting bool  // 是否正在生成快照

	leaderId         int64       // 集群中当前 Leader 节点的 id
	electionTimer    *time.Timer // 选举超时定时器
	heartBeatTimer   *time.Timer // 心跳超时定时器
	heartBeatTimeout uint64      // 心跳超时时间
	baseElecTimeout  uint64      // 选举超时时间
}

// 投票先来先得

func BuildRaft(peers []*RaftClientEnd, me int, DBConn service.DB, applych chan *proto.ApplyMsg, hearttime uint64, electiontime uint64) *Raft {
	newraft := &Raft{
		peers:            peers,
		me:               me,
		dead:             0,
		applyCh:          applych,
		replicatorCond:   make([]*sync.Cond, len(peers)),
		role:             FOLLOWER,
		curTerm:          0,
		votedFor:         None,
		grantedVotes:     0,
		isSnapshoting:    false,
		logs:             MakePersistRaftLog(DBConn),
		persister:        MakePersistRaftLog(DBConn),
		commitIdx:        0,
		lastApplied:      0,
		nextIdx:          make([]int, len(peers)),
		matchIdx:         make([]int, len(peers)),
		heartBeatTimer:   time.NewTimer(time.Millisecond * time.Duration(hearttime)),
		electionTimer:    time.NewTimer(time.Millisecond * time.Duration(public.MakeAnRandomElectionTimeout(int(electiontime)))),
		baseElecTimeout:  electiontime,
		heartBeatTimeout: hearttime,
	}
	newraft.curTerm, newraft.votedFor = newraft.persister.ReadRaftState()
	newraft.ReInitLog()
	newraft.applyCond = sync.NewCond(&newraft.mu)
	LastLogIndex := newraft.logs.lastIdx
	for _, peer := range peers {
		log.Println("peer addr:%s   id:%d ", peer.addr, peer.id)
		newraft.matchIdx[peer.id], newraft.nextIdx[peer.id] = 0, int(LastLogIndex)+1
		if int(peer.id) != me {
			newraft.replicatorCond[peer.id] = sync.NewCond(&sync.Mutex{})
			go newraft.Replicator(peer)
		}
	}

	go newraft.Ticker()

	go newraft.Applier()

	return newraft
}

func (raft *Raft) Ticker() {
	//todo implement me
	panic("need impl")
}

func (raft *Raft) Applier() {
	//todo implement me
	panic("need impl")
}

func (raft *Raft) Replicator(peer *RaftClientEnd) {
	//todo implement me
	panic("need impl")
}

// ReInitLogs
// make logs to init state
func (rfLog *RaftLog) ReInitLogs() error {
	//todo implement me
	panic("need impl")
}

func (raft *Raft) ReInitLog() {
	raft.logs.ReInitLogs()
}

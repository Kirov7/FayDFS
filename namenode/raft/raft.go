package raft

import (
	"context"
	"faydfs/namenode/service"
	"faydfs/proto"
	"faydfs/public"
	"log"
	"sort"
	"sync"
	"sync/atomic"
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
	for !raft.IsKilled() {
		select {
		// 如果收到选举超时的信号,则改变自身状态发起选举
		case <-raft.electionTimer.C:
			{
				raft.mu.Lock()
				raft.ChangeRole(CANDIDATE)
				raft.curTerm += 1
				raft.StartNewElection()
				raft.electionTimer.Reset(time.Millisecond * time.Duration(public.MakeAnRandomElectionTimeout(int(raft.baseElecTimeout))))
				raft.mu.Unlock()
			}
		case <-raft.heartBeatTimer.C:
			{
				if raft.role == LEADER {
					raft.BroadcastHeartbeat()
					raft.heartBeatTimer.Reset(time.Millisecond * time.Duration(raft.heartBeatTimeout))
				}
			}
		}
	}
}

func (raft *Raft) Applier() {
	//todo implement me
	panic("need impl")
}

// replicateOneRound Leader replicates log entries to followers
func (raft *Raft) replicatorOneRound(peer *RaftClientEnd) {
	raft.mu.RLock()
	if raft.role != LEADER {
		raft.mu.RUnlock()
		return
	}
	prevLogIndex := uint64(raft.nextIdx[peer.id] - 1)
	log.Printf("leader send to peer:%d prevLogIndex:%d \n", peer.id, prevLogIndex)
	// snapshot
	if prevLogIndex < raft.logs.firstIdx {
		firstLog := raft.logs.GetFirst()
		snapShotReq := &proto.InstallSnapshotRequest{
			Term:              raft.curTerm,
			LeaderId:          int64(raft.me),
			LastIncludedIndex: firstLog.Index,
			LastIncludedTerm:  int64(firstLog.Term),
			Data:              raft.ReadSnapshot(),
		}
		raft.mu.RUnlock()
		log.Printf("send snapshot to %s with %s\n", peer.addr, snapShotReq.String())
		snapShotResp, err := (*peer.raftServiceCli).Snapshot(context.Background(), snapShotReq)
		if err != nil {
			log.Printf("send snapshot to %s failed %v\n", peer.addr, err.Error())
		}
		raft.mu.Lock()
		log.Printf("send snapshot to %s with resp %s\n", peer.addr, snapShotResp.String())

		if snapShotResp != nil {
			if raft.role == LEADER && raft.curTerm == snapShotReq.Term {
				if snapShotResp.Term > raft.curTerm {
					raft.ChangeRole(FOLLOWER)
					raft.curTerm = snapShotReq.Term
					raft.votedFor = -1
					raft.PersistRaftState()
				} else {
					raft.matchIdx[peer.id] = public.Max(int(snapShotReq.LastIncludedIndex), raft.matchIdx[peer.id])
					raft.nextIdx[peer.id] = raft.matchIdx[peer.id] + 1
				}
			}
		}
		raft.mu.Unlock()
	} else {
		firstIndex := raft.logs.firstIdx
		entries := make([]*proto.Entry, raft.logs.lastIdx-prevLogIndex)
		log.Printf("Leader need copy %d entries to peer %d\n\n", (raft.logs.lastIdx - prevLogIndex), peer.id)
		copy(entries, raft.logs.EraseBefore(int64(prevLogIndex)+1))
		appendEntReq := &proto.AppendEntriesRequest{
			Term:         raft.curTerm,
			LeaderId:     int64(raft.me),
			PrevLogIndex: int64(prevLogIndex),
			PrevLogTerm:  int64(raft.logs.GetEntry(int64(prevLogIndex)).Term),
			Entries:      entries,
			LeaderCommit: raft.commitIdx,
		}
		raft.mu.RUnlock()

		resp, err := (*peer.raftServiceCli).AppendEntries(context.Background(), appendEntReq)
		if err != nil {
			log.Printf("send append entries to %s failed %v\n", peer.addr, err.Error())
		}
		if raft.role == LEADER {
			if resp != nil {
				if resp.Success {
					log.Printf("send heart beat to %s success", peer.addr)
					raft.matchIdx[peer.id] = int(appendEntReq.PrevLogIndex) + len(appendEntReq.Entries)
					raft.nextIdx[peer.id] = raft.matchIdx[peer.id] + 1
					raft.advanceCommitIndexForLeader()
				} else {
					if resp.Term > appendEntReq.Term {
						raft.ChangeRole(FOLLOWER)
						raft.curTerm = resp.Term
						raft.votedFor = None
						raft.PersistRaftState()
					} else {
						raft.nextIdx[peer.id] = int(resp.ConflictIndex)
						if resp.ConflictTerm != -1 {
							for i := appendEntReq.PrevLogIndex; i >= int64(firstIndex); i-- {
								if raft.logs.GetEntry(i).Term == uint64(resp.GetConflictTerm()) {
									raft.nextIdx[peer.id] = int(i + 1)
									break
								}
							}
						}
					}
				}
			}
		}
	}
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

// MatchLog is log matched
//
func (raft *Raft) MatchLog(term, index int64) bool {
	return index <= int64(raft.logs.lastIdx) && index >= int64(raft.logs.firstIdx) &&
		raft.logs.GetEntry(index).Term == uint64(term)
}

// change raft node's role to new role
func (raft *Raft) ChangeRole(newrole RAFTROLE) {
	if raft.role == newrole {
		return
	}
	raft.role = newrole
	log.Printf("node's role change to -> %s\n\n", RoleToString(newrole))
	switch newrole {
	case FOLLOWER:
		raft.heartBeatTimer.Stop()
		raft.electionTimer.Reset(time.Duration(public.MakeAnRandomElectionTimeout(int(raft.baseElecTimeout))) * time.Millisecond)
	case CANDIDATE:

	case LEADER:
		lastLog := raft.logs.GetLast()
		raft.leaderId = int64(raft.me)
		for i := 0; i < len(raft.peers); i++ {
			raft.matchIdx[i], raft.nextIdx[i] = 0, int(lastLog.Index+1)
		}
		raft.electionTimer.Stop()
		raft.heartBeatTimer.Reset(time.Duration(raft.heartBeatTimeout) * time.Millisecond)
	}
}

// StartNewElection Election  make a new election
func (raft *Raft) StartNewElection() {
	log.Println("%d start a new election \n", raft.me)
	raft.grantedVotes = 1
	raft.votedFor = int64(raft.me)
	voteReq := &proto.RequestVoteRequest{
		Term:         raft.curTerm,
		CandidateId:  int64(raft.me),
		LastLogIndex: int64(raft.logs.lastIdx),
		LastLogTerm:  int64(raft.logs.GetLast().Term),
	}
	raft.PersistRaftState()

	for _, peer := range raft.peers {
		if peer.id == uint64(raft.me) || raft.role == LEADER {
			continue
		}
		go func(peer *RaftClientEnd) {
			log.Printf("send request vote to %s %s\n", peer.addr, voteReq.String())

			requestVoteResp, err := (*peer.raftServiceCli).RequestVote(context.Background(), voteReq)

			if err != nil {
				log.Printf("send request vote to %s failed %v\n", peer.addr, err.Error())
			}

			if requestVoteResp != nil {
				raft.mu.Lock()
				defer raft.mu.Unlock()
				log.Printf("send request vote to %s recive -> %s, curterm %d, req term %d\n", peer.addr, requestVoteResp.String(), raft.curTerm, voteReq.Term)
				if raft.curTerm == voteReq.Term && raft.role == CANDIDATE {
					if requestVoteResp.VoteGranted {
						log.Println("I got a vote")
						raft.IncrGrantedVotes()
						if raft.grantedVotes > len(raft.peers)/2 {
							log.Printf("node %d get majority votes int term %d \n", raft.me, raft.curTerm)
							raft.ChangeRole(LEADER)
							raft.BroadcastHeartbeat()
							raft.grantedVotes = 0
						}
					} else if requestVoteResp.Term > raft.curTerm {
						raft.ChangeRole(FOLLOWER)
						raft.curTerm, raft.votedFor = requestVoteResp.Term, None
						raft.PersistRaftState()
					}
				}
			}
		}(peer)
	}
}

// BroadcastHeartbeat broadcast heartbeat to peers
func (raft *Raft) BroadcastHeartbeat() {
	for _, peer := range raft.peers {
		if int(peer.id) == raft.me {
			continue
		}
		log.Printf("send heart beat to %s", peer.addr)
		go func(peer *RaftClientEnd) {
			raft.replicatorOneRound(peer)
		}(peer)
	}
}

func (raft *Raft) IsKilled() bool {
	return atomic.LoadInt32(&raft.dead) == 1
}

func (raft *Raft) PersistRaftState() {
	raft.persister.PersistRaftState(raft.curTerm, raft.votedFor)
}

func (raft *Raft) IncrGrantedVotes() {
	raft.grantedVotes += 1
}

// take a snapshot
func (raft *Raft) Snapshot(index int, snapshot []byte) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	raft.isSnapshoting = true

	firstIndex := raft.logs.GetFirstLogId()
	if index <= int(firstIndex) {
		raft.isSnapshoting = false
		log.Printf("reject snapshot, current snapshotIndex is larger in cur term")
		return
	}
	log.Printf("take a snapshot, index:%d", index)
	raft.logs.EraseBeforeWithDel(int64(index))
	raft.isSnapshoting = false
	raft.logs.PersisSnapshot(snapshot)
}

func (raft *Raft) ReadSnapshot() []byte {
	b, err := raft.logs.ReadSnapshot()
	if err != nil {
		log.Printf(err.Error())
	}
	return b
}

func (raft *Raft) advanceCommitIndexForLeader() {
	match := raft.matchIdx
	sort.Ints(match)
	n := len(match)
	newCommitIndex := match[n/2]
	if int64(newCommitIndex) > raft.commitIdx {
		if raft.MatchLog(raft.curTerm, int64(newCommitIndex)) {
			log.Printf("Leader peer %d advance commit index %d at term %d", raft.me, newCommitIndex, raft.curTerm)
			raft.commitIdx = int64(newCommitIndex)
			raft.applyCond.Signal()
		}
	}
}

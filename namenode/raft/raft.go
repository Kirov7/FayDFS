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

const None int64 = -1 // 不进行投票是的voteFor

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
	persister    *RaftLog // 持久化日志

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
	newRaft := &Raft{
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
	newRaft.curTerm, newRaft.votedFor = newRaft.persister.ReadRaftState()
	newRaft.ReInitLog()
	newRaft.applyCond = sync.NewCond(&newRaft.mu)
	LastLogIndex := newRaft.logs.lastIdx
	for _, peer := range peers {
		log.Printf("peer addr:%s   id:%d \n", peer.addr, peer.id)
		newRaft.matchIdx[peer.id], newRaft.nextIdx[peer.id] = 0, int(LastLogIndex)+1
		if int(peer.id) != me {
			newRaft.replicatorCond[peer.id] = sync.NewCond(&sync.Mutex{})
			go newRaft.Replicator(peer)
		}
	}

	go newRaft.Ticker()

	go newRaft.Applier()

	return newRaft
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

// Applier Write the commited message to the applyCh channel
// and update lastApplied
func (raft *Raft) Applier() {
	for !raft.IsKilled() {
		raft.mu.Lock()
		for raft.lastApplied >= raft.commitIdx {
			log.Printf("applier ... ")
			raft.applyCond.Wait()
		}
		commitIndex, lastApplied := raft.commitIdx, raft.lastApplied
		entries := make([]*proto.Entry, commitIndex-lastApplied)
		log.Printf("%d, applies entries %d-%d in term %d", raft.me, raft.lastApplied+1, commitIndex, raft.curTerm)
		copy(entries, raft.logs.GetRange(lastApplied+1, commitIndex+1))
		raft.mu.Unlock()
		for _, entry := range entries {
			raft.applyCh <- &proto.ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandTerm:  int64(entry.Term),
				CommandIndex: entry.Index,
			}
		}
		raft.mu.Lock()
		raft.lastApplied = int64(public.Max(int(raft.lastApplied), int(commitIndex)))
		raft.mu.Unlock()
	}
}

// Replicator 管理执行日志复制操作
func (raft *Raft) Replicator(peer *RaftClientEnd) {
	raft.replicatorCond[peer.id].L.Lock()
	defer raft.replicatorCond[peer.id].L.Unlock()
	for !raft.IsKilled() {
		log.Printf("peer id:%d wait for replicating...", peer.id)
		// 在循环中调用wait()
		for !(raft.role == LEADER && raft.matchIdx[peer.id] < int(raft.logs.lastIdx)) {
			raft.replicatorCond[peer.id].Wait()
		}
		raft.replicatorOneRound(peer)
	}
}

// replicatorOneRound 复制日志到follower
func (raft *Raft) replicatorOneRound(peer *RaftClientEnd) {
	raft.mu.RLock()
	if raft.role != LEADER {
		raft.mu.RUnlock()
		return
	}
	prevLogIndex := uint64(raft.nextIdx[peer.id] - 1)
	log.Printf("leader send to peer:%d prevLogIndex:%d \n", peer.id, prevLogIndex)
	// snapshot
	// 在复制的时候我们会判断到peer的prevLogIndex,如果比当前日志的第一条索引号还小，
	// 就说明Leader已经把这条日志打到快照中了,这里我们就要构造 InstallSnapshotRequest调用Snapshot RPC将快照数据发送给Follower节点,
	// 在收到成功响应之后,我们会更新rf.matchIdx,rf.nextId为LastIncludedIndex和LastIncludedIndex+1,更新到Follower节点复制进度。
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
					raft.votedFor = None
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

// HandleRequestVote  处理投票请求
func (raft *Raft) HandleRequestVote(req *proto.RequestVoteRequest, resp *proto.RequestVoteResponse) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	defer raft.PersistRaftState()
	log.Printf("Handle vote request: %s", req.String())

	canVote := raft.votedFor == req.CandidateId ||
		(raft.votedFor == None && raft.leaderId == None) ||
		req.Term > raft.curTerm

	if canVote && raft.isUpToDate(req.LastLogIndex, req.LastLogTerm) {
		resp.Term, resp.VoteGranted = raft.curTerm, true
	} else {
		resp.Term, resp.VoteGranted = raft.curTerm, false
		return
	}
	log.Printf("peer %d vote %d", raft.me, req.CandidateId)
	raft.votedFor = req.CandidateId
	// 重置选举超时定时器
	raft.electionTimer.Reset(time.Millisecond * time.Duration(public.MakeAnRandomElectionTimeout(int(raft.baseElecTimeout))))
}

// Append append a new command to it's logs
func (raft *Raft) Append(command []byte) *proto.Entry {
	lastLogIdx := raft.logs.lastIdx
	newLog := &proto.Entry{
		Index: int64(lastLogIdx) + 1,
		Term:  uint64(raft.curTerm),
		Data:  command,
	}
	raft.logs.Append(newLog)
	raft.matchIdx[raft.me] = int(newLog.Index)
	raft.nextIdx[raft.me] = raft.matchIdx[raft.me] + 1
	raft.PersistRaftState()
	return newLog
}

// Propose 用户请求与Raft交互的接口
func (raft *Raft) Propose(payload []byte) (int, int, bool) {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	if raft.role != LEADER {
		return -1, -1, false
	}
	if raft.isSnapshoting {
		return -1, -1, false
	}
	newLog := raft.Append(payload)
	raft.BroadcastAppend()
	return int(newLog.Index), int(newLog.Term), true
}

// ReInitLogs
// make logs to init state
func (rfLog *RaftLog) ReInitLogs() error {
	rfLog.mu.Lock()
	defer rfLog.mu.Unlock()
	log.Printf("start reinitlogs\n")
	// delete all log
	if err := rfLog.db.RaftDelPrefixKeys(string(public.RAFTLOG_PREFIX)); err != nil {
		return err
	}
	// add a empty
	empEnt := &proto.Entry{}
	empEntEncode := EncodeEntry(empEnt)
	rfLog.firstIdx, rfLog.lastIdx = 0, 0
	return rfLog.db.RaftPut(EncodeRaftLogKey(public.INIT_LOG_INDEX), empEntEncode)
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

// ChangeRole change raft node's role to new role
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
	// 为自己投一票
	raft.grantedVotes = 1
	raft.votedFor = int64(raft.me)
	voteReq := &proto.RequestVoteRequest{
		Term:         raft.curTerm,
		CandidateId:  int64(raft.me),
		LastLogIndex: int64(raft.logs.lastIdx),
		LastLogTerm:  int64(raft.logs.GetLast().Term),
	}
	// 改变raft状态
	raft.PersistRaftState()

	for _, peer := range raft.peers {
		if peer.id == uint64(raft.me) || raft.role == LEADER {
			continue
		}
		go func(peer *RaftClientEnd) {
			log.Printf("send request vote to %s %s\n", peer.addr, voteReq.String())
			// 向其他节点发送选举投票请求
			requestVoteResp, err := (*peer.raftServiceCli).RequestVote(context.Background(), voteReq)

			if err != nil {
				log.Printf("send request vote to %s failed %v\n", peer.addr, err.Error())
			}

			if requestVoteResp != nil {
				// 可能会存在多个协程修改同一个非原子变量,故加锁
				raft.mu.Lock()
				defer raft.mu.Unlock()
				log.Printf("send request vote to %s recive -> %s, curterm %d, req term %d\n", peer.addr, requestVoteResp.String(), raft.curTerm, voteReq.Term)
				if raft.curTerm == voteReq.Term && raft.role == CANDIDATE {
					if requestVoteResp.VoteGranted {
						// 获得一票
						log.Println("I got a vote")
						raft.IncrGrantedVotes()
						// 得到超过半数票则成功当选并改变状态向其他节点发送心跳
						if raft.grantedVotes > len(raft.peers)/2 {
							log.Printf("node %d get majority votes int term %d \n", raft.me, raft.curTerm)
							raft.ChangeRole(LEADER)
							raft.BroadcastHeartbeat()
							raft.grantedVotes = 0
						}
					} else if requestVoteResp.Term > raft.curTerm { // 如果有任期大于自己的节点则改变状态为follower
						raft.ChangeRole(FOLLOWER)
						raft.curTerm, raft.votedFor = requestVoteResp.Term, None
						// 保存更正后的新状态
						raft.PersistRaftState()
					}
				}
			}
		}(peer)
	}
}

// BroadcastAppend 向其余节点广播,唤醒负责日志复制操作的协程
func (raft *Raft) BroadcastAppend() {
	for _, peer := range raft.peers {
		if peer.id == uint64(raft.me) {
			continue
		}
		raft.replicatorCond[peer.id].Signal()
	}
}

// BroadcastHeartbeat 向其余节点广播心跳
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

func (raft *Raft) CondInstallSnapshot(lastIncluedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	log.Printf("follower start install snapshot\n")
	if lastIncludedIndex <= int(raft.commitIdx) {
		return false
	}

	if lastIncludedIndex > int(raft.logs.lastIdx) {
		log.Printf("lastIncludedIndex > last log id")
		raft.logs.ReInitLogs()
	} else {
		log.Printf("install snapshot del old log")
		raft.logs.EraseBeforeWithDel(int64(lastIncludedIndex))
	}

	raft.logs.SetEntFirstTermAndIndex(int64(lastIncluedTerm), int64(lastIncludedIndex))

	raft.lastApplied = int64(lastIncludedIndex)
	raft.commitIdx = int64(lastIncludedIndex)

	return true
}

// isUpToDate 是否是新日志
func (raft *Raft) isUpToDate(lastIdx, term int64) bool {
	// 如果任期大于本地最新任期,则直接返回ture
	// 如果任期相同且日志号大于本地最新日志号,则返回ture
	// 否则返回false
	lastTerm := int64(raft.logs.GetLast().Term)
	return term > lastTerm || term == lastTerm && lastIdx >= int64(raft.logs.lastIdx)
}

// Snapshot 通过计算当前level中的日志条目,保存快照
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
	// 通过 EraseBeforeWithDel 删除日志,然后 PersisSnapshot 将快照中状态数据缓存到存储引擎中
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

// HandleInstallSnapshot 从Leader加载快照
func (raft *Raft) HandleInstallSnapshot(request *proto.InstallSnapshotRequest, response *proto.InstallSnapshotResponse) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	response.Term = raft.curTerm

	if request.Term < raft.curTerm {
		return
	}

	if request.Term > raft.curTerm {
		raft.curTerm = request.Term
		raft.votedFor = None
		raft.PersistRaftState()
	}

	raft.ChangeRole(FOLLOWER)
	raft.electionTimer.Reset(time.Millisecond * time.Duration(public.MakeAnRandomElectionTimeout(int(raft.baseElecTimeout))))
	if request.LastIncludedIndex <= raft.commitIdx {
		return
	}

	go func() {
		raft.applyCh <- &proto.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Data,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
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
			// 唤醒做Apply操作的协程
			raft.applyCond.Signal()
		}
	}
}

func (raft *Raft) GetLogCount() int {
	raft.mu.Lock()
	defer raft.mu.Unlock()
	return raft.logs.LogItemCount()
}

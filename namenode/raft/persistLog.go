package raft

import (
	"faydfs/namenode/service"
)

func MakePersistRaftLog(db service.DB) *RaftLog {
	//todo implement me
	panic("need impl")
}

// ReadRaftState
// read the persist curTerm, votedFor for node from storage engine
func (rfLog *RaftLog) ReadRaftState() (curTerm int64, votedFor int64) {
	//todo implement me
	panic("need impl")
}

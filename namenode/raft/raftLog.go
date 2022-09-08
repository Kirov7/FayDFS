package raft

import (
	"faydfs/namenode/service"
	"faydfs/proto"
	"sync"
)

type RaftLog struct {
	mu       sync.RWMutex
	firstIdx uint64
	lastIdx  uint64
	items    []*proto.Entry
	dbEng    service.DB
}

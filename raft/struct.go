package raft

import (
	"log"
)

type stCached struct {
	opts *options
	log  *log.Logger
	cm   *cacheManager
	raft *raftNodeInfo
}

type stCachedContext struct {
	st *stCached
}

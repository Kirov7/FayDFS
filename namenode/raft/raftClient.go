package raft

import (
	"faydfs/proto"
	"google.golang.org/grpc"
)

type RaftClientEnd struct {
	id             uint64
	addr           string
	conns          []*grpc.ClientConn
	raftServiceCli *proto.RaftServiceClient
}

func (raftcli *RaftClientEnd) CloseAllConn() {
	for _, conn := range raftcli.conns {
		conn.Close()
	}
}

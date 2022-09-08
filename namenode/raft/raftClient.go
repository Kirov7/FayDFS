package raft

import "google.golang.org/grpc"

type RaftClientEnd struct {
	id    uint64
	addr  string
	conns []*grpc.ClientConn
	//raftServiceCli *pb.RaftServiceClient
}

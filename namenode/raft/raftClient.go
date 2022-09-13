package raft

import (
	"faydfs/proto"
	"google.golang.org/grpc"
	"log"
)

type RaftClientEnd struct {
	id             uint64
	addr           string
	conns          []*grpc.ClientConn
	raftServiceCli *proto.RaftServiceClient
}

func MakeRaftClientEnd(addr string, id uint64) *RaftClientEnd {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("faild to connect: %v\n", err)
	}
	conns := []*grpc.ClientConn{}
	conns = append(conns, conn)
	rpcClient := proto.NewRaftServiceClient(conn)
	return &RaftClientEnd{
		id:             id,
		addr:           addr,
		conns:          conns,
		raftServiceCli: &rpcClient,
	}
}

func (raftcli *RaftClientEnd) CloseAllConn() {
	for _, conn := range raftcli.conns {
		conn.Close()
	}
}

package raft

import "flag"

type options struct {
	dataDir     string // data directory
	httpAddress string // http server address
	raftAddress string // construct Raft Address
	bootstrap   bool   // start as master or not
	joinAddress string // peer address to join
}

func NewOptions() *options {
	opts := &options{}

	var Address = flag.String("address", "127.0.0.1:6000", "address")
	var raftAddress = flag.String("raft", "127.0.0.1:7000", "raft address")
	var node = flag.String("node", "node1", "raft node name")
	var bootstrap = flag.Bool("bootstrap", false, "start as raft cluster")
	var joinAddress = flag.String("join", "", "join address for raft cluster")
	flag.Parse()

	opts.dataDir = "./" + *node
	opts.httpAddress = *Address
	opts.bootstrap = *bootstrap
	opts.raftAddress = *raftAddress
	opts.joinAddress = *joinAddress
	return opts
}

package main

import (
	"context"
	"faydfs/config"
	"faydfs/proto"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"time"
)

var (
	conf              = config.GetConfig()
	port              = conf.DataNodePort
	nameNodeHostURL   = conf.NameNodeHost + conf.NameNodePort
	heartbeatInterval = conf.HeartbeatInterval
)

type server struct {
	proto.UnimplementedC2DServer
}

func (s server) GetBlock(mode *proto.FileNameAndMode, blockServer proto.C2D_GetBlockServer) error {
	//TODO implement me
	panic("implement me")
}

func (s server) WriteBlock(blockServer proto.C2D_WriteBlockServer) error {
	//TODO implement me
	panic("implement me")
}

func heartBeat() {
	heartbeatDuration := time.Second * time.Duration(heartbeatInterval)
	time.Sleep(heartbeatDuration)
	conn, err := grpc.Dial(nameNodeHostURL, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	// defer conn.Close()
	c := proto.NewD2NClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := c.DatanodeHeartbeat(ctx, &proto.Heartbeat{})
	if err != nil {
		log.Fatalf("did not send heartbeat: %v", err)
	}
	fmt.Println(response)
	heartBeat()
}

func registerDataNode() error {
	conn, err := grpc.Dial(nameNodeHostURL, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := proto.NewD2NClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	registerStatus, err := c.RegisterDataNode(ctx, &proto.RegisterDataNodeReq{New: true})
	if err != nil {
		log.Fatalf("did not register: %v", err)
		return err
	}
	fmt.Println(registerStatus, "registerStatus")
	go heartBeat()
	return nil
}

func main() {

}

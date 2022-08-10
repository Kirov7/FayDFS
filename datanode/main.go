package main

import (
	"context"
	"faydfs/config"
	datanode "faydfs/datanode/service"
	"faydfs/proto"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
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
	b := datanode.GetBlock(mode.FileName, "r")
	// 一直读到末尾，chunk文件块传送
	for b.HasNextChunk() {
		chunk, n, err := b.GetNextChunk()
		if err != nil {
			return err
		}
		blockServer.Send(&proto.File{Content: (*chunk)[:n]})
	}
	b.Close()
	return nil
}

func (s server) WriteBlock(blockServer proto.C2D_WriteBlockServer) error {
	fileWriteStream, err := blockServer.Recv()
	if err == io.EOF {
		blockStatus := proto.OperateStatus{Success: false}
		blockServer.SendAndClose(&blockStatus)
	}
	fileName := fileWriteStream.BlockReplicaList.BlockReplicaList[0].BlockName
	b := datanode.GetBlock(fileName, "w")
	fmt.Println(fileWriteStream, "fileWriteStream")
	file := make([]byte, 0)
	for {
		fileWriteStream, err := blockServer.Recv()
		if err == io.EOF {
			fmt.Println("file", string(file))
			b.Close()
			blockStatus := proto.OperateStatus{Success: true}
			blockServer.SendAndClose(&blockStatus)
			break
		}
		content := fileWriteStream.File.Content
		err = b.WriteChunk(content)
		if err != nil {
			blockStatus := proto.OperateStatus{Success: false}
			blockServer.SendAndClose(&blockStatus)
		}
		file = append(file, content...)
	}
	return nil
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
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	err = registerDataNode()
	if err != nil {
		log.Fatalf("failed to regester to namenode: %v", err)
	}
	proto.RegisterC2DServer(s, &server{})
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

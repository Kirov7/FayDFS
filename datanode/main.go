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

// GetIP 获取本机IP
func GetIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}

// 全局变量作为本机的所有blockList
var blockList = []*proto.BlockLocation{}

// GetBlock 读chunk
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

// WriteBlock 写chunk
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
	// 更新List
	blockList = append(blockList,
		&proto.BlockLocation{
			BlockName: fileName,
			IpAddr:    string(GetIP()),
			BlockSize: b.GetFileSize()})
	return nil
}

// heartBeat 心跳，递归实现
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

// TODO: 完善report中的blockReplicaList
// blockReport 定时报告状态
func blockReport() {
	heartbeatDuration := time.Second * time.Duration(heartbeatInterval)
	time.Sleep(heartbeatDuration * 20)
	conn, err := grpc.Dial(nameNodeHostURL, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	// defer conn.Close()
	c := proto.NewD2NClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// 添加blockList
	response, err := c.BlockReport(ctx, &proto.BlockReplicaList{BlockReplicaList: blockList})
	if err != nil {
		log.Fatalf("did not send heartbeat: %v", err)
	}
	fmt.Println(response)
	blockReport()
}

// 注册DataNode
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
	go blockReport()
	return nil
}

// 启动DataNode1
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

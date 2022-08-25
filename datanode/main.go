package main

import (
	"context"
	"encoding/json"
	"faydfs/config"
	message2 "faydfs/datanode/message"
	datanode "faydfs/datanode/service"
	"faydfs/proto"
	"fmt"
	"github.com/shirou/gopsutil/v3/disk"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	conf            = config.GetConfig()
	port            = conf.DataNodePort
	nameNodeHostURL = conf.NameNodeHost + conf.NameNodePort
	//nameNodeHostURL   = "localhost" + conf.NameNodePort
	heartbeatInterval = conf.HeartbeatInterval
)

// GetIP 获取本机IP
func GetIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return strings.Split(localAddr.String(), ":")[0]
}

// GetDiskUsage 获取空余磁盘容量
func GetDiskUsage(path string) uint64 {
	di, err := disk.Usage(path)
	if err != nil {
		fmt.Println(err, "err")
	}
	return di.Free
}

// 全局变量作为本机的所有blockList
var blockList = []*proto.BlockLocation{}

// 维护本地Port变量
var localNodeIP string
var localRepIP string

// server
type server struct {
	proto.UnimplementedC2DServer
	proto.UnimplementedN2DServer
}

// GetBlock 读chunk
func (s server) GetBlock(mode *proto.FileNameAndMode, stream proto.C2D_GetBlockServer) error {
	b := datanode.GetBlock(mode.FileName, "r")

	// 一直读到末尾，chunk文件块传送
	for b.HasNextChunk() {
		chunk, n, err := b.GetNextChunk()
		if err != nil {
			return err
		}
		stream.Send(&proto.File{Content: (*chunk)[:n]})
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
	fmt.Println("write success")
	// 更新List
	blockList = append(blockList,
		&proto.BlockLocation{
			BlockName:    fileName,
			IpAddr:       localNodeIP,
			BlockSize:    b.GetFileSize(),
			ReplicaState: proto.BlockLocation_ReplicaCommitted,
			ReplicaID:    1,
		})
	return nil
}

// ReloadReplica 复制转移block
func (s server) ReloadReplica(ctx context.Context, info *proto.CopyReplica2DN) (*proto.OperateStatus, error) {
	// TODO Attention:为了方便单机调试，采用端口
	new := info.NewIP[len(info.NewIP)-1:]
	err := ReplicateBlock(info.BlockName, "localhost:5000"+new)
	if err != nil {
		return &proto.OperateStatus{Success: false}, err
	}
	return &proto.OperateStatus{Success: true}, nil
}

// GetBlockReport 获取块的信息
func (s server) GetBlockReport(ctx context.Context, ping *proto.Ping) (*proto.BlockReplicaList, error) {
	return &proto.BlockReplicaList{BlockReplicaList: blockList}, nil
}

// heartBeat 心跳，递归实现
func heartBeat(currentPort string) {
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
	// ATTENTION:需要根据磁盘路径获取空余量，所以默认为D盘
	beat := proto.Heartbeat{IpAddr: currentPort, DiskUsage: GetDiskUsage("D:/")}
	response, err := c.DatanodeHeartbeat(ctx, &beat)
	if err != nil {
		log.Fatalf("did not send heartbeat: %v", err)
	}
	fmt.Printf("response from %v\n", response)
	heartBeat(currentPort)
}

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
func registerDataNode(currentPort string) error {
	fmt.Println("register")
	conn, err := grpc.Dial(nameNodeHostURL, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return err
	}
	defer conn.Close()
	c := proto.NewD2NClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	registerStatus, err := c.RegisterDataNode(ctx, &proto.RegisterDataNodeReq{New: true, DiskUsage: GetDiskUsage("D:/"), IpAddr: currentPort})
	if err != nil {
		log.Fatalf("did not register: %v", err)
		return err
	}
	fmt.Println(registerStatus, "registerStatus")
	go heartBeat(currentPort)
	go blockReport()
	return nil
}

// PipelineServer Replicate the datanode to another
func PipelineServer(currentPort string) {
	localRepIP = currentPort
	fmt.Println("start server...")
	var mu sync.Mutex //创建锁,防止协程将连接的传输写入同一个文件中
	listener, err := net.Listen("tcp", currentPort)
	if err != nil {
		fmt.Println("listen failed,err:", err)
		return
	}
	//接受客户端信息
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept failed,err:", err)
			continue
		}
		//用协程建立连接
		go process(conn, mu)
	}
}

// process DataNode处理函数
func process(conn net.Conn, mu sync.Mutex) {
	mu.Lock() // 并发安全，防止协程同时写入
	defer mu.Unlock()
	defer conn.Close()
	for {
		buf := make([]byte, 10240)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("read err:", err)
			return
		}
		// 解析buf
		var message message2.Message
		err = json.Unmarshal(buf[0:n], &message)
		if err != nil {
			fmt.Println("unmarshal error: ", err)
		}
		// 处理
		if message.Mode == "send" { // NameNode发来任务
			ReplicateBlock(message.BlockName, message.IpAddr)
		} else if message.Mode == "receive" { // DataNode接受信息
			ReceiveReplicate(message.BlockName, message.Content)
		} else if message.Mode == "delete" { // DataNode删除文件
			err := os.Remove(conf.DataDir + "/" + message.BlockName)
			if err != nil {
				return
			}
		}
	}
}

// ReplicateBlock 副结点向新节点发送备份文件
func ReplicateBlock(blockName string, ipAddress string) error {
	//log.Println("DataNode1 接受 NameNode 指令，向DataNode2备份")
	conn, err := net.DialTimeout("tcp", ipAddress, 5*time.Second)
	defer conn.Close()
	if err != nil {
		fmt.Println("Error dialing", err.Error())
		return err
	}
	// 获得Block
	b := datanode.GetBlock(blockName, "r")
	// 构建Message对象并序列化
	m := message2.Message{Mode: "receive", BlockName: blockName, Content: b.LoadBlock()}
	mb, err := json.Marshal(m)
	if err != nil {
		fmt.Println("Error marshal", err.Error())
		return err
	}
	// 传输数据
	conn.Write(mb)
	return nil
}

// ReceiveReplicate 写入备份文件
func ReceiveReplicate(blockName string, content []byte) {
	log.Println("DataNode2接受到DataNode1数据，在本地有相关block备份")
	b := datanode.GetBlock(blockName, "w")
	b.Write(content)
}

// DeleteReplicate 删除备份文件
func DeleteReplicate(blockName string) {
	b := datanode.GetBlock(blockName, "w")
	b.DeleteBlock()
}

// RunDataNode 启动DataNode
func RunDataNode(currentPort string) {
	localNodeIP = currentPort
	lis, err := net.Listen("tcp", currentPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	err = registerDataNode(currentPort)
	if err != nil {
		log.Fatalf("failed to regester to namenode: %v", err)
	}
	proto.RegisterC2DServer(s, &server{})
	proto.RegisterN2DServer(s, &server{})
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// 启动DataNode
func main() {
	// 新建数据文件夹
	os.Mkdir("data", 7050)

	// 启动DataNode交互服务
	go PipelineServer("localhost:50000")
	//go PipelineServer("localhost:50001")
	// 本地开启若干DataNode
	go RunDataNode("localhost:8010")
	//go RunDataNode("localhost:8011")

	// 防止因为main中止造成协程中止
	defer func() {
		select {}
	}()
}

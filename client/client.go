package client

import (
	"bytes"
	"context"
	"faydfs/client/service"
	"faydfs/config"
	"faydfs/proto"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log"
	"time"
)

var (
	conf         = config.GetConfig()
	address      = conf.NameNodeHost + conf.NameNodePort
	datenodePort = conf.DataNodePort
	blocksize    = conf.BlockSize
)

type Client struct {
}

func (c *Client) Put(localFilePath, remoteFilePath string) service.Result {

	return service.Result{}
}

func (c *Client) Get(remoteFilePath, localFilePath string) service.Result {
	return service.Result{}
}

func (c *Client) Delete(remoteFilePath string) service.Result {
	return service.Result{}
}

func (c *Client) Stat(remoteFilePath string) service.Result {
	return service.Result{}
}

func (c *Client) Rename(renameSrcPath, renameDestPath string) service.Result {
	return service.Result{}
}
func (c *Client) Mkdir(remoteFilePath string) service.Result {
	return service.Result{}
}

func (c *Client) List(remoteDirPath string) service.Result {
	return service.Result{}
}

func getGrpcC2NConn(address string) (*grpc.ClientConn, *proto.C2NClient, *context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, address, grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect to %v error %v", address, err)
	}
	client := proto.NewC2NClient(conn)
	return conn, &client, &cancel, err
}

func getGrpcC2DConn(address string) (*grpc.ClientConn, *proto.C2DClient, *context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, address, grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect to %v error %v", address, err)
	}
	client := proto.NewC2DClient(conn)
	return conn, &client, &cancel, err
}

// 整合readBlock的分片返回上层
func read(remoteFilePath string) []byte {
	//1. 调用getFileLocation从namenode读取文件在datanode中分片位置的数组
	filelocationarr := getFileLocation(remoteFilePath)
	blocklist := filelocationarr.FileBlocksList
	file := make([]byte, 0)
	for _, blockreplicas := range blocklist {
		replicalist := blockreplicas.BlockReplicaList
		for _, block := range replicalist {
			tempblock := readBlock(block.BlockName, block.IpAddr)
			file = append(file, tempblock...)
		}
	}
	//2. 按照分片数组的位置调用readBlock循环依次读取
	return []byte{}
}

// 连接dn,读取文件内容
func readBlock(chunkName, ipAddr string) []byte {
	//1. 获取rpc连接
	conn, client, cancel1, _ := getGrpcC2DConn(ipAddr + datenodePort)
	defer (*cancel1)()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fileSteam, err := (*client).GetBlock(ctx, &proto.FileNameAndMode{FileName: chunkName})
	if err != nil {
		log.Fatalf("error getting block %v", err)
	}
	chunkDate := bytes.Buffer{}
	for {
		res, err := fileSteam.Recv()
		if err == io.EOF {
			return chunkDate.Bytes()
		}
		if err != nil {
			log.Fatal("cannot receive response: ", err)
		}
		chunkDate.Write(res.GetContent())
	}
}

// 连接nn,获取文件路径
func getFileLocation(fileName string) *proto.FileLocationArr {
	conn, client, _, _ := getGrpcC2NConn(address)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	filelocationarr, err := (*client).GetFileLocationAndModifyMeta(ctx, &proto.FileNameAndMode{FileName: fileName, Mode: proto.FileNameAndMode_READ})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	return filelocationarr
}

// 返回申请的locationArray
func createFileNameNode(fileName string) *proto.FileLocationArr {
	conn, client, cancel1, _ := getGrpcC2NConn(address + datenodePort)
	defer conn.Close()
	defer (*cancel1)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	filelocationarr, err := (*client).CreateFile(ctx, &proto.FileNameAndMode{FileName: fileName, Mode: proto.FileNameAndMode_READ})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	return filelocationarr
}

// 连接nn,创建文件/目录
func createFile(file string) error {
	filelocation := createFileNameNode(file)
	fileblocks := filelocation.FileBlocksList
	blockreplicas := fileblocks[0]
	writeBlock(file, blockreplicas.BlockReplicaList[0].IpAddr, make([]byte, 0), blockreplicas)
	for _, replica := range blockreplicas.BlockReplicaList {
		fmt.Println(replica.IpAddr, "IpAddress")
		fmt.Println(replica.BlockSize, "BlockName")
	}
	return nil
}

// 控制writeBlock写入文件
func write(fileName string, data []byte) bool {
	for len(data) > 0 {
		filelocation := getFileLocation(fileName)
		blockreplicas := filelocation.FileBlocksList[0]
		blockreplicity := blocksize - blockreplicas.BlockReplicaList[0].BlockSize
		limit := int64(len(data))
		if blockreplicity > int64(len(data)) {
			limit = blockreplicity
		}
		writeBlock(fileName, blockreplicas.BlockReplicaList[0].IpAddr, data[0:limit], blockreplicas)
		data = data[limit:len(data)]
	}
	return false
}

// 连接dn,在块上写数据
func writeBlock(chunkName string, ipAddr string, data []byte, blockReplicaList *proto.BlockReplicaList) error {
	conn, client, _, _ := getGrpcC2DConn(ipAddr + datenodePort)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writeBlockClient, err := (*client).WriteBlock(ctx)
	if err != nil {
		return err
	}
	sentdatelength := 0
	chunkSize := 50
	err = writeBlockClient.Send(&proto.FileWriteStream{BlockReplicaList: blockReplicaList})
	if err != nil {
		return err
	}
	for sentdatelength < len(data) {
		max := (sentdatelength + chunkSize)
		if max > len(data) {
			max = len(data)
		}
		chunk := data[sentdatelength:max]
		_ = writeBlockClient.Send(&proto.FileWriteStream{File: &proto.File{Content: chunk}})
		sentdatelength = chunkSize + sentdatelength
	}
	blockstatus, error := writeBlockClient.CloseAndRecv()
	fmt.Println(blockstatus)
	if error != nil {
		return error
	}
	return nil
}

// 连接nn,调用方法延续租约
func renewLease(fileName string) {

}

package client

import (
	"context"
	"faydfs/client/service"
	"faydfs/proto"
<<<<<<< Updated upstream
	"google.golang.org/grpc"
=======
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
)

var (
	conf         = config.GetConfig()
	address      = conf.NameNodeHost + conf.NameNodePort
	datenodePort = conf.DataNodePort
	blocksize    = conf.BlockSize
>>>>>>> Stashed changes
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

func (c *Client) getGrpcC2NConn(address string) (*grpc.ClientConn, *proto.C2NClient, *context.CancelFunc, error) {
	return nil, nil, nil, nil
}

func (c *Client) getGrpcC2DConn(address string) (*grpc.ClientConn, *proto.C2DClient, *context.CancelFunc, error) {
	return nil, nil, nil, nil
}

// 整合readBlock的分片返回上层
func read(remoteFilePath string) []byte {
	//1. 调用getFileLocation从namenode读取文件在datanode中分片位置的数组
	//2. 按照分片数组的位置调用readBlock循环依次读取
	return []byte{}
}

// 连接dn,读取文件内容
func readBlock(chunkName, ipAddr string) []byte {
	//1. 获取rpc连接
	//2. 调用远程方法获取文件的[]byte数组
	return []byte{}
}

// 连接nn,获取文件路径
func getFileLocation(fileName string) *proto.FileLocationArr {
	return nil
}

// 连接nn,创建文件/目录
func createFile(file string) *proto.FileLocationArr {
	return nil
}

// 控制writeBlock写入文件
func write(fileName string, data []byte) bool {
	return false
}

// 连接dn,在块上写数据
func writeBlock(chunkName string, ipAddr string, data []byte, blockReplicaList *proto.BlockReplicaList) error {
	return nil
}

// 连接nn,调用方法延续租约
func renewLease(fileName string) {

}

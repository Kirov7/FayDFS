package service

import (
	"encoding/json"
	"faydfs/client"
	"faydfs/datanode/message"
	"faydfs/proto"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// t *testing.T

func Read() {
	tempFile := "data/temp.txt"
	tempContent := "here is some temp content"
	createTempFile(tempFile, tempContent)
	fmt.Println("create file success")
	b := GetBlock("temp.txt", "r")
	temp := make([]byte, 0)
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	if tempContent != string(temp) {
		fmt.Println("read failed")
	}
	//deleteTempFile(tempFile)
	fmt.Println("read success")
}

func read() {
	tempFile := "data/temp.txt"
	tempContent := "here is some temp content"
	createTempFile(tempFile, tempContent)
	b := GetBlock("temp.txt", "r")
	temp := make([]byte, 0)
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	if tempContent != string(temp) {
		fmt.Println("read failed")
	}
	//deleteTempFile(tempFile)
	fmt.Println("read success")
	// Begin to read
	b = GetBlock("temp.txt", "w")
	fileBytes := []byte(tempContent)
	iter := 0
	for iter < len(fileBytes) {
		end := iter + conf.IoSize
		if end > len(fileBytes) {
			end = len(fileBytes)
		}
		chunk := fileBytes[iter:end]
		fmt.Println("begin write")
		err := b.WriteChunk(chunk)
		fmt.Println("write done")
		if err != nil {
			fmt.Println(err)
		}
		iter = iter + conf.IoSize
	}
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	fmt.Println(string(temp))
}

func TestReadTwice(t *testing.T) {
	Read()
	Write()
}

func createTempFile(name string, content string) {
	file, err := os.Create(name)
	if err != nil {
		log.Fatal("cannot create file: ", err)
	}
	_, err1 := file.Write([]byte(content))
	if err1 != nil {
		log.Fatal("cannot write file: ", err1)
	}
}

func deleteTempFile(name string) {
	err := os.Remove(name)
	if err != nil {
		log.Fatal("cannot delete file: "+name, err)
	}
}

func Write() {
	tempFile := "temp.txt"
	tempContent := "here is some temp content"
	// createTempFile(tempFile, tempContent)
	b := GetBlock(tempFile, "w")
	fileBytes := []byte(tempContent)
	iter := 0
	for iter < len(fileBytes) {
		end := iter + conf.IoSize
		if end > len(fileBytes) {
			end = len(fileBytes)
		}
		chunk := fileBytes[iter:end]

		err := b.WriteChunk(chunk)
		if err != nil {
			fmt.Println(err)
		}
		iter = iter + conf.IoSize
	}

	b = GetBlock(tempFile, "r")
	temp := make([]byte, 0)
	for b.HasNextChunk() {
		chunk, size, _ := b.GetNextChunk()
		temp = append(temp, (*chunk)[:size]...)
	}
	tempContent += tempContent
	if tempContent != string(temp) {
		fmt.Println("write failed")
	}
	fmt.Println("write success")
}

//func getGrpcClientConn(address string) (*grpc.ClientConn, *proto.DfsClient, *context.CancelFunc, error) {
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
//	// conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
//	if err != nil {
//		log.Fatalf("did not connect to %v error %v", address, err)
//	}
//	client := proto.NewDfsClient(conn)
//	return conn, &client, &cancel, err
//}

// TestReadRpc 读文件测试
func TestReadRpc(t *testing.T) {
	//b := client.ReadBlock("temp.txt", "localhost")
	//fmt.Println(string(b))
}

// TestWriteRpc 写文件测试
func TestWriteRpc(t *testing.T) {
	// 基础数据
	content := "Love you my dear Lina~"
	tranport := []byte(content)
	// 定义replicaList
	var blockList = []*proto.BlockLocation{}
	blockList = append(blockList, &proto.BlockLocation{
		BlockName: "lina.txt",
		IpAddr:    string("localhost")})
	v := proto.BlockReplicaList{BlockReplicaList: blockList}
	// 调用
	client.DwriteBlock("192.168.1.107", tranport, &v)
}

func TestAnother(t *testing.T) {
	// 链接DataNode1的socket服务
	conn, err := net.DialTimeout("tcp", "localhost:50000", 5*time.Second)
	if err != nil {
		fmt.Println("Error dialing", err.Error())
		return
	}
	defer conn.Close()
	// 发送数据，使得DataNode1去备份给DataNode2
	me := message.Message{Mode: "send", BlockName: "love.txt", IpAddr: "localhost:50001"}
	se, err := json.Marshal(me)
	if err != nil {
		fmt.Println("Error marshal", err.Error())
	}
	conn.Write(se)
}

// TestDelete 测试删除功能
func TestDelete(t *testing.T) {
	// 链接DataNode1的socket服务
	conn, err := net.DialTimeout("tcp", "localhost:50000", 5*time.Second)
	if err != nil {
		fmt.Println("Error dialing", err.Error())
		return
	}
	defer conn.Close()
	// 发送数据，使得DataNode1去备份给DataNode2
	me := message.Message{Mode: "delete", BlockName: "temp.txt", IpAddr: "localhost:50000"}
	se, err := json.Marshal(me)
	if err != nil {
		fmt.Println("Error marshal", err.Error())
	}
	conn.Write(se)
}

// TestStringIP 测试IP转string
func TestStringIP(test *testing.T) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip := strings.Split(localAddr.String(), ":")[0]
	fmt.Println(ip)
}

// TestClientPut 测试用户请求
func TestClientPut(t *testing.T) {
	user := client.GetClient()
	result := user.Put("D:\\Documents\\QQdoc\\614300076\\FileRecv\\info.txt", "/info.txt")
	if result.ResultCode != 200 {
		log.Fatal(result.Data)
		return
	}
	fmt.Println(result.ResultExtraMsg)
}

func TestClientGet(t *testing.T) {
	client.GetClient()
	user := client.GetClient()
	fmt.Println(":haah")
	result := user.Get("/info.txt", "D://info.txt")
	if result.ResultCode != 200 {
		log.Fatal(result.Data)
		return
	}
	fmt.Println(result.ResultExtraMsg)
}

// 宕机的DataNode
func TestDownDataNode(t *testing.T) {

}

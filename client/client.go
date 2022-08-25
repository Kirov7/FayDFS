package client

import (
	"bytes"
	"context"
	"faydfs/client/service"
	"faydfs/config"
	"faydfs/proto"
	"fmt"
	"github.com/satori/go.uuid"
	"google.golang.org/grpc"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

var (
	conf           = config.GetConfig()
	address        = conf.NameNodeHost + conf.NameNodePort
	datenodePort   = conf.DataNodePort
	blocksize      = conf.BlockSize
	leaselimit     = conf.LeaseSoftLimit
	replica        = conf.Replica
	blocknum       int64
	renewleaseExit bool      //采用全局变量结束续约协程
	clint          = Client{ // uuid生成唯一标识
		clientname: uuid.NewV4().String(),
	}
)

type Client struct {
	clientname string
}

func GetClient() Client {
	return clint
}
func (c *Client) Put(localFilePath, remoteFilePath string) service.Result {
	//io打开文件查看文件大小
	date, err := ioutil.ReadFile(localFilePath)
	size, _ := os.Stat(localFilePath) //stat方法来获取文件信息
	var filesize = size.Size()
	if int64(len(date))%blocksize != 0 {
		blocknum = (int64(len(date)) / blocksize) + 1
	} else {
		blocknum = int64(len(date)) / blocksize
	}
	if err != nil {
		log.Fatalf("not found localfile")
	}
	fmt.Println("client put BlockNum: ", blocknum)
	fmt.Println("client put data: ", date)
	//将字节流写入分布式文件系统
	//未putsuccess前自动周期续约
	ticker := time.NewTicker(time.Second * time.Duration(leaselimit/2)) // 创建半个周期定时器

	fmt.Println("time===========", time.Duration(leaselimit/2))
	//运行续约协程执行周期续约
	//ticker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				go func() {
					renewLease(localFilePath, clint.clientname)
					if renewleaseExit {
						return
					}
				}()
			}
		}
	}()
	filelocationarr, isture := write(remoteFilePath, date, blocknum)
	// write成功
	if isture {
		// 告知metanode,datanode数据传输完成
		conn, client, _, _ := getGrpcC2NConn(address)
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stuats, err := (*client).PutSuccess(ctx, &proto.MetaStore{
			ClientName:      clint.clientname,
			FilePath:        remoteFilePath,
			FileLocationArr: filelocationarr,
			FileSize:        uint64(filesize),
		})

		if err != nil {
			log.Fatalf("write could not greet: %v", err)
		}
		//put成功
		if stuats.Success {
			//立即停止续约协程
			renewleaseExit = true
			return service.Result{
				ResultCode:     200,
				ResultExtraMsg: " successes put",
				Data:           stuats,
			}
		} else { // put失败
			return service.Result{
				ResultCode:     500,
				ResultExtraMsg: "fail put",
				Data:           err,
			}
		}
	} else { // write失败
		//log.Fatalf("fail put")
		return service.Result{
			ResultCode:     500,
			ResultExtraMsg: "fail put",
			Data:           err,
		}
	}
}

func (c *Client) Get(remoteFilePath, localFilePath string) service.Result {
	date := read(remoteFilePath)
	localfile, err := os.Create(localFilePath)
	if err != nil {
		log.Fatalf("create localfile fail")
	}
	defer localfile.Close()
	fmt.Println("client get data: ", date)
	_, err = localfile.Write(date)
	if err != nil {
		//log.Fatalf("write to local fail")
		return service.Result{
			ResultCode:     500,
			ResultExtraMsg: "write to local fail",
			Data:           err,
		}
	}
	return service.Result{
		ResultCode:     200,
		ResultExtraMsg: "write to local success",
		Data:           localFilePath,
	}
}

func (c *Client) Delete(remoteFilePath string) service.Result {
	conn, client, _, _ := getGrpcC2NConn(address)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := (*client).OperateMeta(ctx, &proto.FileNameAndOperateMode{FileName: remoteFilePath, Mode: proto.FileNameAndOperateMode_DELETE})
	if err != nil {
		return service.Result{
			ResultCode:     500,
			ResultExtraMsg: "delete fail",
			Data:           err,
		}
	}
	return service.Result{
		ResultCode:     200,
		ResultExtraMsg: "delete success",
		Data:           status.Success,
	}
}

func (c *Client) Stat(remoteFilePath string) service.Result {
	conn, client, _, _ := getGrpcC2NConn(address)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := (*client).GetFileMeta(ctx, &proto.PathName{PathName: remoteFilePath})
	if err != nil {
		return service.Result{
			ResultCode:     500,
			ResultExtraMsg: "stat fail",
			Data:           err,
		}
	}
	return service.Result{
		ResultCode:     200,
		ResultExtraMsg: "stat success",
		Data:           status,
	}
}

func (c *Client) Rename(renameSrcPath, renameDestPath string) service.Result {
	//限制rename
	src := strings.Split(renameSrcPath, "\\")
	des := strings.Split(renameDestPath, "\\")
	if len(src) != len(des) {
		log.Fatalf("you can not change dir")
		return service.Result{}
	} else {
		for i := 0; i < len(src)-1; i++ {
			if src[i] != des[i] {
				log.Fatalf("you can not change dir")
				return service.Result{}
			}
		}
	}
	conn, client, _, _ := getGrpcC2NConn(address)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := (*client).RenameFileInMeta(ctx, &proto.SrcAndDestPath{RenameSrcPath: renameSrcPath, RenameDestPath: renameDestPath})
	if err != nil {
		return service.Result{
			ResultCode:     500,
			ResultExtraMsg: "rename fail",
			Data:           err,
		}
	}
	return service.Result{
		ResultCode:     200,
		ResultExtraMsg: "reneme success",
		Data:           status.Success,
	}
}

func (c *Client) Mkdir(remoteFilePath string) service.Result {
	conn, client, _, _ := getGrpcC2NConn(address)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := (*client).OperateMeta(ctx, &proto.FileNameAndOperateMode{FileName: remoteFilePath, Mode: proto.FileNameAndOperateMode_MKDIR})
	if err != nil {
		return service.Result{
			ResultCode:     500,
			ResultExtraMsg: "mkdir fail",
			Data:           err,
		}
	}
	return service.Result{
		ResultCode:     200,
		ResultExtraMsg: "mkdir success",
		Data:           status.Success,
	}
}

func (c *Client) List(remoteDirPath string) service.Result {
	conn, client, _, _ := getGrpcC2NConn(address)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := (*client).GetDirMeta(ctx, &proto.PathName{PathName: remoteDirPath})
	if err != nil {
		return service.Result{
			ResultCode:     500,
			ResultExtraMsg: "show list fail",
			Data:           err,
		}
	}
	return service.Result{
		ResultCode:     200,
		ResultExtraMsg: "show list success",
		Data:           status,
	}
}

func getGrpcC2NConn(address string) (*grpc.ClientConn, *proto.C2NClient, *context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	conn, err := grpc.DialContext(ctx, address, grpc.WithBlock(), grpc.WithInsecure())
	//conn2, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Println("did not connect to %v error %v", address, err)
	}
	client := proto.NewC2NClient(conn)
	return conn, &client, &cancel, err
}

func getGrpcC2DConn(address string) (*grpc.ClientConn, *proto.C2DClient, *context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	// 加入了WithInsecure
	conn, err := grpc.DialContext(ctx, address, grpc.WithBlock(), grpc.WithInsecure())
	//conn2, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Println("C2N did not connect to %v error %v", address, err)
	}
	fmt.Println("conn======", conn)
	client := proto.NewC2DClient(conn)
	return conn, &client, &cancel, err
}

// 整合readBlock的分片返回上层
func read(remoteFilePath string) []byte {
	//1. 调用getFileLocation从namenode读取文件在datanode中分片位置的数组
	//这里传的是无用的参数blocknum:3
	filelocationarr := getFileLocation(remoteFilePath, proto.FileNameAndMode_READ, blocknum)
	blocklist := filelocationarr.FileBlocksList
	file := make([]byte, 0)
	for _, blockreplicas := range blocklist {
		replicalist := blockreplicas.BlockReplicaList
		for j, block := range replicalist {
			fmt.Println(block.IpAddr)
			tempblock, err := ReadBlock(block.BlockName, block.IpAddr)
			if err != nil {
				if j == replica-1 {
					log.Fatal("datanode doesn't work")
					return nil
				}
				continue
			}
			file = append(file, tempblock...)
			break
		}
	}
	//2. 按照分片数组的位置调用readBlock循环依次读取
	return file
}

// 连接dn,读取文件内容
func ReadBlock(chunkName, ipAddr string) ([]byte, error) {
	//1. 获取rpc连接
	conn, client, cancel1, err := getGrpcC2DConn(ipAddr)
	if conn == nil {
		return nil, err
	}
	defer (*cancel1)()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	fmt.Println("======client：", *client)
	fileSteam, err := (*client).GetBlock(ctx, &proto.FileNameAndMode{FileName: chunkName})
	if err != nil {
		log.Fatalf("error getting block %v", err)
	}
	chunkDate := bytes.Buffer{}
	for {
		res, err := fileSteam.Recv()
		if err == io.EOF {
			return chunkDate.Bytes(), nil
		} else if err != nil {
			return nil, err
		}
		chunkDate.Write(res.GetContent())
	}
}

// 连接nn,获取文件路径
func getFileLocation(fileName string, mode proto.FileNameAndMode_Mode, blocknum int64) *proto.FileLocationArr {
	conn, client, _, _ := getGrpcC2NConn(address)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	//blockname应该加在这里，所以block要从外层函数依次传参。对应的blocknum在read和wirte的mode都需要传入，但是只有write_Mode时blocknum才有用。
	filelocationarr, err := (*client).GetFileLocationAndModifyMeta(ctx, &proto.FileNameAndMode{FileName: fileName, Mode: mode, BlockNum: blocknum})
	if err != nil {
		log.Fatalf("getFileLocation could not greet: %v", err)
	}
	return filelocationarr
}

// 返回申请的locationArray
func createFileNameNode(fileName string) *proto.FileLocationArr {
	conn, client, cancel1, _ := getGrpcC2NConn(address)
	defer conn.Close()
	defer (*cancel1)()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	filelocationarr, err := (*client).CreateFile(ctx, &proto.FileNameAndMode{FileName: fileName, Mode: proto.FileNameAndMode_WRITE})
	if err != nil {
		log.Fatalf("createFil could not greet: %v", err)
	}
	return filelocationarr
}

// 连接nn,创建文件/目录
func createFile(file string) error {
	filelocation := createFileNameNode(file)
	fileblocks := filelocation.FileBlocksList
	blockreplicas := fileblocks[0]
	_ = DwriteBlock(blockreplicas.BlockReplicaList[0].IpAddr, make([]byte, 0), blockreplicas)
	for _, replica := range blockreplicas.BlockReplicaList {
		fmt.Println(replica.IpAddr, "IpAddress")
		fmt.Println(replica.BlockSize, "BlockName")
	}
	return nil
}

// 控制writeBlock写入文件
//更改返回值使返回flielocationarr
func write(fileName string, data []byte, blocknum int64) (*proto.FileLocationArr, bool) {
	//filelocation := getFileLocation(fileName, proto.FileNameAndMode_WRITE, blocknum)
	//
	//fmt.Println("==========init datalen: ", len(data), " blocknums:", blocknum)
	//for i := 0; i < int(blocknum); i++ {
	//	//getFileLocation应该有第二个写入参数
	//	blockreplicas := filelocation.FileBlocksList[i]
	//	fmt.Println("==========filelocation: ", filelocation)
	//	fmt.Println("==========blockreplicas: ", blockreplicas)
	//	// TODO
	//	//blockreplicity := blocksize - blockreplicas.BlockReplicaList[0].BlockSize
	//	//limit := int64(len(data))
	//	//if blockreplicity > int64(len(data)) {
	//	//	limit = blockreplicity
	//	//}
	//	//filelocation:
	//	//FileBlocksList:{
	//	//	BlockReplicaList:{ipAddr:"localhost:8011" blockName:"test_1661353986_0" blockSize:128}
	//	//	BlockReplicaList:{ipAddr:"localhost:8012" blockName:"test_1661353986_1" blockSize:128 replicaID:1}
	//	//	BlockReplicaList:{ipAddr:"localhost:8010" blockName:"test_1661353986_2" blockSize:128 replicaID:2}
	//	//}
	//	//FileBlocksList:{
	//	//	BlockReplicaList:{ipAddr:"localhost:8011" blockName:"test_1661353986_0" blockSize:128}
	//	//	BlockReplicaList:{ipAddr:"localhost:8012" blockName:"test_1661353986_1" blockSize:128 replicaID:1}
	//	//	BlockReplicaList:{ipAddr:"localhost:8010" blockName:"test_1661353986_2" blockSize:128 replicaID:2}}
	//	//FileBlocksList:{
	//	//	BlockReplicaList:{ipAddr:"localhost:8011" blockName:"test_1661353986_0" blockSize:128}
	//	//	BlockReplicaList:{ipAddr:"localhost:8012" blockName:"test_1661353986_1" blockSize:128 replicaID:1}
	//	//	BlockReplicaList:{ipAddr:"localhost:8010" blockName:"test_1661353986_2" blockSize:128 replicaID:2}}
	//	if i == int(blocknum)-1 {
	//		_ = DwriteBlock(blockreplicas.BlockReplicaList[1].IpAddr, data[i*int(blocksize):], blockreplicas)
	//		fmt.Println("=======================写入最后一个文件: ", string(data[i*int(blocksize):]))
	//		fmt.Println("ip :", blockreplicas.BlockReplicaList[1].IpAddr, "blockSize: ", blockreplicas.BlockReplicaList[1].BlockName)
	//	} else {
	//		_ = DwriteBlock(blockreplicas.BlockReplicaList[2].IpAddr, data[i*int(blocksize):(i+1)*int(blocksize)], blockreplicas)
	//		fmt.Println("=======================写入第", i, "个文件: ", string(data[i*int(blocksize):(i+1)*int(blocksize)]))
	//		fmt.Println("ip :", blockreplicas.BlockReplicaList[2].IpAddr, "blockSize: ", blockreplicas.BlockReplicaList[2].BlockName)
	//	}
	//}
	//
	//return filelocation, true

	filelocation := getFileLocation(fileName, proto.FileNameAndMode_WRITE, blocknum)
	fmt.Println("==========init datalen: ", len(data), " blocknums:", blocknum)
	for i := 0; i < int(blocknum); i++ {
		blockreplicas := filelocation.FileBlocksList[i]
		fmt.Println("==========filelocation: ", filelocation)
		fmt.Println("==========blockreplicas: ", blockreplicas)
		if i == int(blocknum)-1 {
			for j := 0; j < len(blockreplicas.BlockReplicaList); j++ {
				_ = DwriteBlock(blockreplicas.BlockReplicaList[j].IpAddr, data[i*int(blocksize):], blockreplicas)
				fmt.Println("=======================写入最后一个文件: ", string(data[i*int(blocksize):]))
				fmt.Println("ip :", blockreplicas.BlockReplicaList[j].IpAddr, "blockSize: ", blockreplicas.BlockReplicaList[1].BlockName)
			}
		} else {
			for j := 0; j < len(blockreplicas.BlockReplicaList); j++ {
				_ = DwriteBlock(blockreplicas.BlockReplicaList[j].IpAddr, data[i*int(blocksize):(i+1)*int(blocksize)], blockreplicas)
				fmt.Println("=======================写入第", i, "个文件: ", string(data[i*int(blocksize):(i+1)*int(blocksize)]))
				fmt.Println("ip :", blockreplicas.BlockReplicaList[j].IpAddr, "blockSize: ", blockreplicas.BlockReplicaList[j].BlockName)
			}
		}
	}

	return filelocation, true
}

// 连接dn,在块上写数据
func DwriteBlock(ipAddr string, data []byte, blockReplicaList *proto.BlockReplicaList) error {
	conn, client, _, _ := getGrpcC2DConn(ipAddr)
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
	blockstatus, err := writeBlockClient.CloseAndRecv()
	fmt.Println(blockstatus)
	if err != nil {
		return err
	}
	return nil
}

// 连接nn,调用方法延续租约
func renewLease(fileName string, clientname string) {
	conn, client, cancel1, _ := getGrpcC2NConn(address)
	defer (*cancel1)()
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := (*client).RenewLock(ctx, &proto.GetLease{Pathname: &proto.PathName{PathName: fileName}, ClientName: clientname})
	if err != nil {
		log.Fatalf("renewLease could not greet:%v", err)
	}
	if res.GetSuccess() {
		log.Printf("renewed lease")
	} else {
		log.Printf("not able to renew lease")
	}
}

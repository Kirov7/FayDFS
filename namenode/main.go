package main

import (
	"context"
	"faydfs/config"
	namenode "faydfs/namenode/service"
	"faydfs/proto"
	"faydfs/public"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"strconv"
	"time"
)

var (
	nameNodeHost   = config.GetConfig().NameNode.NameNodeHost
	nameNodePort   = config.GetConfig().NameNode.NameNodePort
	nameNodeIpAddr = nameNodeHost + nameNodePort
	//todo 初始化nodes
	nn = namenode.GetNewNameNode(map[int]string{}, 1, config.GetConfig().Block.BlockSize, config.GetConfig().Block.Replica)
	lm = namenode.GetNewLeaseManager()
)

type server struct {
	proto.UnimplementedC2NServer
	proto.UnimplementedD2NServer
	proto.UnimplementedRaftServiceServer
}

func (s server) DatanodeHeartbeat(ctx context.Context, heartbeat *proto.Heartbeat) (*proto.DatanodeOperation, error) {
	nn.Heartbeat(heartbeat.IpAddr, heartbeat.DiskUsage)
	return &proto.DatanodeOperation{IpAddr: nameNodeIpAddr}, nil
}

func (s server) BlockReport(ctx context.Context, list *proto.BlockReplicaList) (*proto.OperateStatus, error) {

	for _, blockMeta := range list.BlockReplicaList {
		nn.GetBlockReport(blockMeta)
	}
	return &proto.OperateStatus{Success: true}, nil
}

func (s server) RegisterDataNode(ctx context.Context, req *proto.RegisterDataNodeReq) (*proto.OperateStatus, error) {
	nn.RegisterDataNode(req.IpAddr, req.DiskUsage)
	fmt.Println("register", req.IpAddr, "ipAddress")
	return &proto.OperateStatus{Success: true}, nil
}

func (s server) GetFileLocationAndModifyMeta(ctx context.Context, mode *proto.FileNameAndMode) (*proto.FileLocationArr, error) {
	if mode.Mode == proto.FileNameAndMode_READ {
		fileLocationArr, err := nn.GetLocation(mode.FileName)
		if err != nil {
			return nil, err
		}
		return fileLocationArr, nil
	} else {
		fmt.Println("mode", mode)
		fmt.Println("nn", nn)
		fileLocationArr, err := nn.WriteLocation(mode.FileName, mode.BlockNum)
		if err != nil {
			return nil, err
		}
		return fileLocationArr, nil
	}
}

func (s server) CreateFile(ctx context.Context, mode *proto.FileNameAndMode) (*proto.FileLocationArr, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) OperateMeta(ctx context.Context, mode *proto.FileNameAndOperateMode) (*proto.OperateStatus, error) {
	if mode.Mode == proto.FileNameAndOperateMode_MKDIR {
		if ok, err := nn.MakeDir(mode.FileName); !ok {
			return &proto.OperateStatus{Success: false}, err
		}
		return &proto.OperateStatus{Success: true}, nil
	} else {
		if ok, err := nn.DeletePath(mode.FileName); !ok {
			return &proto.OperateStatus{Success: false}, err
		}
		return &proto.OperateStatus{Success: true}, nil
	}
}

func (s server) RenameFileInMeta(ctx context.Context, path *proto.SrcAndDestPath) (*proto.OperateStatus, error) {
	if err := nn.RenameFile(path.RenameSrcPath, path.RenameDestPath); err != nil {
		return &proto.OperateStatus{Success: false}, err
	}
	return &proto.OperateStatus{Success: true}, nil
}

func (s server) GetFileMeta(ctx context.Context, name *proto.PathName) (*proto.FileMeta, error) {
	meta, ok := nn.FileStat(name.PathName)
	if !ok {
		return nil, public.ErrFileNotFound
	}

	return &proto.FileMeta{
		FileName: meta.FileName,
		FileSize: strconv.FormatUint(meta.FileSize, 10),
		IsDir:    meta.IsDir,
	}, nil
}

func (s server) GetDirMeta(ctx context.Context, name *proto.PathName) (*proto.DirMetaList, error) {
	if list, err := nn.GetDirMeta(name.PathName); err != nil {
		return nil, err
	} else {
		var resultList []*proto.FileMeta
		for _, meta := range list {
			childFile := &proto.FileMeta{
				FileName: meta.FileName,
				FileSize: strconv.FormatUint(meta.FileSize, 10),
				IsDir:    meta.IsDir,
			}
			resultList = append(resultList, childFile)
		}
		return &proto.DirMetaList{MetaList: resultList}, nil
	}
}

func (s server) PutSuccess(ctx context.Context, name *proto.MetaStore) (*proto.OperateStatus, error) {
	success := nn.PutSuccess(name.GetFilePath(), name.GetFileSize(), name.FileLocationArr)
	lm.Revoke(name.GetClientName(), name.GetFilePath())
	return &proto.OperateStatus{Success: success}, nil
}

func (s server) RenewLock(ctx context.Context, name *proto.GetLease) (*proto.OperateStatus, error) {

	if lm.Grant(name.GetClientName(), name.Pathname.GetPathName()) {
		return &proto.OperateStatus{Success: true}, nil
	}
	if lm.Renew(name.GetClientName(), name.Pathname.GetPathName()) {
		return &proto.OperateStatus{Success: true}, nil
	}
	return &proto.OperateStatus{Success: false}, nil
}

// RequestVote 请求投票,由candidate在选举期间发起
func (s *server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	resp := &proto.RequestVoteResponse{}
	log.Printf("handle request vote req: %s\n", req.String())
	nn.Rf.HandleRequestVote(req, resp)
	log.Printf("send request vote resp: %s\n", resp.String())
	return resp, nil
}

// AppendEntries 追加条目,由leader发起,用来复制日志和提供一种心跳机制
func (s *server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	resp := &proto.AppendEntriesResponse{}
	// log.MainLogger.Debug().Msgf("handle append entries req: %s", req.String())
	nn.Rf.HandleAppendEntries(req, resp)
	// log.MainLogger.Debug().Msgf("handle append entries resp: " + resp.String())
	return resp, nil
}

// Snapshot 快照请求
func (s *server) Snapshot(ctx context.Context, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	resp := &proto.InstallSnapshotResponse{}
	log.Printf("handle snapshot: %s\n", req.String())
	nn.Rf.HandleInstallSnapshot(req, resp)
	log.Printf("handle snapshot resp: %s\n", resp.String())
	return resp, nil
}

// logData 汇报当前信息
func logData() {
	for {
		heartbeatDuration := time.Second * time.Duration(3)
		time.Sleep(heartbeatDuration)
		log.Println("NameNode Reporting~~~~~~~~~~~~~~~")
		nn.ShowLog()
	}
}

func main() {
	lis, err := net.Listen("tcp", nameNodeIpAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterC2NServer(s, &server{})
	log.Println("==========C2N Server Start==========")
	proto.RegisterD2NServer(s, &server{})
	log.Println("==========D2N Server Start==========")
	go logData()
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	fmt.Println("before Log")

}

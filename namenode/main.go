package main

import (
	"context"
	"faydfs/config"
	namenode "faydfs/namenode/service"
	"faydfs/proto"
	"faydfs/public"
	"fmt"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

var (
	nameNodePort = config.GetConfig()
	nn           = namenode.GetNewNameNode(config.GetConfig().BlockSize, config.GetConfig().Replica)
	lm           = namenode.GetNewLeaseManager()
)

type server struct {
	proto.UnimplementedC2NServer
	proto.UnimplementedD2NServer
}

func (s server) DatanodeHeartbeat(ctx context.Context, heartbeat *proto.Heartbeat) (*proto.DatanodeOperation, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return &proto.DatanodeOperation{Operation: proto.DatanodeOperation_DELETE}, public.ErrRealIPNotFound
	}
	realIPs := md.Get("x-real-ip")
	if len(realIPs) == 0 {
		return &proto.DatanodeOperation{Operation: proto.DatanodeOperation_DELETE}, public.ErrRealIPNotFound
	}

	nn.Heartbeat(realIPs[0], heartbeat.DiskUsage)
	return nil, nil
}

func (s server) BlockReport(ctx context.Context, list *proto.BlockReplicaList) (*proto.OperateStatus, error) {
	//

	for _, blockMeta := range list.BlockReplicaList {
		nn.GetBlockReport(blockMeta)
	}
	return &proto.OperateStatus{Success: true}, nil
}

func (s server) RegisterDataNode(ctx context.Context, req *proto.RegisterDataNodeReq) (*proto.OperateStatus, error) {
	dataNodePeer, _ := peer.FromContext(ctx)
	nn.RegisterDataNode(dataNodePeer.Addr.String(), req.DiskUsage)
	fmt.Println(dataNodePeer.Addr.String(), "ipAdddr")
	return &proto.OperateStatus{Success: true}, nil
}

func (s server) GetFileLocationAndModifyMeta(ctx context.Context, mode *proto.FileNameAndMode) (*proto.FileLocationArr, error) {
	//TODO implement me
	panic("implement me")
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
	panic("implement me")
}

func (s server) RenameFileInMeta(ctx context.Context, path *proto.SrcAndDestPath) (*proto.OperateStatus, error) {
	if nn.RenameFile(path.RenameSrcPath, path.RenameDestPath) {
		return &proto.OperateStatus{Success: true}, nil
	}
	return &proto.OperateStatus{Success: false}, public.ErrRealIPNotFound
}

func (s server) GetFileMeta(ctx context.Context, name *proto.PathName) (*proto.FileMeta, error) {
	meta, ok := nn.FileStat(name.PathName)
	if !ok {
		return nil, public.ErrFileNotFound
	}

	return &proto.FileMeta{
		FileName: meta.FileName,
		FileSize: meta.FileSize,
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
				FileSize: meta.FileSize,
				IsDir:    meta.IsDir,
			}
			resultList = append(resultList, childFile)
		}
		return &proto.DirMetaList{MetaList: resultList}, nil
	}
}

func (s server) PutSuccess(ctx context.Context, name *proto.PathName) (*proto.OperateStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) RenewLock(ctx context.Context, name *proto.PathName) (*proto.OperateStatus, error) {

	//todo clientName
	if lm.Grant("", name.PathName) {
		return &proto.OperateStatus{Success: true}, nil
	}
	if lm.Renew("", name.PathName) {
		return &proto.OperateStatus{Success: true}, nil
	}
	return &proto.OperateStatus{Success: false}, nil
}

func main() {

}

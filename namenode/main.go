package main

import (
	"context"
	"faydfs/config"
	namenode "faydfs/namenode/service"
	"faydfs/proto"
)

var (
	nameNodePort = config.GetConfig()
	nn           = namenode.GetNewNameNode(config.GetConfig().BlockSize, config.GetConfig().Replica)
)

type server struct {
	proto.UnimplementedC2NServer
	proto.UnimplementedD2NServer
}

func (s server) DatanodeHeartbeat(ctx context.Context, heartbeat *proto.Heartbeat) (*proto.DatanodeOperation, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) BlockReport(ctx context.Context, list *proto.BlockReplicaList) (*proto.OperateStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) RegisterDataNode(ctx context.Context, req *proto.RegisterDataNodeReq) (*proto.OperateStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) GetFileLocationAndModifyMeta(ctx context.Context, mode *proto.FileNameAndMode) (*proto.FileLocationArr, error) {
	if mode.Mode == proto.FileNameAndMode_READ {
		fileLocationArr, err := nn.GetLocation(mode.FileName)
		if err != nil {
			return nil, err
		}
		return fileLocationArr, nil
	} else {
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
	//TODO implement me
	panic("implement me")
}

func (s server) RenameFileInMeta(ctx context.Context, path *proto.SrcAndDestPath) (*proto.OperateStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) GetFileMeta(ctx context.Context, name *proto.PathName) (*proto.FileMeta, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) GetDirMeta(ctx context.Context, name *proto.PathName) (*proto.DirMetaList, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) PutSuccess(ctx context.Context, name *proto.PathName) (*proto.OperateStatus, error) {
	//TODO implement me
	panic("implement me")
}

func (s server) RenewLock(ctx context.Context, name *proto.PathName) (*proto.OperateStatus, error) {
	//TODO implement me
	panic("implement me")
}

func main() {

}

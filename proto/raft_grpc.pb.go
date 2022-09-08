// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.20.1
// source: proto/raft.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// RaftServiceClient is the client API for RaftService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type RaftServiceClient interface {
	// 请求投票,由candidate在选举期间发起
	RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error)
	// 追加条目,由leader发起,用来复制日志和提供一种心跳机制
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
	// 快照请求
	Snapshot(ctx context.Context, in *InstallSnapshotRequest, opts ...grpc.CallOption) (*InstallSnapshotResponse, error)
}

type raftServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftServiceClient(cc grpc.ClientConnInterface) RaftServiceClient {
	return &raftServiceClient{cc}
}

func (c *raftServiceClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	out := new(RequestVoteResponse)
	err := c.cc.Invoke(ctx, "/proto.RaftService/RequestVote", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	err := c.cc.Invoke(ctx, "/proto.RaftService/AppendEntries", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) Snapshot(ctx context.Context, in *InstallSnapshotRequest, opts ...grpc.CallOption) (*InstallSnapshotResponse, error) {
	out := new(InstallSnapshotResponse)
	err := c.cc.Invoke(ctx, "/proto.RaftService/Snapshot", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServiceServer is the server API for RaftService service.
// All implementations must embed UnimplementedRaftServiceServer
// for forward compatibility
type RaftServiceServer interface {
	// 请求投票,由candidate在选举期间发起
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	// 追加条目,由leader发起,用来复制日志和提供一种心跳机制
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	// 快照请求
	Snapshot(context.Context, *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	mustEmbedUnimplementedRaftServiceServer()
}

// UnimplementedRaftServiceServer must be embedded to have forward compatible implementations.
type UnimplementedRaftServiceServer struct {
}

func (UnimplementedRaftServiceServer) RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (UnimplementedRaftServiceServer) AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}
func (UnimplementedRaftServiceServer) Snapshot(context.Context, *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Snapshot not implemented")
}
func (UnimplementedRaftServiceServer) mustEmbedUnimplementedRaftServiceServer() {}

// UnsafeRaftServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RaftServiceServer will
// result in compilation errors.
type UnsafeRaftServiceServer interface {
	mustEmbedUnimplementedRaftServiceServer()
}

func RegisterRaftServiceServer(s grpc.ServiceRegistrar, srv RaftServiceServer) {
	s.RegisterService(&RaftService_ServiceDesc, srv)
}

func _RaftService_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.RaftService/RequestVote",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).RequestVote(ctx, req.(*RequestVoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.RaftService/AppendEntries",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).AppendEntries(ctx, req.(*AppendEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_Snapshot_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InstallSnapshotRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).Snapshot(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.RaftService/Snapshot",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).Snapshot(ctx, req.(*InstallSnapshotRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RaftService_ServiceDesc is the grpc.ServiceDesc for RaftService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RaftService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.RaftService",
	HandlerType: (*RaftServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RequestVote",
			Handler:    _RaftService_RequestVote_Handler,
		},
		{
			MethodName: "AppendEntries",
			Handler:    _RaftService_AppendEntries_Handler,
		},
		{
			MethodName: "Snapshot",
			Handler:    _RaftService_Snapshot_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "proto/raft.proto",
}

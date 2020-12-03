package grpc

import (
	"context"
	"errors"
	"net"

	"google.golang.org/grpc"

	"github.com/dhcmrlchtdj/raftoy/rpc"
	"github.com/dhcmrlchtdj/raftoy/server/raft"
)

func NewServer(raftServer *raft.Server) *grpc.Server {
	srv := &rpc2event{raftServer: raftServer}
	grpcServer := grpc.NewServer()
	rpc.RegisterRaftRpcServer(grpcServer, srv)
	return grpcServer
}

func Start(grpcServer *grpc.Server, addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	err = grpcServer.Serve(listener)
	if err != nil {
		panic(err)
	}
}

///

type rpc2event struct {
	rpc.UnimplementedRaftRpcServer
	raftServer *raft.Server
}

func (s *rpc2event) AppendEntries(ctx context.Context, req *rpc.ReqAppendEntries) (*rpc.RespAppendEntries, error) {
	evt := raft.MakeEvent(req)
	s.raftServer.DispatchEvent(evt)
	resp := <-evt.Resp
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespAppendEntries), nil
	}
}

func (s *rpc2event) RequestVote(ctx context.Context, req *rpc.ReqRequestVote) (*rpc.RespRequestVote, error) {
	evt := raft.MakeEvent(req)
	s.raftServer.DispatchEvent(evt)
	resp := <-evt.Resp
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespRequestVote), nil
	}
}

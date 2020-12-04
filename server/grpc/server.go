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
	srv := &server{raftServer: raftServer}
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

type server struct {
	rpc.UnimplementedRaftRpcServer
	raftServer *raft.Server
}

func (s *server) dispatch(req interface{}) interface{} {
	evt := raft.MakeEvent(req)
	s.raftServer.DispatchEvent(evt)
	resp := <-evt.Resp
	return resp
}

///

func (s *server) AppendEntries(ctx context.Context, req *rpc.ReqAppendEntries) (*rpc.RespAppendEntries, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespAppendEntries), nil
	}
}

func (s *server) RequestVote(ctx context.Context, req *rpc.ReqRequestVote) (*rpc.RespRequestVote, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespRequestVote), nil
	}
}

///

func (s *server) PreVote(ctx context.Context, req *rpc.ReqPreVote) (*rpc.RespPreVote, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespPreVote), nil
	}
}

func (s *server) TimeoutNow(ctx context.Context, req *rpc.ReqTimeoutNow) (*rpc.RespTimeoutNow, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespTimeoutNow), nil
	}
}

///

func (s *server) AddServer(ctx context.Context, req *rpc.ReqAddServer) (*rpc.RespServer, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespServer), nil
	}
}

func (s *server) RemoveServer(ctx context.Context, req *rpc.ReqRemoveServer) (*rpc.RespServer, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespServer), nil
	}
}

///

func (s *server) RegisterClient(ctx context.Context, req *rpc.ReqRegisterClient) (*rpc.RespRegisterClient, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespRegisterClient), nil
	}
}

func (s *server) ClientRequest(ctx context.Context, req *rpc.ReqClientRequest) (*rpc.RespClient, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespClient), nil
	}
}

func (s *server) ClientQuery(ctx context.Context, req *rpc.ReqClientQuery) (*rpc.RespClient, error) {
	resp := s.dispatch(req)
	if resp == nil {
		return nil, errors.New("nil")
	} else {
		return resp.(*rpc.RespClient), nil
	}
}

package server

import (
	"github.com/dhcmrlchtdj/raftoy/server/grpc"
	"github.com/dhcmrlchtdj/raftoy/server/raft"
)

func Start(localAddr string, cluster []string) func() {
	raftServer := raft.NewServer(localAddr, cluster)
	grpcServer := grpc.NewServer(raftServer)

	go grpc.Start(grpcServer, localAddr)
	go raftServer.Start()

	return func() {
		raftServer.Stop()
		grpcServer.GracefulStop()
	}
}

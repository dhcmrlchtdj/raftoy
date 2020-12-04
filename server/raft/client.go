package raft

import "github.com/dhcmrlchtdj/raftoy/rpc"

func (s *Server) onReqRegisterClient(req *rpc.ReqRegisterClient, respC chan interface{}) {
	resp := new(rpc.RespRegisterClient)

	if s.role != Leader {
		resp.Status = rpc.RespRegisterClient_NOT_LEADER
		resp.ClientId = nil
		resp.LeaderHint = nil
		if s.votedFor != "" {
			leader := s.votedFor
			resp.LeaderHint = &leader
		}
		respC <- resp
	}





}

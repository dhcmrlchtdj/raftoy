package raft

import "github.com/dhcmrlchtdj/raftoy/rpc"

func (s *Server) onReqRegisterClient(req *rpc.ReqRegisterClient, respC chan interface{}) {
	if s.role != Leader {
		resp := new(rpc.RespRegisterClient)
		resp.Status = rpc.RespRegisterClient_NOT_LEADER
		if s.votedFor != "" {
			leader := s.votedFor
			resp.LeaderHint = &leader
		}
		respC <- resp
	}

	log, commitC := s.appendLog("register-client") // FIXME
	go func(cid uint64) {
		select {
		case <-s.quitSignal:
			return
		case committed := <-commitC:
			resp := new(rpc.RespRegisterClient)
			if committed {
				resp.Status = rpc.RespRegisterClient_OK
				resp.ClientId = &cid
			} else {
				resp.Status = rpc.RespRegisterClient_NOT_LEADER
				if s.votedFor != "" {
					leader := s.votedFor
					resp.LeaderHint = &leader
				}
			}
			respC <- resp
		}
	}(log.Index)
}

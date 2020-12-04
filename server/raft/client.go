package raft

import (
	"fmt"

	"github.com/dhcmrlchtdj/raftoy/rpc"
)

func (s *Server) onReqRegisterClient(req *rpc.ReqRegisterClient, respC chan interface{}) {
	fmt.Printf("%v onReqRegisterClient\n", s.getInfo())

	if s.role != Leader {
		resp := new(rpc.RespRegisterClient)
		resp.Status = rpc.RespRegisterClient_NOT_LEADER
		if s.votedFor != "" {
			leader := s.votedFor
			resp.LeaderHint = &leader
		}
		respC <- resp
	} else {
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
					// TODO client session
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
}

func (s *Server) onReqClientQuery(req *rpc.ReqClientQuery, respC chan interface{}) {
	fmt.Printf("%v onReqClientQuery\n", s.getInfo())

	if s.role != Leader {
		resp := new(rpc.RespClient)
		resp.Status = rpc.RespClient_NOT_LEADER
		if s.votedFor != "" {
			leader := s.votedFor
			resp.LeaderHint = &leader
		}
		respC <- resp
	} else {
		_, commitC := s.appendLog("no-op") // FIXME
		go func() {
			select {
			case <-s.quitSignal:
				return
			case committed := <-commitC:
				resp := new(rpc.RespClient)
				if committed {
					resp.Status = rpc.RespClient_OK
					val, found := s.store.Get(req.Query)
					if found {
						resp.Response = &val
					}
				} else {
					resp.Status = rpc.RespClient_NOT_LEADER
					if s.votedFor != "" {
						leader := s.votedFor
						resp.LeaderHint = &leader
					}
				}
				respC <- resp
			}
		}()
	}
}

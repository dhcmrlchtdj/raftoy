package raft

import "github.com/dhcmrlchtdj/raftoy/rpc"

type Event struct {
	payload interface{}
	Resp    chan interface{}
}

func MakeEvent(payload interface{}) *Event {
	return &Event{
		payload: payload,
		Resp:    make(chan interface{}, 1),
	}
}

///

type evtTick struct{}

type evtCommitIndexUpdated struct{}

type evtPrepareAppendEntries struct {
	peer string
}

type evtRespRequestVote struct {
	peer string
	req  *rpc.ReqRequestVote
	resp *rpc.RespRequestVote
}

type evtRespPreVote struct {
	peer string
	req  *rpc.ReqPreVote
	resp *rpc.RespPreVote
}

type evtRespAppendEntries struct {
	peer string
	req  *rpc.ReqAppendEntries
	resp *rpc.RespAppendEntries
}

package raft

import (
	"context"
	"fmt"
	"sort"

	"google.golang.org/grpc"

	"github.com/dhcmrlchtdj/raftoy/rpc"
)

///

func (s *Server) onTimeoutTick() {
	switch s.role {
	case Follower:
		s.electionTimeoutTick--
		if s.electionTimeoutTick <= 0 {
			s.resetElectronTimeoutTick()
			s.becomeCandidate()
			s.preVoteTerm = s.currentTerm + 1
			s.broadcastPreVote()
		}
	case Candidate:
		s.electionTimeoutTick--
		if s.electionTimeoutTick <= 0 {
			s.resetElectronTimeoutTick()
			s.preVoteTerm++
			s.broadcastPreVote()
		}
	case Leader:
		s.heartbeatTimeoutTick--
		if s.heartbeatTimeoutTick <= 0 {
			s.resetHeartbeatTimeoutTick()
			s.broadcastAppendEntries()
		}
	}
}

func (s *Server) applyCommittedLog() {
	if s.commitIndex > s.lastApplied {
		// TODO apply log to state machine
	}
}

func (s *Server) onCommitIndexUpdate() {
	s.applyCommittedLog()
	for logId, ch := range s.waitingCommit {
		if logId <= s.commitIndex {
			ch <- true
			delete(s.waitingCommit, logId)
		}
	}
}

func (s *Server) appendLog(command string) (*LogEntry, chan bool) {
	newLog := LogEntry{
		Term:    s.currentTerm,
		Index:   uint64(len(s.log)),
		Command: command,
	}
	s.log = append(s.log, newLog)
	s.broadcastAppendEntries()

	commitC := make(chan bool, 1)
	s.waitingCommit[newLog.Index] = commitC

	return &newLog, commitC
}

///

func (s *Server) becomeFollower(term TermID, votedFor string) {
	prevRole := s.role
	s.currentTerm = term
	s.votedFor = votedFor
	s.role = Follower
	s.resetElectronTimeoutTick()
	if prevRole == Leader {
		s.clearLeaderState()
	}
	if prevRole != Follower {
		fmt.Printf("%v become follower\n", s.getInfo())
	}
}

func (s *Server) becomeCandidate() {
	if s.role != Follower {
		panic("become candidate")
	}
	s.role = Candidate
	fmt.Printf("%v become candidate\n", s.getInfo())
}

func (s *Server) becomeLeader() {
	if s.role != Candidate {
		panic("become leader")
	}
	s.role = Leader
	s.resetFollowerIndex()
	s.resetHeartbeatTimeoutTick()
	s.appendLog("no-op") // FIXME
	fmt.Printf("%v become leader\n", s.getInfo())
}

func (s *Server) resetFollowerIndex() {
	s.nextIndex = make(map[string]uint64)
	s.matchIndex = make(map[string]uint64)

	lastLog := s.getLastLog()
	for i := range s.peers {
		peer := s.peers[i]
		if peer == s.myself {
			continue
		}
		s.nextIndex[peer] = lastLog.Index + 1
		s.matchIndex[peer] = 0
	}
}

func (s *Server) resetHeartbeatTimeoutTick() {
	s.heartbeatTimeoutTick = DefaultHeartbeatTimeoutTick
}

func (s *Server) resetElectronTimeoutTick() {
	s.electionTimeoutTick = randomElectionTimeoutTick()
}

func (s *Server) startElection() {
	s.currentTerm++
	s.votedFor = s.myself
	s.votedForMe = 1
	fmt.Printf("%v startElection\n", s.getInfo())
	s.broadcastRequestVote()
}

func (s *Server) updateCommitIndex() {
	if len(s.peers) == 1 {
		// FIXME
		return
	}

	matchIdx := []int{}
	for _, v := range s.matchIndex {
		matchIdx = append(matchIdx, int(v))
	}
	sort.Ints(matchIdx)
	confirmed := uint64(matchIdx[len(matchIdx)/2])
	if confirmed > s.commitIndex && s.log[confirmed].Term == s.currentTerm {
		s.commitIndex = confirmed
		go s.DispatchEvent(MakeEvent(evtCommitIndexUpdated{}))
	}
}

func (s *Server) clearLeaderState() {
	for logId, ch := range s.waitingCommit {
		ch <- false
		delete(s.waitingCommit, logId)
	}
}

///

func (s *Server) getPeerClient(peer string) (rpc.RaftRpcClient, error) {
	client, found := s.peerConn[peer]
	if !found {
		conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
		client = rpc.NewRaftRpcClient(conn)
		s.peerConn[peer] = client
	}
	return client, nil
}

func (s *Server) broadcastRequestVote() {
	fmt.Printf("%v broadcastRequestVote\n", s.getInfo())

	lastLog := s.getLastLog()
	req := &rpc.ReqRequestVote{
		Term:         s.currentTerm,
		CandidateId:  s.myself,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	for i := range s.peers {
		peer := s.peers[i]
		if peer == s.myself {
			continue
		}
		client, err := s.getPeerClient(peer)
		if err != nil {
			continue
		}
		go func() {
			resp, err := client.RequestVote(context.Background(), req)
			if err != nil {
				return
			}
			s.DispatchEvent(MakeEvent(evtRespRequestVote{peer, req, resp}))
		}()
	}
}

func (s *Server) broadcastPreVote() {
	fmt.Printf("%v broadcastPreVote\n", s.getInfo())

	s.votedForMe = 1

	lastLog := s.getLastLog()
	req := &rpc.ReqPreVote{
		PreVoteTerm:  s.preVoteTerm,
		CandidateId:  s.myself,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	for i := range s.peers {
		peer := s.peers[i]
		if peer == s.myself {
			continue
		}
		client, err := s.getPeerClient(peer)
		if err != nil {
			continue
		}
		go func() {
			resp, err := client.PreVote(context.Background(), req)
			if err != nil {
				return
			}
			s.DispatchEvent(MakeEvent(evtRespPreVote{peer, req, resp}))
		}()
	}
}

func (s *Server) broadcastAppendEntries() {
	fmt.Printf("%v broadcastAppendEntries\n", s.getInfo())

	for i := range s.peers {
		peer := s.peers[i]
		s.prepareAppendEntries(peer)
	}
}

func (s *Server) prepareAppendEntries(peer string) {
	if peer == s.myself {
		return
	}
	if len(s.peerChannel[peer]) == 0 {
		s.peerChannel[peer] <- struct{}{}
	}
}

func (s *Server) onPrepareAppendEntries(peer string) {
	client, err := s.getPeerClient(peer)
	if err != nil {
		return
	}
	prevIdx := s.nextIndex[peer] - 1
	prevLog := s.log[prevIdx]

	size := 1
	entries := make([]*rpc.Entry, 0)
	for i := 1; i <= size && int(prevIdx)+i < len(s.log); i++ {
		entry := s.log[int(prevIdx)+i]
		rpcEntry := rpc.Entry{Term: entry.Term, Index: entry.Index, Command: entry.Command}
		entries = append(entries, &rpcEntry)
	}

	req := &rpc.ReqAppendEntries{
		Term:         s.currentTerm,
		LeaderId:     s.myself,
		PrevLogIndex: prevLog.Index,
		PrevLogTerm:  prevLog.Term,
		LeaderCommit: s.commitIndex,
		Entries:      entries,
	}
	go func() {
		resp, err := client.AppendEntries(context.Background(), req)
		if err != nil {
			return
		}
		s.DispatchEvent(MakeEvent(evtRespAppendEntries{peer, req, resp}))
	}()
}

///

func (s *Server) onReqRequestVote(req *rpc.ReqRequestVote) *rpc.RespRequestVote {
	resp := new(rpc.RespRequestVote)
	resp.Term = s.currentTerm

	defer func() {
		if resp.VoteGranted {
			fmt.Printf("%v grant RequestVote [peer=%v,term=%v,logT=%v,logI=%v]\n",
				s.getInfo(), req.CandidateId, req.Term, req.LastLogTerm, req.LastLogIndex)
		} else {
			fmt.Printf("%v reject RequestVote [peer=%v,term=%v,logT=%v,logI=%v]\n",
				s.getInfo(), req.CandidateId, req.Term, req.LastLogTerm, req.LastLogIndex)
		}
	}()

	if req.Term < s.currentTerm {
		resp.VoteGranted = false
		return resp
	}

	if req.Term > s.currentTerm {
		s.becomeFollower(req.Term, "")
		resp.Term = s.currentTerm
	}

	if s.votedFor == "" || s.votedFor == req.CandidateId {
		lastLog := s.getLastLog()

		// compare term
		if lastLog.Term > req.LastLogTerm {
			resp.VoteGranted = false
			return resp
		} else if lastLog.Term < req.LastLogTerm {
			s.becomeFollower(req.Term, req.CandidateId)
			resp.VoteGranted = true
			return resp
		}

		// compare log length
		if lastLog.Index <= req.LastLogIndex {
			s.becomeFollower(req.Term, req.CandidateId)
			resp.VoteGranted = true
			return resp
		} else {
			resp.VoteGranted = false
			return resp
		}
	} else {
		resp.VoteGranted = false
		return resp
	}
}

///

func (s *Server) onReqAppendEntries(req *rpc.ReqAppendEntries) *rpc.RespAppendEntries {
	resp := new(rpc.RespAppendEntries)
	resp.Term = s.currentTerm

	defer func() {
		if resp.Success {
			fmt.Printf("%v accept AppendEntries [peer=%v,term=%v,logT=%v,logI=%v]\n",
				s.getInfo(), req.LeaderId, req.Term, req.PrevLogTerm, req.PrevLogIndex)
		} else {
			fmt.Printf("%v reject AppendEntries [peer=%v,term=%v,logT=%v,logI=%v]\n",
				s.getInfo(), req.LeaderId, req.Term, req.PrevLogTerm, req.PrevLogIndex)
		}
	}()

	if req.Term < s.currentTerm {
		resp.Success = false
		return resp
	}

	if req.Term > s.currentTerm || s.votedFor != req.LeaderId {
		s.becomeFollower(req.Term, req.LeaderId)
		resp.Term = s.currentTerm
	}

	if uint64(len(s.log)) <= req.PrevLogIndex {
		resp.Success = false
		return resp
	}
	prevLog := s.log[req.PrevLogIndex]
	if prevLog.Term != req.PrevLogTerm {
		resp.Success = false
		return resp
	}

	resp.Success = true
	s.resetElectronTimeoutTick()

	// empty entry, heartbeat
	if len(req.Entries) == 0 {
		return resp
	}

	// append new entry
	for _, reqEntry := range req.Entries {
		newEntry := LogEntry{
			Term:    reqEntry.Term,
			Index:   reqEntry.Index,
			Command: reqEntry.Command,
		}
		if newEntry.Index < uint64(len(s.log)) {
			s.log[newEntry.Index] = newEntry
		} else {
			s.log = append(s.log, newEntry)
		}
	}

	if req.LeaderCommit > s.commitIndex {
		s.commitIndex = minUint64(
			req.LeaderCommit,
			req.Entries[len(req.Entries)-1].Index,
		)
	}

	s.applyCommittedLog()

	return resp
}

///

func (s *Server) onRespRequestVote(e evtRespRequestVote) {
	switch s.role {
	case Candidate:
		if e.resp.Term > s.currentTerm {
			s.becomeFollower(e.resp.Term, "")
			return
		}
		if e.req.Term == s.currentTerm && e.resp.VoteGranted {
			cnt := s.votedForMe + 1
			if cnt >= (len(s.peers)/2 + 1) {
				s.becomeLeader()
			}
		}

	case Leader, Follower:
		if e.resp.Term > s.currentTerm {
			s.becomeFollower(e.resp.Term, "")
		}
	}
}

///

func (s *Server) onRespAppendEntries(e evtRespAppendEntries) {
	switch s.role {
	case Leader:
		if e.resp.Term > s.currentTerm {
			s.becomeFollower(e.resp.Term, "")
			return
		}
		if e.resp.Success {
			size := len(e.req.Entries)
			if size > 0 {
				s.nextIndex[e.peer] = s.nextIndex[e.peer] + uint64(size)
				s.matchIndex[e.peer] = e.req.Entries[len(e.req.Entries)-1].Index
				s.updateCommitIndex()
				if s.matchIndex[e.peer] < s.getLastLog().Index {
					s.prepareAppendEntries(e.peer)
				}
			}
		} else {
			s.nextIndex[e.peer]--
			s.prepareAppendEntries(e.peer)
		}

	case Candidate, Follower:
		if e.resp.Term > s.currentTerm {
			s.becomeFollower(e.resp.Term, "")
		}
	}
}

///

func (s *Server) onReqPreVote(req *rpc.ReqPreVote) *rpc.RespPreVote {
	resp := new(rpc.RespPreVote)
	resp.Term = s.currentTerm

	defer func() {
		if resp.Success {
			fmt.Printf("%v grant PreVote [peer=%v,term=%v,logT=%v,logI=%v]\n",
				s.getInfo(), req.CandidateId, req.PreVoteTerm, req.LastLogTerm, req.LastLogIndex)
		} else {
			fmt.Printf("%v reject PreVote [peer=%v,term=%v,logT=%v,logI=%v]\n",
				s.getInfo(), req.CandidateId, req.PreVoteTerm, req.LastLogTerm, req.LastLogIndex)
		}
	}()

	if req.PreVoteTerm < s.currentTerm {
		resp.Success = false
		return resp
	}

	lastLog := s.getLastLog()

	// compare term
	if lastLog.Term > req.LastLogTerm {
		resp.Success = false
		return resp
	} else if lastLog.Term < req.LastLogTerm {
		resp.Success = true
		return resp
	}

	// compare log length
	if lastLog.Index <= req.LastLogIndex {
		resp.Success = true
		return resp
	} else {
		resp.Success = false
		return resp
	}
}

///

func (s *Server) onRespPreVote(e evtRespPreVote) {
	switch s.role {
	case Candidate:
		if e.resp.Term > s.currentTerm {
			s.becomeFollower(e.resp.Term, "")
			return
		}
		if e.req.PreVoteTerm == s.preVoteTerm && e.resp.Success {
			cnt := s.votedForMe + 1
			if cnt >= (len(s.peers)/2 + 1) {
				s.preVoteTerm++
				s.startElection()
			}
		}

	case Leader, Follower:
		if e.resp.Term > s.currentTerm {
			s.becomeFollower(e.resp.Term, "")
		}
	}
}

///

func (s *Server) onReqTimeoutNow(*rpc.ReqTimeoutNow) *rpc.RespTimeoutNow {
	resp := new(rpc.RespTimeoutNow)
	resp.Success = true
	s.electionTimeoutTick = 0
	return resp
}

///

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

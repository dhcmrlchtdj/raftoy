package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/dhcmrlchtdj/raftoy/rpc"
	"google.golang.org/grpc"
)

///

const (
	DefaultTickSpan             time.Duration = 20 * time.Millisecond // 20ms
	DefaultHeartbeatTimeoutTick int           = 20                    // 400ms
)

func randomElectionTimeoutTick() int { // [800ms, 1000ms)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// return [40, 50)
	return 40 + r.Intn(10)
}

///

type Server struct {
	role Role

	// membership
	myself   NodeID
	peers    []NodeID
	peerConn map[NodeID]rpc.RaftRpcClient

	// persistent on all
	currentTerm TermID
	votedFor    NodeID
	log         []LogEntry

	// volatile on all
	commitIndex LogID
	lastApplied LogID

	// volatile on leader
	nextIndex  map[NodeID]LogID
	matchIndex map[NodeID]LogID

	// volatile on candidate
	votedForMe  int
	preVoteTerm TermID

	// tick
	tickSpan             time.Duration
	heartbeatTimeoutTick int
	electionTimeoutTick  int

	// event
	eventhub chan *Event

	// stop
	quitSignal chan struct{}
	quitOnce   sync.Once
	quitWait   sync.WaitGroup
}

func NewServer(addr string, peers []string) *Server {
	s := Server{
		role:                 Follower,
		myself:               addr,
		peers:                peers,
		peerConn:             make(map[string]rpc.RaftRpcClient),
		currentTerm:          0,
		votedFor:             "",
		log:                  []LogEntry{{Term: 0, Index: 0, Command: ""}},
		commitIndex:          0,
		lastApplied:          0,
		nextIndex:            nil,
		matchIndex:           nil,
		votedForMe:           0,
		preVoteTerm:          0,
		tickSpan:             DefaultTickSpan,
		heartbeatTimeoutTick: DefaultHeartbeatTimeoutTick,
		electionTimeoutTick:  randomElectionTimeoutTick(),
		eventhub:             make(chan *Event),
		quitSignal:           make(chan struct{}),
	}
	return &s
}

func (s *Server) getInfo() string {
	lastLog := s.log[len(s.log)-1]
	r := ""
	switch s.role {
	case Leader:
		r = "Leader"
	case Candidate:
		r = "Candidate"
	case Follower:
		r = "Follower"
	}
	return fmt.Sprintf(
		"\n[addr=%v, role=%v, term=%v, logLen=%v, lastLogTerm=%v, lastLogIndex=%v]\n",
		s.myself,
		r,
		s.currentTerm,
		len(s.log),
		lastLog.Term,
		lastLog.Index,
	)
}

///

func (s *Server) Start() {
	go s.loopTick()
	go s.loopEvent()
}

func (s *Server) Stop() {
	s.quitOnce.Do(func() {
		close(s.quitSignal)
		s.quitWait.Wait()
	})
}

func (s *Server) DispatchEvent(evt *Event) {
	select {
	case <-s.quitSignal:
		evt.Resp = nil
	case s.eventhub <- evt:
	}
}

///

func (s *Server) loopTick() {
	s.quitWait.Add(1)
	defer s.quitWait.Done()
	for {
		time.Sleep(s.tickSpan)
		select {
		case <-s.quitSignal:
			return
		case s.eventhub <- MakeEvent(evtTick{}):
		}
	}
}

func (s *Server) loopEvent() {
	s.quitWait.Add(1)
	defer s.quitWait.Done()
	for {
		select {
		case <-s.quitSignal:
			return
		case evt := <-s.eventhub:
			switch e := evt.payload.(type) {
			case evtTick:
				s.onTimeoutTick()
			case evtCommitIndexUpdated:
				// response to client read/write
				s.onCommitIndexUpdate()

			// receive rpc request
			case *rpc.ReqAppendEntries:
				evt.Resp <- s.onReqAppendEntries(e)
			case *rpc.ReqRequestVote:
				evt.Resp <- s.onReqRequestVote(e)
			case *rpc.ReqPreVote:
				evt.Resp <- s.onReqPreVote(e)

			// receive broadcast response
			case evtRespRequestVote:
				s.onRespRequestVote(e)
			case evtRespAppendEntries:
				s.onRespAppendEntries(e)
			case evtRespPreVote:
				s.onRespPreVote(e)
			}
		}
	}
}

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
			s.broadcastHeartbeat()
		}
	}
}

func (s *Server) onCommitIndexUpdate() {
	// TODO
}

///

func (s *Server) becomeFollower(term TermID, votedFor string) {
	if s.role != Follower {
		fmt.Printf("%v become follower\n", s.getInfo())
	}
	s.currentTerm = term
	s.role = Follower
	s.votedFor = votedFor
	s.resetElectronTimeoutTick()
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
	fmt.Printf("%v become leader\n", s.getInfo())
	s.resetFollowerIndex()
	s.resetHeartbeatTimeoutTick()
	s.broadcastHeartbeat()
}

func (s *Server) updateCommitIndex() {
	matchIdx := []int{}
	for _, i := range s.matchIndex {
		matchIdx = append(matchIdx, int(i))
	}
	sort.Ints(matchIdx)
	for i := len(matchIdx) / 2; i >= 0; i-- {
		idx := uint64(matchIdx[i])
		if idx > s.commitIndex && s.log[idx].Index == s.currentTerm {
			s.commitIndex = idx
			go s.DispatchEvent(MakeEvent(evtCommitIndexUpdated{}))
			break
		}
	}
}

func (s *Server) resetFollowerIndex() {
	s.nextIndex = make(map[string]uint64)
	s.matchIndex = make(map[string]uint64)

	lastLog := s.log[len(s.log)-1]
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

	lastLog := s.log[len(s.log)-1]
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

	lastLog := s.log[len(s.log)-1]
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

func (s *Server) broadcastHeartbeat() {
	fmt.Printf("%v broadcastHeartbeat\n", s.getInfo())

	for i := range s.peers {
		peer := s.peers[i]
		if peer == s.myself {
			continue
		}
		s.sendAppendEntries(peer, 0)
	}
}

func (s *Server) sendAppendEntries(peer string, size int) {
	client, err := s.getPeerClient(peer)
	if err != nil {
		return
	}
	prevIdx := s.nextIndex[peer] - 1
	prevLog := s.log[prevIdx]

	entries := make([]*rpc.Entry, size)
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
		lastLog := s.log[len(s.log)-1]

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

	if s.commitIndex > s.lastApplied {
		// TODO apply log to state machine
	}

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
				s.sendAppendEntries(e.peer, size)
			}
		} else {
			s.nextIndex[e.peer]--
			s.sendAppendEntries(e.peer, len(e.req.Entries))
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

	lastLog := s.log[len(s.log)-1]

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

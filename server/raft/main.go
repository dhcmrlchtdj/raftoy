package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/dhcmrlchtdj/raftoy/rpc"
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
	eventhub         chan *Event
	peerChannel      map[NodeID](chan struct{})
	peerChannelClose map[NodeID](chan struct{})

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
		peerChannel:          nil,
		peerChannelClose:     nil,
		quitSignal:           make(chan struct{}),
	}
	s.setupPeerChannel()
	return &s
}

func (s *Server) setupPeerChannel() {
	s.peerChannel = make(map[string]chan struct{})
	s.peerChannelClose = make(map[string]chan struct{})
	for i := range s.peers {
		peer := s.peers[i]
		if peer == s.myself {
			continue
		}
		msgC := make(chan struct{}, 1)
		closeC := make(chan struct{})
		go func() {
			for {
				select {
				case <-s.quitSignal:
					return
				case <-closeC:
					return
				case <-msgC:
					s.DispatchEvent(MakeEvent(evtPrepareAppendEntries{peer}))
				}
			}
		}()
		s.peerChannel[peer] = msgC
		s.peerChannelClose[peer] = closeC
	}
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
				s.onCommitIndexUpdate()
			case evtPrepareAppendEntries:
				s.onPrepareAppendEntries(e.peer)

			// receive rpc request
			case *rpc.ReqAppendEntries:
				evt.Resp <- s.onReqAppendEntries(e)
			case *rpc.ReqRequestVote:
				evt.Resp <- s.onReqRequestVote(e)
			case *rpc.ReqPreVote:
				evt.Resp <- s.onReqPreVote(e)
			case *rpc.ReqTimeoutNow:
				evt.Resp <- s.onReqTimeoutNow(e)
			// receive broadcast response
			case evtRespRequestVote:
				s.onRespRequestVote(e)
			case evtRespAppendEntries:
				s.onRespAppendEntries(e)
			case evtRespPreVote:
				s.onRespPreVote(e)

			// client
			case *rpc.ReqRegisterClient:
				s.onReqRegisterClient(e, evt.Resp)
			}
		}
	}
}

package raft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	// "math/rand"
	"sync"
	"time"

	"github.com/dhcmrlchtdj/raftoy/rpc"
	"github.com/dhcmrlchtdj/raftoy/server/store"
)

///

const (
	DefaultTickSpan             time.Duration = 20 * time.Millisecond // 20ms
	DefaultHeartbeatTimeoutTick int           = 20                    // 400ms
)

func randomElectionTimeoutTick() int { // [800ms, 1000ms)
	// r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// return 40 + r.Intn(10) // [40, 50)
	n, err := rand.Int(rand.Reader, big.NewInt(10))
	if err != nil {
		panic(err)
	}
	return 40 + n.Sign() // [40, 50)
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

	// state machine
	store *store.Store

	// tick
	tickSpan             time.Duration
	heartbeatTimeoutTick int
	electionTimeoutTick  int

	// event
	eventhub chan *Event

	peerChannel   map[NodeID](chan struct{})
	waitingCommit map[LogID](chan bool)

	// stop
	quitSignal chan struct{}
	quitOnce   sync.Once
	quitWait   sync.WaitGroup
}

type Role int32

const (
	Follower  Role = 1
	Candidate Role = 2
	Leader    Role = 3
)

type (
	ClientID = string
	NodeID   = string
	TermID   = uint64
	LogID    = uint64
)

type LogEntry struct {
	Term    TermID
	Index   LogID
	Command string
}

func NewServer(addr string, peers []string) *Server {
	s := Server{
		role:                 Follower,
		myself:               addr,
		peers:                peers,
		peerConn:             make(map[string]rpc.RaftRpcClient),
		currentTerm:          0,
		votedFor:             "",
		log:                  []LogEntry{{Term: 0, Index: 0, Command: "no-op"}},
		commitIndex:          0,
		lastApplied:          0,
		nextIndex:            nil,
		matchIndex:           nil,
		votedForMe:           0,
		preVoteTerm:          0,
		store:                store.New(),
		tickSpan:             DefaultTickSpan,
		heartbeatTimeoutTick: DefaultHeartbeatTimeoutTick,
		electionTimeoutTick:  randomElectionTimeoutTick(),
		eventhub:             make(chan *Event),
		peerChannel:          make(map[string]chan struct{}),
		waitingCommit:        make(map[uint64]chan bool),
		quitSignal:           make(chan struct{}),
	}
	s.setupPeerChannel()
	return &s
}

func (s *Server) setupPeerChannel() {
	for i := range s.peers {
		peer := s.peers[i]
		if peer == s.myself {
			continue
		}
		msgC := make(chan struct{}, 1)
		go func() {
			for {
				select {
				case <-s.quitSignal:
					return
				case <-msgC:
					s.DispatchEvent(MakeEvent(evtPrepareAppendEntries{peer}))
				}
			}
		}()
		s.peerChannel[peer] = msgC
	}
}

func (s *Server) getLastLog() LogEntry {
	return s.log[len(s.log)-1]
}

func (s *Server) getInfo() string {
	lastLog := s.getLastLog()
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
				s.onCommitIndexUpdate(e)
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
			case *rpc.ReqClientQuery:
				s.onReqClientQuery(e, evt.Resp)
			case *rpc.ReqClientRequest:
				s.onReqClientRequest(e, evt.Resp)
			}
		}
	}
}

package raft

type Role = int32

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

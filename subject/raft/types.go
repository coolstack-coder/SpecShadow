package raft

import (
	"context"
	"errors"
	"fmt"
)

// Role is what a node currently thinks it is.
type Role int

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("role(%d)", int(r))
}

// EntryType tells the applier what to do with an entry.
type EntryType uint8

const (
	EntryNormal EntryType = iota
	// EntryNoop is appended by every new leader so it can commit entries
	// from older terms (raft paper, section 5.4.2 / figure 8).
	EntryNoop
)

// Entry is one slot in the replicated log.
type Entry struct {
	Index uint64    `json:"i"`
	Term  uint64    `json:"t"`
	Type  EntryType `json:"y,omitempty"`
	Data  []byte    `json:"d,omitempty"`
}

// HardState is what must hit disk before we answer any RPC.
type HardState struct {
	Term uint64 `json:"term"`
	Vote string `json:"vote"`
}

// SnapshotMeta identifies the last entry folded into a snapshot.
type SnapshotMeta struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

type VoteRequest struct {
	Group        string `json:"group"`
	Term         uint64 `json:"term"`
	Candidate    string `json:"candidate"`
	LastLogIndex uint64 `json:"last_index"`
	LastLogTerm  uint64 `json:"last_term"`
	PreVote      bool   `json:"pre_vote,omitempty"`
}

type VoteResponse struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

type AppendRequest struct {
	Group        string  `json:"group"`
	Term         uint64  `json:"term"`
	Leader       string  `json:"leader"`
	PrevLogIndex uint64  `json:"prev_index"`
	PrevLogTerm  uint64  `json:"prev_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"commit"`
}

type AppendResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	// conflict hints so the leader can skip a whole term at a time instead
	// of walking nextIndex back one entry per round trip
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	LastIndex     uint64 `json:"last_index"`
}

type SnapshotRequest struct {
	Group  string       `json:"group"`
	Term   uint64       `json:"term"`
	Leader string       `json:"leader"`
	Meta   SnapshotMeta `json:"meta"`
	Data   []byte       `json:"data"`
}

type SnapshotResponse struct {
	Term uint64 `json:"term"`
}

// Transport moves RPCs between nodes. Implementations must be safe for
// concurrent use.
type Transport interface {
	SendVote(ctx context.Context, to string, req *VoteRequest) (*VoteResponse, error)
	SendAppend(ctx context.Context, to string, req *AppendRequest) (*AppendResponse, error)
	SendSnapshot(ctx context.Context, to string, req *SnapshotRequest) (*SnapshotResponse, error)
}

// StateMachine is the thing being replicated. Apply is called from a single
// goroutine, in log order, exactly once per committed entry per process
// lifetime (after a restart the snapshot is restored and entries after it
// are replayed).
type StateMachine interface {
	Apply(e Entry) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Storage persists hard state, log entries and snapshots.
type Storage interface {
	// Load returns everything needed to rebuild a node after a restart.
	Load() (HardState, SnapshotMeta, []byte, []Entry, error)
	// Save durably writes hs (if non-nil) and ents. ents[0].Index may be at
	// or below the current last index, in which case the stored log is
	// truncated there first.
	Save(hs *HardState, ents []Entry) error
	// SaveSnapshot stores a snapshot and drops log entries it covers. hs and
	// retained are the full post-compaction state.
	SaveSnapshot(meta SnapshotMeta, data []byte, hs HardState, retained []Entry) error
	Close() error
}

var (
	ErrStopped = errors.New("raft: node stopped")
	// ErrLeadershipLost means the entry we proposed was overwritten. The
	// command did not and will not take effect.
	ErrLeadershipLost = errors.New("raft: leadership lost before commit")
	ErrNotReady       = errors.New("raft: leader has not committed an entry in its term yet")
)

// NotLeaderError is returned when a write or linearizable read reaches a
// follower. Leader is a hint and may be empty.
type NotLeaderError struct {
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not leader (leader unknown)"
	}
	return "raft: not leader, try " + e.Leader
}

// Status is a point-in-time view of a node, used by the dashboard and
// metrics.
type Status struct {
	ID            string            `json:"id"`
	Group         string            `json:"group"`
	Role          string            `json:"role"`
	Term          uint64            `json:"term"`
	Leader        string            `json:"leader"`
	Vote          string            `json:"vote"`
	CommitIndex   uint64            `json:"commit_index"`
	AppliedIndex  uint64            `json:"applied_index"`
	LastIndex     uint64            `json:"last_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	Peers         []string          `json:"peers"`
	Match         map[string]uint64 `json:"match,omitempty"`
	Stats         Stats             `json:"stats"`
}

// Stats are monotonic counters since process start.
type Stats struct {
	ElectionsStarted   uint64 `json:"elections_started"`
	ElectionsWon       uint64 `json:"elections_won"`
	Proposals          uint64 `json:"proposals"`
	Applied            uint64 `json:"applied"`
	SnapshotsTaken     uint64 `json:"snapshots_taken"`
	SnapshotsSent      uint64 `json:"snapshots_sent"`
	SnapshotsInstalled uint64 `json:"snapshots_installed"`
	StepDowns          uint64 `json:"step_downs"`
	LeaseReads         uint64 `json:"lease_reads"`
	QuorumReads        uint64 `json:"quorum_reads"`
}

package raft

// SpecShadow additions to AegisDB's raft node: an injectable clock, trace
// hooks and switches for planted bugs. With Config.Trace and Config.Bug left
// empty and Config.Clock nil, the node behaves exactly like AegisDB.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/coolstack-coder/specshadow/trace"
)

// Clock is where the node reads time. Every timing decision in the node
// (election deadlines, CheckQuorum, leader stickiness, the read lease) goes
// through it, so tests can make one node's clock run slow, fast or stop.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Planted bugs. Each one is a small, realistic mistake in a raft
// implementation; the evaluation checks which of them the checker and
// AegisDB's own tests notice.
const (
	BugNone          = ""
	BugDoubleVote    = "double-vote"     // grant a second vote in the same term
	BugNoUpToDate    = "no-up-to-date"   // vote without the log up-to-date check
	BugCommitOldTerm = "commit-old-term" // no election no-op, and old-term entries committed by counting (raft figure 8)
	BugCommitOverrun = "commit-overrun"  // follower commit = leaderCommit, not min(leaderCommit, lastNew)
	BugLongLease     = "long-lease"      // lease = 2 x election timeout
	BugStaleTerm     = "stale-term"      // follower accepts AppendEntries from an older term
	BugVoteNotSynced = "vote-not-synced" // answer a vote before the vote is on disk
)

// Bugs lists every planted bug in evaluation order.
var Bugs = []string{BugDoubleVote, BugNoUpToDate, BugCommitOldTerm, BugCommitOverrun,
	BugLongLease, BugStaleTerm, BugVoteNotSynced}

func (n *Node) bug(b string) bool {
	if n.cfg.Bug != "" {
		return n.cfg.Bug == b
	}
	return envBug == b
}

// envBug lets AegisDB's original test suite run against a planted bug.
var envBug = os.Getenv("SPECSHADOW_BUG")

func (n *Node) now() time.Time { return n.clock.Now() }

func roleCode(r Role) string {
	switch r {
	case Leader:
		return "L"
	case Candidate:
		return "C"
	case PreCandidate:
		return "P"
	}
	return "F"
}

// stateLocked snapshots the protocol state. mu must be held.
func (n *Node) stateLocked() *trace.State {
	terms := make([]uint64, 0, len(n.entries)-1)
	for _, e := range n.entries[1:] {
		terms = append(terms, e.Term)
	}
	return &trace.State{Term: n.term, Role: roleCode(n.role), Vote: n.vote, Log: terms, Commit: n.commitIndex}
}

// emitLocked records an event with the node's post-state. mu must be held.
func (n *Node) emitLocked(kind string, e trace.Event) {
	if n.cfg.Trace == nil || n.stopped {
		return // a crashed process says nothing
	}
	if n.entries[0].Index != 0 {
		panic("specshadow: traced nodes must not compact their log")
	}
	e.Node, e.Kind, e.Site = n.id, kind, site()
	e.State = n.stateLocked()
	n.cfg.Trace.Emit(e)
}

// emitNoState records an event that carries no node state (client side).
func (n *Node) emitNoState(kind string, e trace.Event) {
	if n.cfg.Trace == nil {
		return
	}
	n.mu.Lock()
	stopped := n.stopped
	n.mu.Unlock()
	if stopped {
		return
	}
	e.Node, e.Kind, e.Site = n.id, kind, site()
	n.cfg.Trace.Emit(e)
}

// fired marks that a planted bug changed what the node did, so the
// evaluation can tell "not caught" apart from "never triggered". mu held.
func (n *Node) fired(b string) {
	if n.cfg.Trace != nil && !n.stopped && n.bug(b) {
		n.cfg.Trace.Emit(trace.Event{Node: n.id, Kind: "BugFired", Note: b, Site: site()})
	}
}

func site() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	return fmt.Sprintf("subject/raft/%s:%d", filepath.Base(file), line)
}

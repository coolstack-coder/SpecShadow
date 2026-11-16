package checker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/coolstack-coder/specshadow/trace"
)

// Hand-written traces, each breaking exactly one rule, to make sure the spec
// catches what it claims to catch (and accepts a correct history).

func jar(t *testing.T) string {
	t.Helper()
	p := os.Getenv("SPECSHADOW_TLC")
	if p == "" {
		p = filepath.Join("..", "tools", "tla2tools.jar")
	}
	if _, err := os.Stat(p); err != nil {
		t.Skip("tla2tools.jar not found; set SPECSHADOW_TLC")
	}
	return p
}

func st(term uint64, role, vote string, commit uint64, log ...uint64) *trace.State {
	return &trace.State{Term: term, Role: role, Vote: vote, Log: log, Commit: commit}
}

// boot: three nodes start, n1 wins term 1 with n2's vote and commits its no-op.
func boot() []trace.Event {
	return []trace.Event{
		{Node: "n1", Kind: "Restart", State: st(0, "F", "", 0)},
		{Node: "n2", Kind: "Restart", State: st(0, "F", "", 0)},
		{Node: "n3", Kind: "Restart", State: st(0, "F", "", 0)},
		{Node: "n1", Kind: "PreCandidate", State: st(0, "P", "", 0)},
		{Node: "n1", Kind: "BecomeCandidate", State: st(1, "C", "n1", 0)},
		{Node: "n2", Kind: "BecomeFollower", State: st(1, "F", "", 0)},
		{Node: "n2", Kind: "Vote", Peer: "n1", MTerm: 1, Granted: true, State: st(1, "F", "n1", 0)},
		{Node: "n1", Kind: "BecomeLeader", State: st(1, "L", "n1", 0, 1)},
		{Node: "n2", Kind: "AppendOK", Peer: "n1", MTerm: 1, Prev: 0, N: 1, State: st(1, "F", "n1", 0, 1)},
		{Node: "n1", Kind: "LeaderCommit", Idx: 1, State: st(1, "L", "n1", 1, 1)},
		{Node: "n1", Kind: "ClientRequest", Idx: 2, State: st(1, "L", "n1", 1, 1, 1)},
		{Node: "n2", Kind: "AppendOK", Peer: "n1", MTerm: 1, Prev: 1, N: 1, Idx: 1, State: st(1, "F", "n1", 1, 1, 1)},
		{Node: "n1", Kind: "LeaderCommit", Idx: 2, State: st(1, "L", "n1", 2, 1, 1)},
		{Node: "n1", Kind: "Apply", Lo: 1, Hi: 2, Terms: []uint64{1, 1}, State: st(1, "L", "n1", 2, 1, 1)},
		{Node: "n1", Kind: "WriteAck", Idx: 2, MTerm: 1},
	}
}

func check(t *testing.T, evs []trace.Event) *Result {
	t.Helper()
	for i := range evs {
		evs[i].Seq = i + 1
	}
	r, err := Check(evs, Options{Jar: jar(t)})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCorrectHistoryPasses(t *testing.T) {
	evs := append(boot(),
		trace.Event{Node: "n1", Kind: "ReadStart", RID: 1, State: st(1, "L", "n1", 2, 1, 1)},
		trace.Event{Node: "n1", Kind: "ReadServe", RID: 1, Idx: 2, Lease: true, State: st(1, "L", "n1", 2, 1, 1)},
	)
	if r := check(t, evs); !r.OK {
		t.Fatalf("correct history rejected: %s at step %d", r.Rule, r.FailStep)
	}
}

func TestRules(t *testing.T) {
	cases := []struct {
		rule  string
		extra []trace.Event
	}{
		{"DoubleVote", []trace.Event{
			{Node: "n3", Kind: "BecomeFollower", State: st(1, "F", "", 0)},
			{Node: "n3", Kind: "Vote", Peer: "n1", MTerm: 1, Granted: true, State: st(1, "F", "n1", 0)},
			{Node: "n3", Kind: "Vote", Peer: "n2", MTerm: 1, Granted: true, State: st(1, "F", "n2", 0)},
		}},
		{"VoteForStaleLog", []trace.Event{
			{Node: "n2", Kind: "BecomeFollower", State: st(2, "F", "", 1, 1, 1)},
			{Node: "n2", Kind: "Vote", Peer: "n3", MTerm: 2, Granted: true, LastIdx: 0, LastTerm: 0, State: st(2, "F", "n3", 1, 1, 1)},
		}},
		{"LeaderWithoutQuorum", []trace.Event{
			{Node: "n3", Kind: "BecomeCandidate", State: st(2, "C", "n3", 0)},
			{Node: "n3", Kind: "BecomeLeader", State: st(2, "L", "n3", 0, 2)},
		}},
		{"StaleRead", []trace.Event{
			{Node: "n3", Kind: "AppendOK", Peer: "n1", MTerm: 1, Prev: 0, N: 2, State: st(1, "F", "", 0, 1, 1)},
			{Node: "n3", Kind: "PreCandidate", State: st(1, "P", "", 0, 1, 1)},
			{Node: "n3", Kind: "BecomeCandidate", State: st(2, "C", "n3", 0, 1, 1)},
			{Node: "n2", Kind: "BecomeFollower", State: st(2, "F", "", 2, 1, 1)},
			{Node: "n2", Kind: "Vote", Peer: "n3", MTerm: 2, Granted: true, LastIdx: 2, LastTerm: 1, State: st(2, "F", "n3", 2, 1, 1)},
			{Node: "n3", Kind: "BecomeLeader", State: st(2, "L", "n3", 0, 1, 1, 2)},
			{Node: "n2", Kind: "AppendOK", Peer: "n3", MTerm: 2, Prev: 2, N: 1, State: st(2, "F", "n3", 2, 1, 1, 2)},
			{Node: "n3", Kind: "LeaderCommit", Idx: 3, State: st(2, "L", "n3", 3, 1, 1, 2)},
			{Node: "n3", Kind: "ClientRequest", Idx: 4, State: st(2, "L", "n3", 3, 1, 1, 2, 2)},
			{Node: "n2", Kind: "AppendOK", Peer: "n3", MTerm: 2, Prev: 3, N: 1, State: st(2, "F", "n3", 3, 1, 1, 2, 2)},
			{Node: "n3", Kind: "LeaderCommit", Idx: 4, State: st(2, "L", "n3", 4, 1, 1, 2, 2)},
			{Node: "n3", Kind: "WriteAck", Idx: 4, MTerm: 2},
			// n1 still believes it leads term 1 and serves a read at index 2
			{Node: "n1", Kind: "ReadStart", RID: 7, State: st(1, "L", "n1", 2, 1, 1)},
			{Node: "n1", Kind: "ReadServe", RID: 7, Idx: 2, Lease: true, State: st(1, "L", "n1", 2, 1, 1)},
		}},
		{"CommittedOldTermByCounting", []trace.Event{
			{Node: "n1", Kind: "ClientRequest", Idx: 3, State: st(1, "L", "n1", 2, 1, 1, 1)},
			{Node: "n2", Kind: "AppendOK", Peer: "n1", MTerm: 1, Prev: 2, N: 1, State: st(1, "F", "n1", 2, 1, 1, 1)},
			{Node: "n2", Kind: "PreCandidate", State: st(1, "P", "n1", 2, 1, 1, 1)},
			{Node: "n2", Kind: "BecomeCandidate", State: st(2, "C", "n2", 2, 1, 1, 1)},
			{Node: "n3", Kind: "BecomeFollower", State: st(2, "F", "", 0)},
			{Node: "n3", Kind: "Vote", Peer: "n2", MTerm: 2, Granted: true, LastIdx: 3, LastTerm: 1, State: st(2, "F", "n2", 0)},
			// no no-op from the new leader, then it counts replicas of a term-1 entry
			{Node: "n2", Kind: "BecomeLeader", State: st(2, "L", "n2", 2, 1, 1, 1)},
			{Node: "n2", Kind: "LeaderCommit", Idx: 3, State: st(2, "L", "n2", 3, 1, 1, 1)},
		}},
		{"AcceptedStaleLeader", []trace.Event{
			{Node: "n2", Kind: "BecomeFollower", State: st(5, "F", "", 2, 1, 1)},
			{Node: "n2", Kind: "AppendOK", Peer: "n1", MTerm: 1, Prev: 2, N: 0, State: st(5, "F", "", 2, 1, 1)},
		}},
		{"LostVoteOnRestart", []trace.Event{
			{Node: "n3", Kind: "BecomeFollower", State: st(1, "F", "", 0)},
			{Node: "n3", Kind: "Vote", Peer: "n1", MTerm: 1, Granted: true, State: st(1, "F", "n1", 0)},
			{Node: "n3", Kind: "Crash"},
			{Node: "n3", Kind: "Restart", State: st(0, "F", "", 0)},
		}},
		{"AppliedUncommittedEntry", []trace.Event{
			{Node: "n2", Kind: "Apply", Lo: 1, Hi: 3, Terms: []uint64{1, 1, 1}, State: st(1, "F", "n1", 1, 1, 1)},
		}},
	}
	for _, c := range cases {
		t.Run(c.rule, func(t *testing.T) {
			evs := append(boot(), c.extra...)
			r := check(t, evs)
			if r.OK || r.Rule != c.rule {
				t.Fatalf("got ok=%v rule=%q at step %d, want %q", r.OK, r.Rule, r.FailStep, c.rule)
			}
			if want := len(boot()) + len(c.extra); r.FailStep != want {
				t.Fatalf("caught at step %d, want the last step %d", r.FailStep, want)
			}
		})
	}
}

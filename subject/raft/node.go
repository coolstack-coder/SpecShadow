// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, 2014) with the usual production extras:
// PreVote, CheckQuorum, leader leases, ReadIndex reads, snapshots with
// InstallSnapshot, conflict-term fast backtracking and group commit on the
// leader.
//
// Locking: n.persistMu serializes every write to Storage, n.mu guards the
// in-memory state. When both are needed persistMu is always taken first.
// The leader's own log writes go through persistLoop so proposals don't
// block on fsync; followers persist synchronously before replying, which is
// what the paper requires.
package raft

import (
	"context"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/coolstack-coder/specshadow/trace"
)

// Config for a single raft group member.
type Config struct {
	ID           string
	Group        string
	Peers        []string // every voter, including ID
	Storage      Storage
	Transport    Transport
	StateMachine StateMachine

	HeartbeatInterval time.Duration // default 50ms
	ElectionTimeout   time.Duration // base, randomized in [ET, 2*ET). default 300ms
	SnapshotThreshold uint64        // entries between snapshots, 0 = default 2000
	MaxAppendEntries  int           // per AppendEntries RPC, default 256
	DisableLeaseReads bool

	// SpecShadow
	Clock                Clock         // nil = wall clock
	Trace                trace.Sink    // nil = no tracing
	Bug                  string        // planted bug, see instrument.go
	PauseAfterLeaseCheck time.Duration // stall a read right after the lease check

	Logger *slog.Logger
}

type applyResult struct {
	val any
	err error
}

type waiter struct {
	term uint64
	ch   chan applyResult
}

// Node is one member of one raft group.
type Node struct {
	cfg    Config
	clock  Clock
	id     string
	peers  []string
	quorum int
	log    *slog.Logger

	persistMu sync.Mutex
	mu        sync.Mutex
	applyCond *sync.Cond

	role    Role
	term    uint64
	vote    string
	leader  string
	hsDirty bool

	// entries[0] is a sentinel holding the snapshot's index and term
	entries     []Entry
	persisted   uint64
	commitIndex uint64
	lastApplied uint64
	snapData    []byte
	pendingSnap *SnapshotRequest

	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	lastContact map[string]time.Time // when we last heard back (CheckQuorum)
	ackSent     map[string]time.Time // send time of latest acked RPC (lease)
	leaderStop  chan struct{}
	kicks       map[string]chan struct{}
	persistKick chan struct{}

	electionDeadline time.Time
	lastHeardLeader  time.Time
	bootGraceUntil   time.Time
	preVotes         map[string]bool
	votes            map[string]bool

	waiters   map[uint64]waiter
	appliedCh chan struct{}

	stats   Stats
	stopped bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	rng     *rand.Rand
}

// NewNode restores state from storage and starts the node's goroutines.
func NewNode(cfg Config) (*Node, error) {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 300 * time.Millisecond
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 2000
	}
	if cfg.MaxAppendEntries == 0 {
		cfg.MaxAppendEntries = 256
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	n := &Node{
		cfg:         cfg,
		id:          cfg.ID,
		quorum:      len(cfg.Peers)/2 + 1,
		log:         cfg.Logger.With("group", cfg.Group, "node", cfg.ID),
		persistKick: make(chan struct{}, 1),
		waiters:     make(map[uint64]waiter),
		appliedCh:   make(chan struct{}),
		stopCh:      make(chan struct{}),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(cfg.ID)*7919))),
	}
	n.clock = cfg.Clock
	if n.clock == nil {
		n.clock = realClock{}
	}
	n.applyCond = sync.NewCond(&n.mu)
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.peers = append(n.peers, p)
		}
	}

	hs, meta, snap, ents, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	if len(snap) > 0 {
		if err := cfg.StateMachine.Restore(snap); err != nil {
			return nil, err
		}
	}
	n.term, n.vote = hs.Term, hs.Vote
	n.entries = append([]Entry{{Index: meta.Index, Term: meta.Term}}, ents...)
	n.snapData = snap
	n.persisted = n.lastIndex()
	n.commitIndex = meta.Index
	n.lastApplied = meta.Index

	now := n.now()
	// A freshly restarted node has forgotten whether it heard from a leader
	// recently. Refusing pre-votes for one election timeout keeps the
	// current leader's lease honest across fast restarts.
	n.bootGraceUntil = now.Add(cfg.ElectionTimeout)
	n.resetElectionTimerLocked()
	n.emitLocked("Restart", trace.Event{})

	n.wg.Add(3)
	go n.tickLoop()
	go n.applyLoop()
	go n.persistLoop()
	n.log.Info("raft node started", "term", n.term, "snapshot", meta.Index, "last_index", n.lastIndex())
	return n, nil
}

// ---- log helpers (call with mu held) ----

func (n *Node) firstIndex() uint64 { return n.entries[0].Index }
func (n *Node) lastIndex() uint64  { return n.entries[len(n.entries)-1].Index }
func (n *Node) lastTerm() uint64   { return n.entries[len(n.entries)-1].Term }

// termAt returns the term of entry i, ok=false if it was compacted or does
// not exist yet.
func (n *Node) termAt(i uint64) (uint64, bool) {
	first := n.firstIndex()
	if i < first || i > n.lastIndex() {
		return 0, false
	}
	return n.entries[i-first].Term, true
}

// slice copies entries [lo, hi]. lo must be > firstIndex.
func (n *Node) slice(lo, hi uint64) []Entry {
	if lo > hi {
		return nil
	}
	first := n.firstIndex()
	out := make([]Entry, hi-lo+1)
	copy(out, n.entries[lo-first:hi-first+1])
	return out
}

// truncateFrom drops entries >= i and fails anyone waiting on them.
func (n *Node) truncateFrom(i uint64) {
	first := n.firstIndex()
	if i <= first || i > n.lastIndex() {
		return
	}
	for idx := i; idx <= n.lastIndex(); idx++ {
		if w, ok := n.waiters[idx]; ok {
			w.ch <- applyResult{err: ErrLeadershipLost}
			delete(n.waiters, idx)
		}
	}
	n.entries = n.entries[:i-first]
	if n.persisted >= i {
		n.persisted = i - 1
	}
}

func (n *Node) lastIndexOfTerm(t uint64) uint64 {
	for i := len(n.entries) - 1; i > 0; i-- {
		if n.entries[i].Term == t {
			return n.entries[i].Index
		}
		if n.entries[i].Term < t {
			break
		}
	}
	return 0
}

func (n *Node) upToDate(lastIdx, lastTerm uint64) bool {
	myTerm := n.lastTerm()
	ok := lastTerm > myTerm || (lastTerm == myTerm && lastIdx >= n.lastIndex())
	if n.bug(BugNoUpToDate) {
		if !ok {
			n.fired(BugNoUpToDate)
		}
		return true
	}
	return ok
}

func (n *Node) resetElectionTimerLocked() {
	et := n.cfg.ElectionTimeout
	n.electionDeadline = n.now().Add(et + time.Duration(n.rng.Int63n(int64(et))))
}

// persistLocked writes hard state (if dirty) and every entry past
// n.persisted. Requires persistMu and mu.
func (n *Node) persistLocked() error {
	var hs *HardState
	if n.hsDirty {
		hs = &HardState{Term: n.term, Vote: n.vote}
	}
	if n.persisted > n.lastIndex() {
		n.persisted = n.lastIndex()
	}
	ents := n.slice(n.persisted+1, n.lastIndex())
	if hs == nil && len(ents) == 0 {
		return nil
	}
	if err := n.cfg.Storage.Save(hs, ents); err != nil {
		n.log.Error("persist failed", "err", err)
		return err
	}
	n.hsDirty = false
	n.persisted = n.lastIndex()
	return nil
}

// ---- role transitions (mu held) ----

func (n *Node) becomeFollower(term uint64, leader string) {
	if term > n.term {
		n.term = term
		n.vote = ""
		n.hsDirty = true
	}
	if n.role == Leader {
		close(n.leaderStop)
		n.kicks = nil
		n.stats.StepDowns++
		n.log.Info("stepping down", "term", n.term)
	}
	n.role = Follower
	n.leader = leader
	n.emitLocked("BecomeFollower", trace.Event{})
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leader = n.id
	n.stats.ElectionsWon++
	last := n.lastIndex()
	now := n.now()
	n.nextIndex = make(map[string]uint64)
	n.matchIndex = make(map[string]uint64)
	n.lastContact = make(map[string]time.Time)
	n.ackSent = make(map[string]time.Time)
	n.kicks = make(map[string]chan struct{})
	n.leaderStop = make(chan struct{})
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.lastContact[p] = now // grace period before CheckQuorum kicks in
		n.kicks[p] = make(chan struct{}, 1)
	}
	if !n.bug(BugCommitOldTerm) {
		n.entries = append(n.entries, Entry{Index: last + 1, Term: n.term, Type: EntryNoop})
	}
	n.emitLocked("BecomeLeader", trace.Event{})
	n.log.Info("became leader", "term", n.term, "last_index", last+1)
	for _, p := range n.peers {
		n.wg.Add(1)
		go n.replicator(p, n.term, n.leaderStop, n.kicks[p])
	}
	n.kickLocked()
	if len(n.peers) == 0 {
		// single node group: nothing to wait for besides our own disk
		n.advanceCommitLocked()
	}
}

func (n *Node) kickLocked() {
	select {
	case n.persistKick <- struct{}{}:
	default:
	}
	for _, k := range n.kicks {
		select {
		case k <- struct{}{}:
		default:
		}
	}
}

func (n *Node) advanceCommitLocked() {
	if n.role != Leader {
		return
	}
	matches := make([]uint64, 0, len(n.peers)+1)
	matches = append(matches, n.persisted)
	for _, p := range n.peers {
		matches = append(matches, n.matchIndex[p])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	N := matches[n.quorum-1]
	if N <= n.commitIndex {
		return
	}
	// only entries from our own term are committed by counting replicas
	if t, ok := n.termAt(N); ok && (t == n.term || n.bug(BugCommitOldTerm)) {
		if t != n.term {
			n.fired(BugCommitOldTerm)
		}
		n.commitIndex = N
		n.applyCond.Broadcast()
		n.emitLocked("LeaderCommit", trace.Event{Idx: N})
	}
}

// ---- background loops ----

func (n *Node) tickLoop() {
	defer n.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		now := n.now()
		if n.role == Leader {
			if len(n.peers) > 0 {
				alive := 1
				for _, p := range n.peers {
					if now.Sub(n.lastContact[p]) < n.cfg.ElectionTimeout {
						alive++
					}
				}
				if alive < n.quorum {
					// CheckQuorum: we're probably on the wrong side of a
					// partition, stop pretending to be leader
					n.log.Warn("lost contact with quorum", "term", n.term)
					n.becomeFollower(n.term, "")
					n.resetElectionTimerLocked()
				}
			}
		} else if now.After(n.electionDeadline) {
			n.resetElectionTimerLocked()
			go n.campaign()
		}
		n.mu.Unlock()
	}
}

func (n *Node) persistLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.persistKick:
		}
		n.persistMu.Lock()
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			n.persistMu.Unlock()
			return
		}
		var hs *HardState
		if n.hsDirty {
			hs = &HardState{Term: n.term, Vote: n.vote}
		}
		if n.persisted > n.lastIndex() {
			n.persisted = n.lastIndex()
		}
		to := n.lastIndex()
		ents := n.slice(n.persisted+1, to)
		n.mu.Unlock()

		var err error
		if hs != nil || len(ents) > 0 {
			err = n.cfg.Storage.Save(hs, ents)
		}

		n.mu.Lock()
		if err != nil {
			n.log.Error("persist failed", "err", err)
		} else {
			if hs != nil && hs.Term == n.term && hs.Vote == n.vote {
				n.hsDirty = false
			}
			// the log can only be truncated while holding persistMu, which
			// we do, so everything up to `to` is now on disk
			if to > n.persisted {
				n.persisted = to
			}
			n.advanceCommitLocked()
		}
		n.mu.Unlock()
		n.persistMu.Unlock()
	}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	sm := n.cfg.StateMachine
	for {
		n.mu.Lock()
		for !n.stopped && n.pendingSnap == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if ps := n.pendingSnap; ps != nil {
			n.pendingSnap = nil
			n.mu.Unlock()
			if err := sm.Restore(ps.Data); err != nil {
				n.log.Error("snapshot restore failed", "err", err)
			}
			n.mu.Lock()
			if ps.Meta.Index > n.lastApplied {
				n.lastApplied = ps.Meta.Index
			}
			for idx, w := range n.waiters {
				if idx <= ps.Meta.Index {
					w.ch <- applyResult{err: ErrLeadershipLost}
					delete(n.waiters, idx)
				}
			}
			n.signalAppliedLocked()
			n.mu.Unlock()
			continue
		}
		start := n.lastApplied + 1
		end := n.commitIndex
		if end-start > 511 {
			end = start + 511
		}
		ents := n.slice(start, end)
		n.mu.Unlock()

		results := make([]any, len(ents))
		for i, e := range ents {
			if e.Type == EntryNormal {
				results[i] = sm.Apply(e)
			}
		}

		n.mu.Lock()
		if end > n.lastApplied {
			n.lastApplied = end
		}
		if len(ents) > 0 {
			terms := make([]uint64, len(ents))
			for i, e := range ents {
				terms[i] = e.Term
			}
			n.emitLocked("Apply", trace.Event{Lo: start, Hi: end, Terms: terms})
		}
		for i, e := range ents {
			if w, ok := n.waiters[e.Index]; ok {
				delete(n.waiters, e.Index)
				if w.term == e.Term {
					w.ch <- applyResult{val: results[i]}
				} else {
					w.ch <- applyResult{err: ErrLeadershipLost}
				}
			}
		}
		n.stats.Applied += uint64(len(ents))
		n.signalAppliedLocked()
		needSnap := n.lastApplied-n.firstIndex() >= n.cfg.SnapshotThreshold
		n.mu.Unlock()

		if needSnap {
			n.takeSnapshot()
		}
	}
}

func (n *Node) signalAppliedLocked() {
	close(n.appliedCh)
	n.appliedCh = make(chan struct{})
}

// takeSnapshot runs on the apply goroutine, so the state machine can't move
// underneath it.
func (n *Node) takeSnapshot() {
	data, err := n.cfg.StateMachine.Snapshot()
	if err != nil {
		n.log.Error("snapshot failed", "err", err)
		return
	}
	n.persistMu.Lock()
	defer n.persistMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()
	idx := n.lastApplied
	if n.stopped || idx <= n.firstIndex() || n.pendingSnap != nil {
		return
	}
	term, ok := n.termAt(idx)
	if !ok {
		return
	}
	retained := n.slice(idx+1, n.lastIndex())
	meta := SnapshotMeta{Index: idx, Term: term}
	if err := n.cfg.Storage.SaveSnapshot(meta, data, HardState{Term: n.term, Vote: n.vote}, retained); err != nil {
		n.log.Error("saving snapshot failed", "err", err)
		return
	}
	n.entries = append([]Entry{{Index: idx, Term: term}}, retained...)
	n.snapData = data
	n.persisted = n.lastIndex()
	n.hsDirty = false
	n.stats.SnapshotsTaken++
	n.log.Debug("snapshot taken", "index", idx, "bytes", len(data))
}

// ---- elections ----

func (n *Node) campaign() {
	n.mu.Lock()
	if n.stopped || n.role == Leader {
		n.mu.Unlock()
		return
	}
	n.role = PreCandidate
	n.leader = ""
	term := n.term
	n.emitLocked("PreCandidate", trace.Event{})
	n.preVotes = map[string]bool{n.id: true}
	req := &VoteRequest{
		Group: n.cfg.Group, Term: term + 1, Candidate: n.id,
		LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(), PreVote: true,
	}
	n.mu.Unlock()

	if n.quorum == 1 {
		n.elect(term)
		return
	}
	for _, p := range n.peers {
		go func(p string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.SendVote(ctx, p, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.role != PreCandidate || n.term != term {
				return
			}
			if resp.Granted {
				n.preVotes[p] = true
				if len(n.preVotes) == n.quorum {
					go n.elect(term)
				}
			} else if resp.Term > n.term {
				n.becomeFollower(resp.Term, "")
			}
		}(p)
	}
}

func (n *Node) elect(prevTerm uint64) {
	n.persistMu.Lock()
	n.mu.Lock()
	if n.stopped || n.role != PreCandidate || n.term != prevTerm {
		n.mu.Unlock()
		n.persistMu.Unlock()
		return
	}
	n.term++
	n.vote = n.id
	n.hsDirty = true
	n.role = Candidate
	n.votes = map[string]bool{n.id: true}
	n.stats.ElectionsStarted++
	n.resetElectionTimerLocked()
	if err := n.persistLocked(); err != nil {
		n.becomeFollower(n.term, "")
		n.mu.Unlock()
		n.persistMu.Unlock()
		return
	}
	term := n.term
	n.emitLocked("BecomeCandidate", trace.Event{LastIdx: n.lastIndex(), LastTerm: n.lastTerm()})
	n.log.Debug("starting election", "term", term)
	if n.quorum == 1 {
		n.becomeLeader()
		n.mu.Unlock()
		n.persistMu.Unlock()
		return
	}
	req := &VoteRequest{
		Group: n.cfg.Group, Term: term, Candidate: n.id,
		LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(),
	}
	n.mu.Unlock()
	n.persistMu.Unlock()

	for _, p := range n.peers {
		go func(p string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.SendVote(ctx, p, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.term {
				n.becomeFollower(resp.Term, "")
				return
			}
			if n.role != Candidate || n.term != term || !resp.Granted {
				return
			}
			n.votes[p] = true
			if len(n.votes) == n.quorum {
				n.becomeLeader()
			}
		}(p)
	}
}

// ---- replication (leader side) ----

func (n *Node) replicator(peer string, term uint64, stop <-chan struct{}, kick <-chan struct{}) {
	defer n.wg.Done()
	hb := time.NewTicker(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		for {
			more, ok := n.replicateOnce(peer, term)
			if !ok {
				return
			}
			if !more {
				break
			}
		}
		select {
		case <-n.stopCh:
			return
		case <-stop:
			return
		case <-kick:
		case <-hb.C:
		}
	}
}

func (n *Node) markAckLocked(peer string, sent time.Time) {
	n.lastContact[peer] = n.now()
	if sent.After(n.ackSent[peer]) {
		n.ackSent[peer] = sent
	}
}

// replicateOnce sends one AppendEntries or InstallSnapshot to peer. ok is
// false once we're no longer leader of term; more is true if the peer is
// still behind and we should send again right away.
func (n *Node) replicateOnce(peer string, term uint64) (more, ok bool) {
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.term != term {
		n.mu.Unlock()
		return false, false
	}
	next := n.nextIndex[peer]
	if next <= n.firstIndex() {
		req := &SnapshotRequest{
			Group: n.cfg.Group, Term: term, Leader: n.id,
			Meta: SnapshotMeta{Index: n.firstIndex(), Term: n.entries[0].Term},
			Data: n.snapData,
		}
		n.mu.Unlock()
		timeout := 4 * n.cfg.ElectionTimeout
		if timeout < 2*time.Second {
			timeout = 2 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		sent := n.now()
		resp, err := n.cfg.Transport.SendSnapshot(ctx, peer, req)
		cancel()
		n.mu.Lock()
		defer n.mu.Unlock()
		if err != nil {
			return false, true
		}
		if resp.Term > n.term {
			n.becomeFollower(resp.Term, "")
			n.resetElectionTimerLocked()
			return false, false
		}
		if n.role != Leader || n.term != term {
			return false, false
		}
		n.markAckLocked(peer, sent)
		n.stats.SnapshotsSent++
		if req.Meta.Index > n.matchIndex[peer] {
			n.matchIndex[peer] = req.Meta.Index
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitLocked()
		n.log.Info("sent snapshot", "peer", peer, "index", req.Meta.Index)
		return n.nextIndex[peer] <= n.lastIndex(), true
	}

	prev := next - 1
	prevTerm, _ := n.termAt(prev)
	end := n.lastIndex()
	if end > prev+uint64(n.cfg.MaxAppendEntries) {
		end = prev + uint64(n.cfg.MaxAppendEntries)
	}
	req := &AppendRequest{
		Group: n.cfg.Group, Term: term, Leader: n.id,
		PrevLogIndex: prev, PrevLogTerm: prevTerm,
		Entries: n.slice(next, end), LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
	sent := n.now()
	resp, err := n.cfg.Transport.SendAppend(ctx, peer, req)
	cancel()

	n.mu.Lock()
	defer n.mu.Unlock()
	if err != nil {
		return false, true
	}
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		n.resetElectionTimerLocked()
		return false, false
	}
	if n.role != Leader || n.term != term {
		return false, false
	}
	n.markAckLocked(peer, sent)
	if resp.Success {
		m := prev + uint64(len(req.Entries))
		if m > n.matchIndex[peer] {
			n.matchIndex[peer] = m
		}
		if m+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = m + 1
		}
		n.advanceCommitLocked()
	} else {
		nx := resp.ConflictIndex
		if resp.ConflictTerm != 0 {
			if li := n.lastIndexOfTerm(resp.ConflictTerm); li > 0 {
				nx = li + 1
			}
		}
		if nx < 1 {
			nx = 1
		}
		if nx > n.lastIndex()+1 {
			nx = n.lastIndex() + 1
		}
		n.nextIndex[peer] = nx
	}
	return n.nextIndex[peer] <= n.lastIndex(), true
}

// ---- RPC handlers ----

// HandleVote answers RequestVote and PreVote.
func (n *Node) HandleVote(req *VoteRequest) *VoteResponse {
	if req.PreVote {
		n.mu.Lock()
		defer n.mu.Unlock()
		resp := &VoteResponse{Term: n.term}
		if n.stopped || req.Term < n.term {
			return resp
		}
		now := n.now()
		// leader stickiness: don't help anyone depose a leader we can hear
		if n.role == Leader || now.Before(n.bootGraceUntil) ||
			(n.leader != "" && now.Sub(n.lastHeardLeader) < n.cfg.ElectionTimeout) {
			return resp
		}
		resp.Granted = n.upToDate(req.LastLogIndex, req.LastLogTerm)
		return resp
	}

	n.persistMu.Lock()
	defer n.persistMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || req.Term < n.term {
		return &VoteResponse{Term: n.term}
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	granted := false
	if (n.vote == "" || n.vote == req.Candidate || n.bug(BugDoubleVote)) &&
		n.upToDate(req.LastLogIndex, req.LastLogTerm) {
		if n.vote != "" && n.vote != req.Candidate {
			n.fired(BugDoubleVote)
		}
		if n.vote != req.Candidate {
			n.vote = req.Candidate
			n.hsDirty = true
		}
		granted = true
		n.resetElectionTimerLocked()
	}
	if n.bug(BugVoteNotSynced) {
		// hard state is written with the next log append, not now
	} else if err := n.persistLocked(); err != nil {
		granted = false
	}
	if granted {
		n.emitLocked("Vote", trace.Event{Peer: req.Candidate, MTerm: req.Term, Granted: true,
			LastIdx: req.LastLogIndex, LastTerm: req.LastLogTerm})
	}
	return &VoteResponse{Term: n.term, Granted: granted}
}

// HandleAppend answers AppendEntries (and heartbeats).
func (n *Node) HandleAppend(req *AppendRequest) *AppendResponse {
	n.persistMu.Lock()
	defer n.persistMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &AppendResponse{Term: n.term}
	if n.stopped || (req.Term < n.term && !n.bug(BugStaleTerm)) {
		return resp
	}
	if req.Term < n.term {
		n.fired(BugStaleTerm)
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollower(req.Term, req.Leader)
	}
	n.leader = req.Leader
	n.lastHeardLeader = n.now()
	n.resetElectionTimerLocked()
	resp.Term = n.term

	lastNew := req.PrevLogIndex + uint64(len(req.Entries))
	prev, prevTerm, ents := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	// anything at or below our snapshot is committed and therefore matches
	if first := n.firstIndex(); prev < first {
		skip := first - prev
		if skip >= uint64(len(ents)) {
			ents = nil
		} else {
			ents = ents[skip:]
		}
		prev, prevTerm = first, n.entries[0].Term
	}

	if prev > n.lastIndex() {
		resp.ConflictIndex = n.lastIndex() + 1
		resp.LastIndex = n.lastIndex()
		n.persistLocked() // the term may have changed
		return resp
	}
	if t, _ := n.termAt(prev); t != prevTerm {
		resp.ConflictTerm = t
		ci := prev
		for ci > n.firstIndex()+1 {
			if pt, _ := n.termAt(ci - 1); pt != t {
				break
			}
			ci--
		}
		resp.ConflictIndex = ci
		resp.LastIndex = n.lastIndex()
		n.persistLocked()
		return resp
	}

	for i, e := range ents {
		if e.Index > n.lastIndex() {
			n.entries = append(n.entries, ents[i:]...)
			break
		}
		if t, _ := n.termAt(e.Index); t != e.Term {
			n.truncateFrom(e.Index)
			n.entries = append(n.entries, ents[i:]...)
			break
		}
	}
	if err := n.persistLocked(); err != nil {
		return resp
	}

	if req.LeaderCommit > n.commitIndex {
		c := req.LeaderCommit
		if lastNew < c && !n.bug(BugCommitOverrun) {
			c = lastNew
		}
		if lastNew < c && c > n.commitIndex && n.lastIndex() > lastNew {
			n.fired(BugCommitOverrun)
		}
		if c > n.lastIndex() {
			c = n.lastIndex()
		}
		if c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Broadcast()
		}
	}
	resp.Success = true
	resp.LastIndex = n.lastIndex()
	n.emitLocked("AppendOK", trace.Event{Peer: req.Leader, MTerm: req.Term, Prev: req.PrevLogIndex,
		N: len(req.Entries), Idx: req.LeaderCommit})
	return resp
}

// HandleSnapshot answers InstallSnapshot.
func (n *Node) HandleSnapshot(req *SnapshotRequest) *SnapshotResponse {
	n.persistMu.Lock()
	defer n.persistMu.Unlock()
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &SnapshotResponse{Term: n.term}
	if n.stopped || req.Term < n.term {
		return resp
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollower(req.Term, req.Leader)
	}
	n.leader = req.Leader
	n.lastHeardLeader = n.now()
	n.resetElectionTimerLocked()
	resp.Term = n.term

	if req.Meta.Index <= n.commitIndex {
		n.persistLocked()
		return resp
	}
	var retained []Entry
	if t, ok := n.termAt(req.Meta.Index); ok && t == req.Meta.Term {
		retained = n.slice(req.Meta.Index+1, n.lastIndex())
	} else {
		n.truncateFrom(n.firstIndex() + 1)
	}
	hs := HardState{Term: n.term, Vote: n.vote}
	if err := n.cfg.Storage.SaveSnapshot(req.Meta, req.Data, hs, retained); err != nil {
		n.log.Error("install snapshot failed", "err", err)
		return resp
	}
	n.entries = append([]Entry{{Index: req.Meta.Index, Term: req.Meta.Term}}, retained...)
	n.snapData = req.Data
	n.persisted = n.lastIndex()
	n.hsDirty = false
	n.commitIndex = req.Meta.Index
	n.pendingSnap = req
	n.stats.SnapshotsInstalled++
	n.applyCond.Broadcast()
	n.log.Info("installed snapshot", "index", req.Meta.Index, "from", req.Leader)
	return resp
}

// ---- client API ----

// Propose replicates data and blocks until it is applied, returning what the
// state machine returned. If ctx expires the outcome is unknown: the entry
// may still commit later, so clients must make retries idempotent.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != Leader {
		l := n.leader
		n.mu.Unlock()
		return nil, &NotLeaderError{Leader: l}
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: data}
	n.entries = append(n.entries, e)
	ch := make(chan applyResult, 1)
	n.waiters[e.Index] = waiter{term: e.Term, ch: ch}
	n.stats.Proposals++
	n.emitLocked("ClientRequest", trace.Event{Idx: e.Index})
	n.kickLocked()
	n.mu.Unlock()

	select {
	case r := <-ch:
		if r.err == nil {
			n.emitNoState("WriteAck", trace.Event{Idx: e.Index, MTerm: e.Term})
		}
		return r.val, r.err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, e.Index)
		n.mu.Unlock()
		return nil, ctx.Err()
	case <-n.stopCh:
		return nil, ErrStopped
	}
}

// ReadIndex returns once the local state machine reflects every write that
// was committed before the call started, i.e. reading locally afterwards is
// linearizable. Uses the leader lease when it is valid, otherwise confirms
// leadership with a round of heartbeats.
func (n *Node) ReadIndex(ctx context.Context) error {
	n.mu.Lock()
	rid := 0
	if n.cfg.Trace != nil && n.role == Leader && !n.stopped {
		rid = n.cfg.Trace.NextReadID()
		n.emitLocked("ReadStart", trace.Event{RID: rid})
	}
	for {
		if n.stopped {
			n.mu.Unlock()
			return ErrStopped
		}
		if n.role != Leader {
			l := n.leader
			n.mu.Unlock()
			return &NotLeaderError{Leader: l}
		}
		// a new leader doesn't know the real commit index until its noop
		// from this term commits
		if t, ok := n.termAt(n.commitIndex); ok && t == n.term {
			break
		}
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
		n.mu.Lock()
	}
	readIdx := n.commitIndex
	term := n.term
	lease := n.leaseValidLocked()
	if lease {
		n.stats.LeaseReads++
		n.mu.Unlock()
	} else {
		n.stats.QuorumReads++
		n.mu.Unlock()
		if err := n.confirmLeadership(ctx, term); err != nil {
			return err
		}
	}
	if d := n.cfg.PauseAfterLeaseCheck; d > 0 {
		time.Sleep(d) // a GC pause or a descheduled thread, at the worst moment
	}
	if err := n.WaitApplied(ctx, readIdx); err != nil {
		return err
	}
	if rid != 0 {
		n.mu.Lock()
		n.emitLocked("ReadServe", trace.Event{RID: rid, Idx: readIdx, Lease: lease, MTerm: term})
		n.mu.Unlock()
	}
	return nil
}

func (n *Node) leaseValidLocked() bool {
	if n.cfg.DisableLeaseReads {
		return false
	}
	if len(n.peers) == 0 {
		return true
	}
	lease := n.cfg.ElectionTimeout * 3 / 4 // leave room for clock drift
	if n.bug(BugLongLease) {
		lease = 2 * n.cfg.ElectionTimeout
	}
	now := n.now()
	count, strict := 1, 1
	for _, p := range n.peers {
		if now.Sub(n.ackSent[p]) < lease {
			count++
		}
		if now.Sub(n.ackSent[p]) < n.cfg.ElectionTimeout*3/4 {
			strict++
		}
	}
	if count >= n.quorum && strict < n.quorum {
		n.fired(BugLongLease)
	}
	return count >= n.quorum
}

func (n *Node) confirmLeadership(ctx context.Context, term uint64) error {
	if len(n.peers) == 0 {
		return nil
	}
	n.mu.Lock()
	req := &AppendRequest{Group: n.cfg.Group, Term: term, Leader: n.id, LeaderCommit: n.commitIndex}
	n.mu.Unlock()
	acks := make(chan bool, len(n.peers))
	for _, p := range n.peers {
		go func(p string) {
			cctx, cancel := context.WithTimeout(ctx, n.cfg.ElectionTimeout)
			defer cancel()
			sent := n.now()
			resp, err := n.cfg.Transport.SendAppend(cctx, p, req)
			if err != nil {
				acks <- false
				return
			}
			n.mu.Lock()
			ok := resp.Term == term && n.role == Leader && n.term == term
			if resp.Term > n.term {
				n.becomeFollower(resp.Term, "")
			} else if ok {
				n.markAckLocked(p, sent)
			}
			n.mu.Unlock()
			acks <- ok
		}(p)
	}
	got, done := 1, 0
	for done < len(n.peers) {
		select {
		case ok := <-acks:
			done++
			if ok {
				got++
			}
			if got >= n.quorum {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	n.mu.Lock()
	l := n.leader
	n.mu.Unlock()
	return &NotLeaderError{Leader: l}
}

// WaitApplied blocks until lastApplied >= idx.
func (n *Node) WaitApplied(ctx context.Context, idx uint64) error {
	for {
		n.mu.Lock()
		if n.lastApplied >= idx {
			n.mu.Unlock()
			return nil
		}
		ch := n.appliedCh
		n.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrStopped
		}
	}
}

// StepDown is a cheap leadership transfer: give up leadership and stay
// quiet for a few election timeouts so somebody else wins. Good enough for
// rebalancing in the demo lab.
func (n *Node) StepDown() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return
	}
	n.becomeFollower(n.term, "")
	n.electionDeadline = n.now().Add(3 * n.cfg.ElectionTimeout)
}

// IsLeader reports whether this node currently believes it leads.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// Leader returns the current leader hint.
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{
		ID: n.id, Group: n.cfg.Group, Role: n.role.String(), Term: n.term,
		Leader: n.leader, Vote: n.vote, CommitIndex: n.commitIndex,
		AppliedIndex: n.lastApplied, LastIndex: n.lastIndex(),
		SnapshotIndex: n.firstIndex(), Peers: append([]string(nil), n.cfg.Peers...),
		Stats: n.stats,
	}
	if n.role == Leader {
		s.Match = map[string]uint64{n.id: n.persisted}
		for p, m := range n.matchIndex {
			s.Match[p] = m
		}
	}
	return s
}

// Stop shuts the node down. Safe to call more than once.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	if n.hsDirty && n.vote != "" && n.bug(BugVoteNotSynced) {
		n.fired(BugVoteNotSynced) // crashing with a granted vote not on disk
	}
	n.stopped = true
	close(n.stopCh)
	if n.role == Leader {
		close(n.leaderStop)
	}
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()

	n.persistMu.Lock()
	n.mu.Lock()
	for idx, w := range n.waiters {
		w.ch <- applyResult{err: ErrStopped}
		delete(n.waiters, idx)
	}
	n.mu.Unlock()
	n.cfg.Storage.Close()
	n.persistMu.Unlock()
}

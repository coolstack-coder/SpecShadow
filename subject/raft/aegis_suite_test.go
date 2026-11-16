package raft_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/coolstack-coder/specshadow/subject/raft"
	"github.com/coolstack-coder/specshadow/subject/simnet"
)

// recorder is a trivial state machine that remembers every command.
type recorder struct {
	mu  sync.Mutex
	log []string
}

func (r *recorder) Apply(e raft.Entry) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, string(e.Data))
	return len(r.log)
}

func (r *recorder) Snapshot() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return json.Marshal(r.log)
}

func (r *recorder) Restore(b []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return json.Unmarshal(b, &r.log)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.log...)
}

type cluster struct {
	t        *testing.T
	net      *simnet.Network
	ids      []string
	nodes    map[string]*raft.Node
	sms      map[string]*recorder
	stores   map[string]*raft.MemoryStorage
	snapEach uint64
	mu       sync.Mutex
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newCluster(t *testing.T, n int, snapEach uint64) *cluster {
	c := &cluster{
		t: t, net: simnet.New(time.Now().UnixNano()),
		nodes: map[string]*raft.Node{}, sms: map[string]*recorder{},
		stores: map[string]*raft.MemoryStorage{}, snapEach: snapEach,
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.stores[id] = raft.NewMemoryStorage()
		c.start(id)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) start(id string) {
	sm := &recorder{}
	node, err := raft.NewNode(raft.Config{
		ID: id, Group: "g", Peers: c.ids,
		Storage: c.stores[id], Transport: c.net.Transport(id), StateMachine: sm,
		HeartbeatInterval: 20 * time.Millisecond, ElectionTimeout: 120 * time.Millisecond,
		SnapshotThreshold: c.snapEach, Logger: quiet,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[id], c.sms[id] = node, sm
	c.mu.Unlock()
	c.net.Register(id, node)
}

func (c *cluster) crash(id string) {
	c.net.Unregister(id)
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n != nil {
		n.Stop()
	}
}

func (c *cluster) shutdown() {
	for _, id := range c.ids {
		c.crash(id)
	}
}

func (c *cluster) node(id string) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// leader waits until exactly one node in `among` claims leadership in the
// highest term and returns it.
func (c *cluster) leader(among ...string) string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		byTerm := map[uint64][]string{}
		var top uint64
		for _, id := range among {
			n := c.node(id)
			if n == nil {
				continue
			}
			st := n.Status()
			if st.Role == "leader" {
				byTerm[st.Term] = append(byTerm[st.Term], id)
				if st.Term > top {
					top = st.Term
				}
			}
		}
		for term, ls := range byTerm {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
		}
		if len(byTerm[top]) == 1 {
			return byTerm[top][0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected among %v", among)
	return ""
}

// propose submits cmd through whoever is leader, retrying until it sticks.
func (c *cluster) propose(cmd string, among ...string) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		l := c.leader(among...)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := c.node(l).Propose(ctx, []byte(cmd))
		cancel()
		if err == nil {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	c.t.Fatalf("could not commit %q", cmd)
}

// converge waits until every live node applied the same log of at least
// min commands.
func (c *cluster) converge(min int, among ...string) []string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	deadline := time.Now().Add(10 * time.Second)
	var last []string
	for time.Now().Before(deadline) {
		var ref []string
		same := true
		for i, id := range among {
			c.mu.Lock()
			sm := c.sms[id]
			c.mu.Unlock()
			got := sm.snapshot()
			if i == 0 {
				ref = got
				continue
			}
			if !equal(ref, got) {
				same = false
				break
			}
		}
		if same && len(ref) >= min {
			return ref
		}
		last = ref
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("logs did not converge (want >= %d, first node has %d)", min, len(last))
	return nil
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.leader()
	term := c.node(l).Status().Term
	// nothing is wrong, so leadership should be stable
	time.Sleep(600 * time.Millisecond)
	if l2 := c.leader(); l2 != l || c.node(l2).Status().Term != term {
		t.Fatalf("leader changed without failures: %s/%d -> %s/%d", l, term, l2, c.node(l2).Status().Term)
	}
}

func TestReElectionAfterLeaderIsolated(t *testing.T) {
	c := newCluster(t, 3, 0)
	old := c.leader()
	c.net.Isolate(old)
	var rest []string
	for _, id := range c.ids {
		if id != old {
			rest = append(rest, id)
		}
	}
	nl := c.leader(rest...)
	if nl == old {
		t.Fatal("isolated node still leader")
	}
	// CheckQuorum should make the old leader give up on its own
	time.Sleep(400 * time.Millisecond)
	if c.node(old).IsLeader() {
		t.Fatal("isolated leader did not step down")
	}
	c.net.Heal(old)
	c.propose("after-heal")
	c.converge(1)
}

func TestPreVoteStopsDisruption(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.leader()
	term := c.node(l).Status().Term
	var victim string
	for _, id := range c.ids {
		if id != l {
			victim = id
			break
		}
	}
	// a partitioned follower would keep bumping its term without pre-vote
	c.net.Isolate(victim)
	time.Sleep(time.Second)
	if vt := c.node(victim).Status().Term; vt != term {
		t.Fatalf("isolated follower bumped its term %d -> %d", term, vt)
	}
	c.net.Heal(victim)
	time.Sleep(300 * time.Millisecond)
	if l2 := c.leader(); l2 != l {
		t.Fatalf("rejoining follower disrupted leader %s -> %s", l, l2)
	}
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 5, 0)
	for i := 0; i < 50; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i))
	}
	got := c.converge(50)
	for i, cmd := range got {
		if cmd != fmt.Sprintf("cmd-%d", i) {
			t.Fatalf("entry %d = %q", i, cmd)
		}
	}
}

func TestNoProgressWithoutQuorum(t *testing.T) {
	c := newCluster(t, 5, 0)
	l := c.leader()
	c.propose("one")
	var cut []string
	for _, id := range c.ids {
		if id != l && len(cut) < 3 {
			cut = append(cut, id)
		}
	}
	for _, id := range cut {
		c.net.Isolate(id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err := c.node(l).Propose(ctx, []byte("should-not-commit"))
	cancel()
	if err == nil {
		t.Fatal("committed without a quorum")
	}
	for _, id := range cut {
		c.net.Heal(id)
	}
	c.propose("two")
	c.converge(2)
}

func TestCrashAndRecoverAll(t *testing.T) {
	c := newCluster(t, 3, 0)
	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprintf("a%d", i))
	}
	c.converge(20)
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	c.propose("after-restart")
	got := c.converge(21)
	if got[0] != "a0" || got[20] != "after-restart" {
		t.Fatalf("unexpected log after restart: %v", got)
	}
}

func TestLaggingFollowerGetsSnapshot(t *testing.T) {
	c := newCluster(t, 3, 50)
	l := c.leader()
	var lag string
	for _, id := range c.ids {
		if id != l {
			lag = id
			break
		}
	}
	c.crash(lag)
	for i := 0; i < 300; i++ {
		c.propose(fmt.Sprintf("x%d", i))
	}
	if st := c.node(c.leader()).Status(); st.SnapshotIndex == 0 {
		t.Fatal("leader never compacted its log")
	}
	c.start(lag)
	c.converge(300)
	if c.node(lag).Status().Stats.SnapshotsInstalled == 0 {
		t.Fatal("lagging follower caught up without a snapshot; threshold too high?")
	}
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.propose("w1")
	l := c.leader()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.node(l).ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.ids {
		if id == l {
			continue
		}
		var nle *raft.NotLeaderError
		if err := c.node(id).ReadIndex(ctx); err == nil {
			t.Fatal("follower served a linearizable read")
		} else if !asNotLeader(err, &nle) {
			t.Fatalf("unexpected error %v", err)
		}
	}
	// an isolated leader must refuse reads once its lease runs out
	c.net.Isolate(l)
	time.Sleep(200 * time.Millisecond)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	if err := c.node(l).ReadIndex(ctx2); err == nil {
		t.Fatal("isolated leader served a linearizable read")
	}
}

func asNotLeader(err error, out **raft.NotLeaderError) bool {
	e, ok := err.(*raft.NotLeaderError)
	if ok {
		*out = e
	}
	return ok
}

// TestChaos is a figure-8 style torture test: random crashes, restarts,
// partitions and a lossy network while clients keep proposing. At the end
// every node must hold the same log, and every acknowledged command must be
// in it exactly once.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newCluster(t, 5, 80)
	c.net.SetUnreliable(0.05, 8*time.Millisecond)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var (
		ackMu sync.Mutex
		acked = map[string]bool{}
		stop  = make(chan struct{})
		wg    sync.WaitGroup
	)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				cmd := fmt.Sprintf("w%d-%d", w, i)
				for _, id := range c.ids {
					n := c.node(id)
					if n == nil || !n.IsLeader() {
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					_, err := n.Propose(ctx, []byte(cmd))
					cancel()
					if err == nil {
						ackMu.Lock()
						acked[cmd] = true
						ackMu.Unlock()
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	down := map[string]bool{}
	for round := 0; round < 25; round++ {
		time.Sleep(time.Duration(100+rng.Intn(200)) * time.Millisecond)
		switch rng.Intn(3) {
		case 0: // crash or restart someone, keep a majority alive
			id := c.ids[rng.Intn(len(c.ids))]
			if down[id] {
				c.start(id)
				delete(down, id)
			} else if len(down) < 2 {
				c.crash(id)
				down[id] = true
			}
		case 1: // random partition
			p := rng.Perm(len(c.ids))
			k := 1 + rng.Intn(2)
			var a, b []string
			for i, j := range p {
				if i < k {
					a = append(a, c.ids[j])
				} else {
					b = append(b, c.ids[j])
				}
			}
			c.net.Partition(a, b)
		case 2:
			c.net.HealAll()
		}
	}
	close(stop)
	wg.Wait()
	c.net.HealAll()
	c.net.SetUnreliable(0, 0)
	for id := range down {
		c.start(id)
	}
	c.propose("final")
	got := c.converge(1)

	seen := map[string]int{}
	for _, cmd := range got {
		seen[cmd]++
	}
	for cmd, n := range seen {
		if n > 1 {
			t.Fatalf("%q applied %d times", cmd, n)
		}
	}
	ackMu.Lock()
	defer ackMu.Unlock()
	for cmd := range acked {
		if seen[cmd] == 0 {
			t.Fatalf("acknowledged command %q was lost", cmd)
		}
	}
	t.Logf("%d commands acknowledged, %d in final log", len(acked), len(got))
}

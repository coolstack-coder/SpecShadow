// Package harness runs an instrumented AegisDB raft cluster in one process,
// injects faults, drives client traffic and collects the trace.
package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/coolstack-coder/specshadow/subject/raft"
	"github.com/coolstack-coder/specshadow/subject/simnet"
	"github.com/coolstack-coder/specshadow/trace"
)

type Options struct {
	Nodes           int
	Seed            int64
	Bug             string
	Heartbeat       time.Duration
	ElectionTimeout time.Duration
	PauseAfterLease time.Duration
}

func (o *Options) defaults() {
	if o.Nodes == 0 {
		o.Nodes = 3
	}
	if o.Heartbeat == 0 {
		o.Heartbeat = 20 * time.Millisecond
	}
	if o.ElectionTimeout == 0 {
		o.ElectionTimeout = 120 * time.Millisecond
	}
}

type nopSM struct{}

func (nopSM) Apply(raft.Entry) any      { return nil }
func (nopSM) Snapshot() ([]byte, error) { return nil, nil }
func (nopSM) Restore([]byte) error      { return nil }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type Cluster struct {
	Opt    Options
	IDs    []string
	Net    *simnet.Network
	Rec    *trace.Recorder
	Rng    *rand.Rand
	Clocks map[string]*SimClock

	mu       sync.Mutex
	nodes    map[string]*raft.Node
	stores   map[string]*raft.MemoryStorage
	frozen   map[string]bool
	stopping bool
}

func NewCluster(opt Options) *Cluster {
	opt.defaults()
	c := &Cluster{
		Opt: opt, Net: simnet.New(opt.Seed), Rec: trace.NewRecorder(),
		Rng: rand.New(rand.NewSource(opt.Seed)), Clocks: map[string]*SimClock{},
		nodes: map[string]*raft.Node{}, stores: map[string]*raft.MemoryStorage{},
		frozen: map[string]bool{},
	}
	for i := 1; i <= opt.Nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		c.IDs = append(c.IDs, id)
		c.stores[id] = raft.NewMemoryStorage()
		c.Clocks[id] = NewSimClock()
	}
	for _, id := range c.IDs {
		c.Start(id)
	}
	return c
}

// Start boots (or reboots) a node from its storage.
func (c *Cluster) Start(id string) {
	n, err := raft.NewNode(raft.Config{
		ID: id, Group: "g", Peers: c.IDs, Storage: c.stores[id],
		Transport: c.Net.Transport(id), StateMachine: nopSM{},
		HeartbeatInterval: c.Opt.Heartbeat, ElectionTimeout: c.Opt.ElectionTimeout,
		SnapshotThreshold: 1 << 62, Logger: quiet,
		Clock: c.Clocks[id], Trace: c.Rec, Bug: c.Opt.Bug,
		PauseAfterLeaseCheck: c.Opt.PauseAfterLease,
	})
	if err != nil {
		panic(err)
	}
	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()
	c.Net.Register(id, n)
}

// Crash kills a node. Its MemoryStorage survives, like a disk.
func (c *Cluster) Crash(id string) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	if n == nil {
		return
	}
	c.Net.Unregister(id)
	n.Stop()
	c.Rec.Emit(trace.Event{Node: id, Kind: "Crash", Site: "harness"})
}

func (c *Cluster) Node(id string) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *Cluster) Up() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var up []string
	for _, id := range c.IDs {
		if c.nodes[id] != nil {
			up = append(up, id)
		}
	}
	return up
}

// Leader returns the node that claims leadership in the highest term.
func (c *Cluster) Leader() string {
	best, top := "", uint64(0)
	for _, id := range c.Up() {
		if n := c.Node(id); n != nil {
			if st := n.Status(); st.Role == "leader" && st.Term >= top {
				best, top = id, st.Term
			}
		}
	}
	return best
}

func (c *Cluster) WaitLeader(d time.Duration) string {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if l := c.Leader(); l != "" {
			return l
		}
		time.Sleep(5 * time.Millisecond)
	}
	return ""
}

// Freeze stops a node's clock and cuts it off, like a suspended VM. Its
// client stops sending too. Thaw resumes everything at once.
func (c *Cluster) Freeze(id string) {
	c.mu.Lock()
	c.frozen[id] = true
	c.mu.Unlock()
	c.Clocks[id].SetRate(0)
	c.Net.Isolate(id)
	c.Rec.Emit(trace.Event{Node: id, Kind: "Fault", Note: "freeze", Site: "harness"})
}

func (c *Cluster) Thaw(id string, rate float64) {
	c.Clocks[id].SetRate(rate)
	c.Net.Heal(id)
	c.mu.Lock()
	delete(c.frozen, id)
	c.mu.Unlock()
	c.Rec.Emit(trace.Event{Node: id, Kind: "Fault", Note: "thaw", Site: "harness"})
}

func (c *Cluster) isFrozen(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.frozen[id]
}

// Clients writes through whoever is leader and keeps reading from every node
// that believes it is leader, until ctx ends.
func (c *Cluster) Clients(ctx context.Context) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		seq := 0
		for ctx.Err() == nil {
			l := c.Leader()
			if n := c.Node(l); n != nil && !c.isFrozen(l) {
				seq++
				cctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
				n.Propose(cctx, []byte(fmt.Sprintf("w%d", seq)))
				cancel()
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	for _, id := range c.IDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for ctx.Err() == nil {
				if n := c.Node(id); n != nil && !c.isFrozen(id) && n.Status().Role == "leader" {
					cctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
					n.ReadIndex(cctx)
					cancel()
				}
				time.Sleep(3 * time.Millisecond)
			}
		}(id)
	}
	return &wg
}

// Stop shuts every node down and returns the trace.
func (c *Cluster) Stop() []trace.Event {
	for _, id := range c.IDs {
		if c.isFrozen(id) {
			c.Thaw(id, 1)
		}
	}
	c.mu.Lock()
	nodes := make([]*raft.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.nodes = map[string]*raft.Node{}
	c.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
	return c.Rec.Events()
}

// Package simnet is an in-process network for testing raft. Messages can be
// delayed, dropped, or cut by partitions, in the spirit of MIT 6.824's
// labrpc. Requests and responses are round-tripped through JSON so nodes
// never share memory.
package simnet

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/coolstack-coder/specshadow/subject/raft"
)

var ErrUnreachable = errors.New("simnet: unreachable")

// Handler is implemented by *raft.Node.
type Handler interface {
	HandleVote(*raft.VoteRequest) *raft.VoteResponse
	HandleAppend(*raft.AppendRequest) *raft.AppendResponse
	HandleSnapshot(*raft.SnapshotRequest) *raft.SnapshotResponse
}

type Network struct {
	mu       sync.Mutex
	handlers map[string]Handler
	isolated map[string]bool
	cut      map[[2]string]bool
	dropRate float64
	maxDelay time.Duration
	rng      *rand.Rand
}

func New(seed int64) *Network {
	return &Network{
		handlers: make(map[string]Handler),
		isolated: make(map[string]bool),
		cut:      make(map[[2]string]bool),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Register (or replace, after a restart) the handler for id.
func (n *Network) Register(id string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Unregister makes id unreachable, like a crashed process.
func (n *Network) Unregister(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

func (n *Network) Isolate(id string) { n.mu.Lock(); n.isolated[id] = true; n.mu.Unlock() }
func (n *Network) Heal(id string)    { n.mu.Lock(); delete(n.isolated, id); n.mu.Unlock() }

// Partition splits nodes into groups that can only talk among themselves.
func (n *Network) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = make(map[[2]string]bool)
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					n.cut[[2]string{a, b}] = true
				}
			}
		}
	}
}

// HealAll removes every partition and isolation.
func (n *Network) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = make(map[[2]string]bool)
	n.isolated = make(map[string]bool)
}

// SetUnreliable sets a drop probability and max random delay per message.
func (n *Network) SetUnreliable(drop float64, maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate, n.maxDelay = drop, maxDelay
}

func (n *Network) route(from, to string) (Handler, time.Duration, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h, ok := n.handlers[to]
	if !ok || n.isolated[from] || n.isolated[to] || n.cut[[2]string{from, to}] {
		return nil, 0, false
	}
	if _, alive := n.handlers[from]; !alive {
		return nil, 0, false
	}
	if n.dropRate > 0 && n.rng.Float64() < n.dropRate {
		return nil, 0, false
	}
	var d time.Duration
	if n.maxDelay > 0 {
		d = time.Duration(n.rng.Int63n(int64(n.maxDelay)))
	}
	return h, d, true
}

func wait(ctx context.Context, d time.Duration) error {
	if d == 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func clone[T any](v *T) *T {
	b, _ := json.Marshal(v)
	out := new(T)
	json.Unmarshal(b, out)
	return out
}

func call[Req, Resp any](ctx context.Context, n *Network, from, to string, req *Req, fn func(Handler, *Req) *Resp) (*Resp, error) {
	h, d, ok := n.route(from, to)
	if !ok {
		// pretend the packet vanished: the caller notices via timeout, but
		// we don't make it wait the full timeout to keep tests fast
		wait(ctx, 5*time.Millisecond)
		return nil, ErrUnreachable
	}
	if err := wait(ctx, d); err != nil {
		return nil, err
	}
	resp := fn(h, clone(req))
	// the reply can get lost too
	if _, d2, ok := n.route(to, from); !ok {
		return nil, ErrUnreachable
	} else if err := wait(ctx, d2); err != nil {
		return nil, err
	}
	return clone(resp), nil
}

type transport struct {
	net  *Network
	from string
}

// Transport returns the raft.Transport used by node `from`.
func (n *Network) Transport(from string) raft.Transport { return &transport{net: n, from: from} }

func (t *transport) SendVote(ctx context.Context, to string, req *raft.VoteRequest) (*raft.VoteResponse, error) {
	return call(ctx, t.net, t.from, to, req, func(h Handler, r *raft.VoteRequest) *raft.VoteResponse { return h.HandleVote(r) })
}

func (t *transport) SendAppend(ctx context.Context, to string, req *raft.AppendRequest) (*raft.AppendResponse, error) {
	return call(ctx, t.net, t.from, to, req, func(h Handler, r *raft.AppendRequest) *raft.AppendResponse { return h.HandleAppend(r) })
}

func (t *transport) SendSnapshot(ctx context.Context, to string, req *raft.SnapshotRequest) (*raft.SnapshotResponse, error) {
	return call(ctx, t.net, t.from, to, req, func(h Handler, r *raft.SnapshotRequest) *raft.SnapshotResponse { return h.HandleSnapshot(r) })
}

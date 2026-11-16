package harness

import (
	"context"
	"fmt"
	"time"

	"github.com/coolstack-coder/specshadow/trace"
)

// Scenario names accepted by Run.
const (
	Chaos     = "chaos"      // partitions, crashes, restarts, lossy network
	LeaseRate = "lease-rate" // leader's clock runs at Rate while it gets partitioned
	Freeze    = "freeze"     // leader's VM is suspended (clock and process stop) then resumed
	Pause     = "pause"      // every read stalls right after its lease check
)

type RunConfig struct {
	Options
	Scenario string
	Duration time.Duration
	Rate     float64 // LeaseRate: leader clock speed
}

// Run executes one scenario and returns its trace.
func Run(rc RunConfig) ([]trace.Event, error) {
	if rc.Duration == 0 {
		rc.Duration = 2 * time.Second
	}
	if rc.Scenario == Pause && rc.PauseAfterLease == 0 {
		rc.PauseAfterLease = 3 * rc.electionTimeout()
	}
	c := NewCluster(rc.Options)
	if c.WaitLeader(3*time.Second) == "" {
		c.Stop()
		return nil, fmt.Errorf("no leader elected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), rc.Duration)
	clients := c.Clients(ctx)

	switch rc.Scenario {
	case Chaos:
		chaos(ctx, c)
	case LeaseRate, Pause:
		rate := rc.Rate
		if rc.Scenario == Pause {
			rate = 1
		}
		isolateLeaderLoop(ctx, c, rate)
	case Freeze:
		freezeLoop(ctx, c)
	default:
		cancel()
		c.Stop()
		return nil, fmt.Errorf("unknown scenario %q", rc.Scenario)
	}
	<-ctx.Done()
	cancel()
	clients.Wait()
	c.Net.HealAll()
	return c.Stop(), nil
}

func (rc RunConfig) electionTimeout() time.Duration {
	if rc.ElectionTimeout == 0 {
		return 120 * time.Millisecond
	}
	return rc.ElectionTimeout
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func (c *Cluster) note(s string) {
	c.Rec.Emit(trace.Event{Node: "-", Kind: "Fault", Note: s, Site: "harness"})
}

// chaos keeps a majority alive but otherwise does whatever the dice say.
func chaos(ctx context.Context, c *Cluster) {
	go func() {
		quorum := len(c.IDs)/2 + 1
		for {
			if !sleep(ctx, time.Duration(30+c.Rng.Intn(120))*time.Millisecond) {
				return
			}
			switch c.Rng.Intn(8) {
			case 0: // split the cluster
				perm := c.Rng.Perm(len(c.IDs))
				k := 1 + c.Rng.Intn(len(c.IDs)-1)
				var a, b []string
				for i, p := range perm {
					if i < k {
						a = append(a, c.IDs[p])
					} else {
						b = append(b, c.IDs[p])
					}
				}
				c.Net.Partition(a, b)
				c.note(fmt.Sprintf("partition %v | %v", a, b))
			case 1:
				if l := c.Leader(); l != "" {
					c.Net.Isolate(l)
					c.note("isolate leader " + l)
				}
			case 2:
				c.Net.HealAll()
				c.note("heal")
			case 3, 6:
				if up := c.Up(); len(up) > quorum {
					id := up[c.Rng.Intn(len(up))]
					if c.Rng.Intn(2) == 0 {
						if l := c.Leader(); l != "" {
							id = l
						}
					}
					c.Crash(id)
				}
			case 4, 7:
				for _, id := range c.IDs {
					if c.Node(id) == nil {
						c.Start(id)
						break
					}
				}
			case 5:
				drop := float64(c.Rng.Intn(20)) / 100
				delay := time.Duration(c.Rng.Intn(30)) * time.Millisecond
				if c.Rng.Intn(3) == 0 {
					delay = time.Duration(c.Rng.Intn(250)) * time.Millisecond // a latency spike
				}
				c.Net.SetUnreliable(drop, delay)
				c.note(fmt.Sprintf("unreliable drop=%.2f max_delay=%v", drop, delay))
			}
		}
	}()
}

// isolateLeaderLoop repeatedly cuts the current leader off from everyone
// while its clock runs at rate, then heals.
func isolateLeaderLoop(ctx context.Context, c *Cluster, rate float64) {
	go func() {
		et := c.Opt.ElectionTimeout
		for {
			if !sleep(ctx, 4*et) {
				return
			}
			l := c.Leader()
			if l == "" {
				continue
			}
			c.Clocks[l].SetRate(rate)
			c.Net.Isolate(l)
			c.note(fmt.Sprintf("isolate %s, clock rate %.2f", l, rate))
			if !sleep(ctx, 6*et) {
				return
			}
			c.Net.HealAll()
			c.Clocks[l].SetRate(1)
			c.note("heal")
		}
	}()
}

// freezeLoop suspends the leader like a paused VM, then resumes it.
func freezeLoop(ctx context.Context, c *Cluster) {
	go func() {
		et := c.Opt.ElectionTimeout
		for {
			if !sleep(ctx, 4*et) {
				return
			}
			l := c.Leader()
			if l == "" {
				continue
			}
			c.Freeze(l)
			if !sleep(ctx, 4*et) {
				c.Thaw(l, 1)
				return
			}
			c.Thaw(l, 1)
		}
	}()
}

package harness

import (
	"sync"
	"time"
)

// SimClock is a node-local clock that can run fast, slow or stop. It returns
// times built from one real starting point, so Go still compares them with
// the monotonic reading, exactly like time.Now() values.
type SimClock struct {
	mu       sync.Mutex
	base     time.Time
	lastReal time.Time
	virt     time.Duration
	rate     float64
}

func NewSimClock() *SimClock {
	now := time.Now()
	return &SimClock{base: now, lastReal: now, rate: 1}
}

func (c *SimClock) advanceLocked() {
	now := time.Now()
	c.virt += time.Duration(float64(now.Sub(c.lastReal)) * c.rate)
	c.lastReal = now
}

func (c *SimClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advanceLocked()
	return c.base.Add(c.virt)
}

// SetRate changes how fast this clock runs relative to real time.
// 1 is normal, 0.5 is half speed, 0 is stopped.
func (c *SimClock) SetRate(r float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advanceLocked()
	c.rate = r
}

func (c *SimClock) Rate() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rate
}

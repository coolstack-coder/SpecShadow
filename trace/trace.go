// Package trace records what a running raft cluster actually did, one event
// per state change, in a single global order.
//
// Every node emits its events while holding its own mutex, and the recorder
// serialises them under one more lock. Because a reply is always handled
// after the request that caused it, this order is consistent with causality,
// which is what lets the checker replay the trace step by step against the
// protocol's rules.
package trace

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// State is a node's protocol state right after the event.
type State struct {
	Term   uint64   `json:"term"`
	Role   string   `json:"role"` // F follower, P pre-candidate, C candidate, L leader
	Vote   string   `json:"vote"`
	Log    []uint64 `json:"log"` // term of every entry, index 1..last
	Commit uint64   `json:"commit"`
}

// Event is one atomic step of one node (or of a client).
type Event struct {
	Seq  int    `json:"seq"`
	TUs  int64  `json:"t_us"` // real time since the recorder started
	Node string `json:"node"`
	Kind string `json:"kind"`
	Site string `json:"site,omitempty"` // where in the implementation it happened

	Peer     string   `json:"peer,omitempty"`  // candidate (Vote) or leader (AppendOK)
	MTerm    uint64   `json:"mterm,omitempty"` // term carried by the message
	Granted  bool     `json:"granted,omitempty"`
	Prev     uint64   `json:"prev,omitempty"` // AppendOK: prevLogIndex
	N        int      `json:"n,omitempty"`    // AppendOK: number of entries
	LastIdx  uint64   `json:"last_idx,omitempty"`
	LastTerm uint64   `json:"last_term,omitempty"`
	Lo       uint64   `json:"lo,omitempty"` // Apply: first index applied
	Hi       uint64   `json:"hi,omitempty"` // Apply: last index applied
	Terms    []uint64 `json:"terms,omitempty"`
	RID      int      `json:"rid,omitempty"` // read id
	Idx      uint64   `json:"idx,omitempty"` // ReadServe: read index, WriteAck: entry index
	Lease    bool     `json:"lease,omitempty"`
	Note     string   `json:"note,omitempty"`

	State *State `json:"state,omitempty"`
}

// Sink receives events from instrumented nodes.
type Sink interface {
	Emit(e Event)
	NextReadID() int
}

// Recorder keeps every event in memory in emission order.
type Recorder struct {
	mu     sync.Mutex
	start  time.Time
	events []Event
	reads  int
}

func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

func (r *Recorder) Emit(e Event) {
	r.mu.Lock()
	e.Seq = len(r.events) + 1
	e.TUs = time.Since(r.start).Microseconds()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *Recorder) NextReadID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads++
	return r.reads
}

func (r *Recorder) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Event(nil), r.events...)
}

// Write stores events as newline-delimited JSON.
func Write(path string, evs []Event) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, e := range evs {
		if err := enc.Encode(e); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Read loads a trace written by Write (optionally gzipped, *.gz).
func Read(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var r io.Reader = bufio.NewReader(f)
	if strings.HasSuffix(path, ".gz") {
		zr, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		r = zr
	}
	var evs []Event
	dec := json.NewDecoder(r)
	for dec.More() {
		var e Event
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("event %d: %w", len(evs)+1, err)
		}
		evs = append(evs, e)
	}
	return evs, nil
}

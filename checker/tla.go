package checker

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/coolstack-coder/specshadow/trace"
)

// Steps are the events the spec replays, in order. Pure annotations
// (fault notes from the harness) are dropped; Steps[k] maps back to the
// original event.
func Steps(evs []trace.Event) []trace.Event {
	out := make([]trace.Event, 0, len(evs))
	for _, e := range evs {
		if e.Kind == "Fault" || e.Kind == "BugFired" {
			continue
		}
		out = append(out, e)
	}
	return out
}

// WriteTraceModule writes the TraceData module: the node set and the trace
// as a TLA+ sequence of records. Logs are delta-encoded per node (keep the
// first lk entries, then append la) so the module stays small.
func WriteTraceModule(w io.Writer, steps []trace.Event) error {
	bw := bufio.NewWriter(w)
	nodes := map[string]bool{}
	for _, e := range steps {
		nodes[e.Node] = true
	}
	ids := make([]string, 0, len(nodes))
	for n := range nodes {
		ids = append(ids, strconv.Quote(n))
	}
	sort.Strings(ids)

	fmt.Fprintln(bw, "---- MODULE TraceData ----")
	fmt.Fprintln(bw, "EXTENDS Sequences")
	fmt.Fprintf(bw, "Nodes == {%s}\n", strings.Join(ids, ", "))
	fmt.Fprintln(bw, "Trace == <<")

	prev := map[string][]uint64{}     // last log the spec knows for each node
	last := map[string]*trace.State{} // last full state, for stateless events
	for k, e := range steps {
		st := e.State
		if st == nil {
			st = last[e.Node]
			if st == nil {
				st = &trace.State{Role: "F"}
			}
		} else {
			last[e.Node] = st
		}
		old := prev[e.Node]
		keep := 0
		for keep < len(old) && keep < len(st.Log) && old[keep] == st.Log[keep] {
			keep++
		}
		add := st.Log[keep:]
		if e.State != nil {
			prev[e.Node] = st.Log
		} else {
			keep, add = len(old), nil
		}
		sep := ","
		if k == len(steps)-1 {
			sep = ""
		}
		fmt.Fprintf(bw,
			"[kind |-> %q, node |-> %q, peer |-> %q, mterm |-> %d, prev |-> %d, n |-> %d, "+
				"last_idx |-> %d, last_term |-> %d, lo |-> %d, hi |-> %d, terms |-> %s, "+
				"rid |-> %d, idx |-> %d, term |-> %d, role |-> %q, vote |-> %q, commit |-> %d, "+
				"lk |-> %d, la |-> %s]%s\n",
			e.Kind, e.Node, e.Peer, e.MTerm, e.Prev, e.N, e.LastIdx, e.LastTerm, e.Lo, e.Hi,
			seq(e.Terms), e.RID, e.Idx, st.Term, st.Role, st.Vote, st.Commit, keep, seq(add), sep)
	}
	fmt.Fprintln(bw, ">>")
	fmt.Fprintln(bw, "====")
	return bw.Flush()
}

func seq(v []uint64) string {
	if len(v) == 0 {
		return "<<>>"
	}
	var b strings.Builder
	b.WriteString("<<")
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatUint(x, 10))
	}
	b.WriteString(">>")
	return b.String()
}

const cfg = `SPECIFICATION Spec
CHECK_DEADLOCK TRUE
`

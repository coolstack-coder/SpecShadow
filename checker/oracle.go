package checker

import "github.com/coolstack-coder/specshadow/trace"

// ReadStats counts linearizable and stale reads in a trace, using the same
// definition as the spec's StaleRead rule: a read is stale if it reads below
// a write that was acknowledged before the read started. TLC stops at the
// first violation; this counts all of them, for the clock-rate sweep.
type ReadStats struct {
	Reads     int `json:"reads"`
	Stale     int `json:"stale"`
	MaxBehind int `json:"max_behind"` // most acknowledged writes a single read missed
	// Overlapped reads returned older data than writes already acknowledged
	// when they returned, but those writes were acknowledged after the read
	// started, so the read is still linearizable.
	Overlapped int `json:"overlapped"`
}

func CountReads(evs []trace.Event) ReadStats {
	var st ReadStats
	acked := uint64(0)
	start := map[int]uint64{}
	for _, e := range evs {
		switch e.Kind {
		case "WriteAck":
			if e.Idx > acked {
				acked = e.Idx
			}
		case "ReadStart":
			start[e.RID] = acked
		case "ReadServe":
			st.Reads++
			if a, ok := start[e.RID]; ok && e.Idx < a {
				st.Stale++
				if d := int(a - e.Idx); d > st.MaxBehind {
					st.MaxBehind = d
				}
			} else if e.Idx < acked {
				st.Overlapped++
			}
		}
	}
	return st
}

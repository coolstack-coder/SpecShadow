// Package checker turns a recorded trace into a TLA+ module and asks TLC
// whether that execution obeys the spec in spec/TraceRaft.tla.
package checker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/coolstack-coder/specshadow/spec"
	"github.com/coolstack-coder/specshadow/trace"
)

type Options struct {
	Jar     string        // tla2tools.jar
	Java    string        // java binary, default "java" from PATH or $JAVA_HOME
	KeepDir string        // keep generated files here instead of a temp dir
	Timeout time.Duration // default 10 minutes
}

// DefaultJar looks for tla2tools.jar in $SPECSHADOW_TLC, then ./tools.
func DefaultJar() string {
	if p := os.Getenv("SPECSHADOW_TLC"); p != "" {
		return p
	}
	return filepath.Join("tools", "tla2tools.jar")
}

type Result struct {
	OK       bool
	Steps    int    // events replayed by the spec
	FailStep int    // 1-based step that broke a rule (0 if OK)
	Rule     string // the rule that broke
	States   int    // distinct states TLC generated
	Elapsed  time.Duration
	Output   string // raw TLC output (truncated)
}

var (
	reViolation = regexp.MustCompile(`<<"VIOLATION", (\d+), "([^"]*)">>`)
	reStates    = regexp.MustCompile(`(\d+) distinct states found`)
	reDeadlock  = regexp.MustCompile(`/\\ i = (\d+)`)
)

// Check validates one trace.
func Check(evs []trace.Event, opt Options) (*Result, error) {
	steps := Steps(evs)
	dir := opt.KeepDir
	if dir == "" {
		d, err := os.MkdirTemp("", "specshadow-*")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(d)
		dir = d
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "TraceRaft.tla"), []byte(spec.TraceRaft), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "TraceRaft.cfg"), []byte(cfg), 0o644); err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join(dir, "TraceData.tla"))
	if err != nil {
		return nil, err
	}
	if err := WriteTraceModule(f, steps); err != nil {
		f.Close()
		return nil, err
	}
	f.Close()

	jar, err := filepath.Abs(opt.Jar)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(jar); err != nil {
		return nil, fmt.Errorf("tla2tools.jar not found at %s (set -tlc or SPECSHADOW_TLC)", jar)
	}
	java := opt.Java
	if java == "" {
		java = "java"
		if h := os.Getenv("JAVA_HOME"); h != "" {
			java = filepath.Join(h, "bin", "java")
		}
	}
	if opt.Timeout == 0 {
		opt.Timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), opt.Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, java, "-XX:+UseParallelGC", "-Xss64m", "-cp", jar, "tlc2.TLC",
		"-config", "TraceRaft.cfg", "-workers", "1", "-cleanup",
		"-metadir", filepath.Join(dir, "states"), "TraceRaft")
	cmd.Dir = dir
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	start := time.Now()
	runErr := cmd.Run()
	res := &Result{Steps: len(steps), Elapsed: time.Since(start)}
	text := out.String()
	res.Output = truncate(text, 200_000)
	// TLC prints progress lines during long runs; the last count is final
	if m := reStates.FindAllStringSubmatch(text, -1); len(m) > 0 {
		res.States, _ = strconv.Atoi(m[len(m)-1][1])
	}

	switch {
	case reViolation.MatchString(text):
		m := reViolation.FindStringSubmatch(text)
		res.FailStep, _ = strconv.Atoi(m[1])
		res.Rule = m[2]
	case strings.Contains(text, "Deadlock reached"):
		res.Rule = "UnrecognisedEvent"
		if s := reDeadlock.FindAllStringSubmatch(text, -1); len(s) > 0 {
			res.FailStep, _ = strconv.Atoi(s[len(s)-1][1])
		}
	case strings.Contains(text, "Model checking completed. No error has been found."):
		res.OK = true
		if res.States != len(steps)+1 {
			return res, fmt.Errorf("TLC replayed %d states for %d steps", res.States, len(steps))
		}
	default:
		if runErr != nil {
			return res, fmt.Errorf("TLC failed: %v\n%s", runErr, truncate(text, 4000))
		}
		return res, fmt.Errorf("unexpected TLC output:\n%s", truncate(text, 4000))
	}
	return res, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n/2] + "\n...\n" + s[len(s)-n/2:]
}

// Report explains the result in terms of the original events.
func (r *Result) Report(evs []trace.Event) string {
	var b strings.Builder
	steps := Steps(evs)
	if r.OK {
		fmt.Fprintf(&b, "OK: all %d steps obey the spec (%d states, TLC %.1fs)\n",
			r.Steps, r.States, r.Elapsed.Seconds())
		return b.String()
	}
	fmt.Fprintf(&b, "VIOLATION: %s at step %d of %d (TLC %.1fs)\n", r.Rule, r.FailStep, r.Steps, r.Elapsed.Seconds())
	if r.FailStep < 1 || r.FailStep > len(steps) {
		return b.String()
	}
	bad := steps[r.FailStep-1]
	fmt.Fprintf(&b, "  event #%d at %.3fs: %s on %s  (%s)\n", bad.Seq, float64(bad.TUs)/1e6, bad.Kind, bad.Node, bad.Site)
	fmt.Fprintln(&b, "  last events before it:")
	from := r.FailStep - 8
	if from < 1 {
		from = 1
	}
	for k := from; k <= r.FailStep; k++ {
		e := steps[k-1]
		mark := "   "
		if k == r.FailStep {
			mark = ">>>"
		}
		fmt.Fprintf(&b, "  %s %5d %-3s %-15s %s\n", mark, k, e.Node, e.Kind, Describe(e))
	}
	return b.String()
}

// Describe is a one-line summary of an event.
func Describe(e trace.Event) string {
	var parts []string
	if s := e.State; s != nil {
		parts = append(parts, fmt.Sprintf("term=%d role=%s vote=%q len=%d commit=%d", s.Term, s.Role, s.Vote, len(s.Log), s.Commit))
	}
	switch e.Kind {
	case "Vote":
		parts = append(parts, fmt.Sprintf("for=%s mterm=%d cand_last=(%d,t%d)", e.Peer, e.MTerm, e.LastIdx, e.LastTerm))
	case "AppendOK":
		parts = append(parts, fmt.Sprintf("from=%s mterm=%d prev=%d n=%d", e.Peer, e.MTerm, e.Prev, e.N))
	case "LeaderCommit", "WriteAck":
		parts = append(parts, fmt.Sprintf("idx=%d", e.Idx))
	case "Apply":
		parts = append(parts, fmt.Sprintf("lo=%d hi=%d", e.Lo, e.Hi))
	case "ReadStart":
		parts = append(parts, fmt.Sprintf("rid=%d", e.RID))
	case "ReadServe":
		parts = append(parts, fmt.Sprintf("rid=%d read_idx=%d lease=%v", e.RID, e.Idx, e.Lease))
	}
	return strings.Join(parts, " ")
}

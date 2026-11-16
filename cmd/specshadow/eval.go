package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coolstack-coder/specshadow/checker"
	"github.com/coolstack-coder/specshadow/harness"
	"github.com/coolstack-coder/specshadow/subject/raft"
	"github.com/coolstack-coder/specshadow/trace"
)

// RunResult is one recorded and checked execution.
type RunResult struct {
	Group     string  `json:"group"`
	Scenario  string  `json:"scenario"`
	Bug       string  `json:"bug,omitempty"`
	Rate      float64 `json:"rate"`
	Seed      int64   `json:"seed"`
	Events    int     `json:"events"`
	Steps     int     `json:"steps"`
	OK        bool    `json:"ok"`
	Rule      string  `json:"rule,omitempty"`
	FailStep  int     `json:"fail_step,omitempty"`
	FailAtMs  float64 `json:"fail_at_ms,omitempty"` // real time of the violating event
	TLCSec    float64 `json:"tlc_sec"`
	Elections int     `json:"elections"`
	Crashes   int     `json:"crashes"`
	BugFired  int     `json:"bug_fired"` // times the planted bug changed behaviour
	checker.ReadStats
	Error string `json:"error,omitempty"`
	Trace string `json:"trace,omitempty"`
}

// SuiteResult is AegisDB's own raft test suite run against one planted bug.
type SuiteResult struct {
	Bug    string   `json:"bug"`
	Passed bool     `json:"passed"`
	Failed []string `json:"failed_tests,omitempty"`
	Sec    float64  `json:"sec"`
}

type Results struct {
	Started  string        `json:"started"`
	Finished string        `json:"finished"`
	Machine  string        `json:"machine"`
	Runs     []RunResult   `json:"runs"`
	Suite    []SuiteResult `json:"aegis_suite"`
}

type job struct {
	group, scenario, bug string
	rate                 float64
	seed                 int64
}

func cmdEval(args []string) error {
	fs := flag.NewFlagSet("eval", flag.ExitOnError)
	out := fs.String("out", "experiments/results.json", "results file")
	runs := fs.Int("runs", 20, "seeds per bug / correct configuration")
	sweep := fs.Int("sweep", 5, "seeds per clock rate")
	par := fs.Int("par", 3, "runs in parallel")
	jar := fs.String("tlc", checker.DefaultJar(), "tla2tools.jar")
	keep := fs.String("keep", "experiments/traces", "where to keep violating traces")
	skipSuite := fs.Bool("skip-suite", false, "don't run AegisDB's own tests per bug")
	only := fs.String("only", "", "comma-separated groups: correct,bugs,sweep,freeze,pause")
	fs.Parse(args)

	want := func(g string) bool { return *only == "" || strings.Contains(","+*only+",", ","+g+",") }
	var jobs []job
	if want("correct") {
		for s := 1; s <= *runs; s++ {
			jobs = append(jobs, job{"correct", harness.Chaos, "", 1, int64(s)})
		}
	}
	if want("bugs") {
		for _, b := range raft.Bugs {
			for s := 1; s <= *runs; s++ {
				jobs = append(jobs, job{"bugs", harness.Chaos, b, 1, int64(100 + s)})
			}
		}
	}
	if want("sweep") {
		for _, r := range []float64{1, 0.9, 0.8, 0.75, 0.7, 0.65, 0.6, 0.5, 0.4, 0.3} {
			for s := 1; s <= *sweep; s++ {
				jobs = append(jobs, job{"sweep", harness.LeaseRate, "", r, int64(200 + s)})
			}
		}
	}
	if want("freeze") {
		for s := 1; s <= *runs/2; s++ {
			jobs = append(jobs, job{"freeze", harness.Freeze, "", 1, int64(300 + s)})
		}
	}
	if want("pause") {
		for s := 1; s <= *runs/2; s++ {
			jobs = append(jobs, job{"pause", harness.Pause, "", 1, int64(400 + s)})
		}
	}

	res := &Results{Started: time.Now().Format(time.RFC3339), Machine: machine()}
	if !*skipSuite && want("bugs") {
		for _, b := range append([]string{""}, raft.Bugs...) {
			sr := runSuite(b)
			fmt.Printf("aegis suite, bug=%-17q passed=%v %v\n", b, sr.Passed, sr.Failed)
			res.Suite = append(res.Suite, sr)
		}
	}

	os.MkdirAll(*keep, 0o755)
	var mu sync.Mutex
	ch := make(chan job)
	var wg sync.WaitGroup
	done := 0
	for w := 0; w < *par; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range ch {
				r := runOne(j, *jar, *keep)
				mu.Lock()
				res.Runs = append(res.Runs, r)
				done++
				verdict := "ok"
				if r.Error != "" {
					verdict = "ERROR " + r.Error
				} else if !r.OK {
					verdict = r.Rule
				}
				fmt.Printf("[%3d/%d] %-7s %-10s bug=%-16s rate=%.2f seed=%-3d steps=%-6d stale=%-4d fired=%-3d %s\n",
					done, len(jobs), r.Group, r.Scenario, r.Bug, r.Rate, r.Seed, r.Steps, r.Stale, r.BugFired, verdict)
				mu.Unlock()
			}
		}()
	}
	for _, j := range jobs {
		ch <- j
	}
	close(ch)
	wg.Wait()

	sort.Slice(res.Runs, func(a, b int) bool {
		x, y := res.Runs[a], res.Runs[b]
		if x.Group != y.Group {
			return x.Group < y.Group
		}
		if x.Bug != y.Bug {
			return x.Bug < y.Bug
		}
		if x.Rate != y.Rate {
			return x.Rate > y.Rate
		}
		return x.Seed < y.Seed
	})
	res.Finished = time.Now().Format(time.RFC3339)
	os.MkdirAll(filepath.Dir(*out), 0o755)
	b, _ := json.MarshalIndent(res, "", " ")
	return os.WriteFile(*out, b, 0o644)
}

func runOne(j job, jar, keep string) RunResult {
	r := RunResult{Group: j.group, Scenario: j.scenario, Bug: j.bug, Rate: j.rate, Seed: j.seed}
	evs, err := harness.Run(harness.RunConfig{
		Options:  harness.Options{Seed: j.seed, Bug: j.bug},
		Scenario: j.scenario, Duration: 3 * time.Second, Rate: j.rate,
	})
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.Events = len(evs)
	r.ReadStats = checker.CountReads(evs)
	for _, e := range evs {
		switch e.Kind {
		case "BecomeLeader":
			r.Elections++
		case "Crash":
			r.Crashes++
		case "BugFired":
			r.BugFired++
		}
	}
	cr, err := checker.Check(evs, checker.Options{Jar: jar})
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.Steps, r.OK, r.Rule, r.FailStep, r.TLCSec = cr.Steps, cr.OK, cr.Rule, cr.FailStep, cr.Elapsed.Seconds()
	if !cr.OK {
		steps := checker.Steps(evs)
		if cr.FailStep >= 1 && cr.FailStep <= len(steps) {
			r.FailAtMs = float64(steps[cr.FailStep-1].TUs) / 1000
		}
		name := fmt.Sprintf("%s-%s-%s-r%.2f-s%d.ndjson", j.group, j.scenario, orNone(j.bug), j.rate, j.seed)
		r.Trace = filepath.Join(keep, name)
		trace.Write(r.Trace, evs)
	}
	return r
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// runSuite runs AegisDB's original raft tests with a planted bug switched on.
func runSuite(bug string) SuiteResult {
	start := time.Now()
	cmd := exec.Command("go", "test", "./subject/raft/", "-count=1", "-v", "-timeout", "180s")
	cmd.Env = append(os.Environ(), "SPECSHADOW_BUG="+bug)
	out, err := cmd.CombinedOutput()
	sr := SuiteResult{Bug: bug, Passed: err == nil, Sec: time.Since(start).Seconds()}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--- FAIL:") {
			if f := strings.Fields(strings.TrimSpace(line)); len(f) >= 3 {
				sr.Failed = append(sr.Failed, f[2])
			}
		}
	}
	return sr
}

func machine() string {
	out, _ := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
	return strings.TrimSpace(string(out))
}

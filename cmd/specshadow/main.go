// Command specshadow records real executions of AegisDB's raft and checks
// each one against a TLA+ specification of the protocol's safety rules.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/coolstack-coder/specshadow/checker"
	"github.com/coolstack-coder/specshadow/harness"
	"github.com/coolstack-coder/specshadow/trace"
)

const usage = `specshadow: check real raft executions against a TLA+ spec

  specshadow run   -scenario chaos -seed 1 [-bug NAME] [-rate 0.5] -out t.ndjson
  specshadow check -trace t.ndjson [-tlc path/to/tla2tools.jar]
  specshadow eval  -out experiments/results.json [-runs 20]

scenarios: chaos, lease-rate, freeze, pause
bugs:      double-vote, no-up-to-date, commit-old-term, commit-overrun,
           long-lease, stale-term, vote-not-synced
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(os.Args[2:])
	case "check":
		err = cmdCheck(os.Args[2:])
	case "eval":
		err = cmdEval(os.Args[2:])
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	sc := fs.String("scenario", harness.Chaos, "scenario")
	seed := fs.Int64("seed", 1, "random seed")
	bug := fs.String("bug", "", "planted bug")
	rate := fs.Float64("rate", 1, "leader clock rate (lease-rate)")
	dur := fs.Duration("dur", 2*time.Second, "run length")
	out := fs.String("out", "trace.ndjson", "output trace")
	fs.Parse(args)
	evs, err := harness.Run(harness.RunConfig{
		Options:  harness.Options{Seed: *seed, Bug: *bug},
		Scenario: *sc, Duration: *dur, Rate: *rate,
	})
	if err != nil {
		return err
	}
	if err := trace.Write(*out, evs); err != nil {
		return err
	}
	fmt.Printf("%d events -> %s\n", len(evs), *out)
	return nil
}

func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	path := fs.String("trace", "trace.ndjson", "trace to check")
	jar := fs.String("tlc", checker.DefaultJar(), "tla2tools.jar")
	keep := fs.String("keep", "", "directory to keep the generated TLA+ files in")
	fs.Parse(args)
	evs, err := trace.Read(*path)
	if err != nil {
		return err
	}
	res, err := checker.Check(evs, checker.Options{Jar: *jar, KeepDir: *keep})
	if err != nil {
		return err
	}
	fmt.Print(res.Report(evs))
	if !res.OK {
		os.Exit(3)
	}
	return nil
}

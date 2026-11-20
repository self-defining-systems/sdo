// Command lanes (formerly fuzzops-bench) runs fuzzops experiments from spec files on lanes
// (one kind cluster each) across hosts, and keeps every result under one
// root directory (docs/bench.md).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/bench"
)

const usage = `usage: lanes VERB --root R ...
  serve                      run the manager (reconciles every 10 s; steps run on without it)
  add FILE                   validate a spec and add it as an experiment
  remove NAME [--teardown]   drop its queued runs; runs in flight finish; results stay; its lanes return free with
                             their app copy (keep_lanes: clusters kept); --teardown deletes them like an experiment without keep_lanes
  pause NAME | resume NAME   stop / restart new runs of an experiment
  status [NAME]              experiments, lanes and hosts
  results NAME [--with BASELINE-EXP ...]   grade the done runs into <root>/NAME/results
  lane reset LANE            return a down lane to free after a human looked
  gc                         on the manager's next pass: delete the lane clusters no experiment needs and the
                             host staging nothing uses (results are never removed)
  preflight [--spec FILE|DIR ...] [--max-load N] [--allow-memory-diag S ...]   checks before any lane is built: credentials, sink, proto,
                             images, kind node image, memory, capacity, pack; PASS/FAIL per check per host
  step STEP.json             run one step (the manager starts it detached on the lane's host)
  ledger [--state DIR] show | prune | adopt LANE CPUS MEMORY   the host's lane ledger (cores and memory each lane holds): list it,
                             drop lanes whose cluster is gone, or register a lane that exists (run on the host)
  host reserve HOST|all | unreserve HOST [--drain] | status [HOST] | adopt HOST [--cpus LIST] | install-helper HOST
                             the hosts.yaml reserve: block: apply it through the root helper, undo it, show it, record an existing one
  probe --staging S [--fuzzops F,..] LANE=STEP ...   how a host's steps stand, as JSON (the manager runs it over ssh)`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	os.Exit(run(os.Args[1], os.Args[2:]))
}

func run(verb string, args []string) int {
	fs := flag.NewFlagSet(verb, flag.ContinueOnError)
	root := fs.String("root", os.Getenv("FUZZOPS_BENCH_ROOT"), "results root (default $FUZZOPS_BENCH_ROOT)")
	var with multi
	var specs multi
	maxLoad := fs.Float64("max-load", 1.0, "preflight: highest accepted load5 per core")
	var allow multi
	if verb == "preflight" {
		fs.Var(&allow, "allow-memory-diag", "preflight: a memory-check diagnostic substring that does not fail the check (repeatable)")
		fs.Var(&specs, "spec", "preflight: a spec file or a directory of specs (repeatable; default <root>/specs)")
	}
	if verb == "results" {
		fs.Var(&with, "with", "a baseline experiment whose noop runs grade this one (repeatable)")
	}
	teardown := false
	if verb == "remove" {
		fs.BoolVar(&teardown, "teardown", false, "remove: delete the lanes' clusters even when the spec keeps its lanes")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if verb == "probe" {
		return probe(args)
	}
	if verb == "host" {
		inv, err := bench.LoadInventory(*root)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		run := bench.HostRunner(inv)
		return bench.HostCommand(ctx, bench.Root(*root), args, os.Stdout, os.Stderr, bench.HelperVia(run), run)
	}
	if verb == "ledger" {
		return bench.LedgerCommand(args, os.Stdout, os.Stderr)
	}
	if verb == "gc-staging" {
		fs := flag.NewFlagSet(verb, flag.ContinueOnError)
		staging := fs.String("staging", "", "the host's staging directory")
		if err := fs.Parse(args); err != nil || *staging == "" {
			return 2
		}
		b, _ := json.Marshal(bench.GCStaging(*staging, fs.Args()))
		fmt.Println(string(b))
		return 0
	}
	if verb == "step" {
		// A step ignores SIGINT/SIGTERM/SIGHUP from a terminal or ssh going away: it runs to its end.
		signal.Ignore(syscall.SIGHUP)
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, usage)
			return 2
		}
		if err := bench.RunStep(context.Background(), args[0]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return 2
	}
	if *root == "" {
		fmt.Fprintln(os.Stderr, "--root (or $FUZZOPS_BENCH_ROOT) is required")
		return 2
	}
	abs, err := filepath.Abs(*root) // steps and fuzzops run in other directories
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	r := bench.Root(abs)
	need := func(n int) bool {
		if len(pos) != n {
			fmt.Fprintln(os.Stderr, usage)
			return false
		}
		return true
	}
	switch verb {
	case "serve":
		err = (&bench.Manager{Root: r, Log: os.Stderr, RootPointer: bench.PointerPath(os.Getenv)}).Serve(ctx)
	case "gc":
		err = bench.GC(r)
	case "add":
		if !need(1) {
			return 2
		}
		err = bench.Add(ctx, r, pos[0], os.Stderr)
	case "remove":
		if !need(1) {
			return 2
		}
		err = bench.Remove(r, pos[0], teardown)
	case "pause", "resume":
		if !need(1) {
			return 2
		}
		err = map[string]func(bench.Root, string) error{"pause": bench.Pause, "resume": bench.Resume}[verb](r, pos[0])
	case "status":
		name := ""
		if len(pos) == 1 {
			name = pos[0]
		}
		err = bench.Status(r, name, os.Stdout)
	case "results":
		if !need(1) {
			return 2
		}
		err = bench.Results(ctx, r, pos[0], with, os.Stderr)
	case "preflight":
		err = bench.Preflight(ctx, r, specs, bench.PreflightOptions{MaxLoadPerCore: *maxLoad, AllowMemoryDiag: allow}, os.Stdout)
	case "lane":
		if len(pos) != 2 || pos[0] != "reset" {
			fmt.Fprintln(os.Stderr, usage)
			return 2
		}
		err = bench.LaneReset(r, pos[1])
	default:
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// parseInterleaved parses flags given before or after the positional arguments (`remove NAME --teardown`) and returns
// the positional ones.
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		if args[0] == "--" {
			return append(pos, args[1:]...), nil
		}
		pos, args = append(pos, args[0]), args[1:]
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(s string) error { *m = append(*m, s); return nil }

// probe implements `fuzzops-bench probe --staging S [--fuzzops F,...] LANE=STEP-ID ...`.
func probe(args []string) int {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	staging := fs.String("staging", "", "the host's staging directory")
	fz := fs.String("fuzzops", "", "fuzzops checkouts whose in-flight markers to list (comma-separated)")
	if err := fs.Parse(args); err != nil || *staging == "" {
		return 2
	}
	steps := map[string]string{}
	for _, a := range fs.Args() {
		l, id, ok := strings.Cut(a, "=")
		if !ok {
			fmt.Fprintln(os.Stderr, "want LANE=STEP-ID:", a)
			return 2
		}
		steps[l] = id
	}
	var dirs []string
	if *fz != "" {
		dirs = strings.Split(*fz, ",")
	}
	b, _ := json.Marshal(bench.ProbeStaging(*staging, steps, dirs))
	fmt.Println(string(b))
	return 0
}

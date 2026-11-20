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
	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

const usage = `usage: lanes VERB --root R ...
  manager start|stop|restart|status|logs   the manager as a systemd --user unit (use this, not serve, for real runs)
  serve                      run the manager in the foreground (reconciles every 10 s; steps run on without it)
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
  plan add FILE [--dry-run [--json]] | pause|resume|remove [--keep] NAME | skip [--cancel] NAME ITEM | unskip NAME ITEM | status [NAME] [--json] | results NAME [--tsv] [--final] [--done] [--grade-fields a,b]   the plan engine: arms x items x reps with verified-artifact edges (docs/plan.md)
  probe --staging S [--fuzzops F,..] LANE=STEP ...   how a host's steps stand, as JSON (the manager runs it over ssh)`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if wantsHelp(os.Args[1:2]) {
		printOverview(os.Stdout)
		return
	}
	os.Exit(run(os.Args[1], os.Args[2:]))
}

func run(verb string, args []string) int {
	if wantsHelp(args) && printHelp(verb, os.Stdout) {
		return 0
	}
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
	jsonOut := fs.Bool("json", false, "plan status, plan add --dry-run: print JSON")
	cancel := fs.Bool("cancel", false, "plan remove, plan skip: also cancel the runs in flight (their queued runs are dropped; a run on a lane finishes)")
	keep := fs.Bool("keep", false, "plan remove: only retire the plan (no new work; runs in flight finish); its experiments and name stay")
	dryRun := fs.Bool("dry-run", false, "plan add: validate fully and print the expanded plan; store nothing")
	tsv := fs.Bool("tsv", false, "plan results: tab-separated rows with a header (default: JSON lines)")
	final := fs.Bool("final", false, "plan results: only the last attempt of each node")
	doneOnly := fs.Bool("done", false, "plan results: only done rows")
	gradeFields := fs.String("grade-fields", "", "plan results --tsv: top-level fields of the grade to add as columns (comma-separated)")
	teardown := false
	if verb == "remove" {
		fs.BoolVar(&teardown, "teardown", false, "remove: delete the lanes' clusters even when the spec keeps its lanes")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if verb == "probe" {
		return probe(args)
	}
	if verb == "version" {
		fmt.Println(revision())
		return 0
	}
	if verb == "manager" {
		exe, _ := os.Executable()
		return managerCommand(args, os.Environ(), exe, os.Stdout, os.Stderr, execRunner)
	}
	if verb == "host" {
		// The host verb has flags of its own (--cpus, --prev, --drain), so only --root is taken out here.
		hostRoot, hostArgs := extractRoot(args)
		if hostRoot != "" {
			*root = hostRoot
		}
		args = hostArgs
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
	case "plan":
		err = planVerb(r, pos, *jsonOut, *dryRun, *cancel, *keep, bench.ResultsOptions{TSV: *tsv, Final: *final, Done: *doneOnly, GradeFields: *gradeFields})
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

// extractRoot removes --root (also -root, --root=R) from args and returns its value ("" when absent).
func extractRoot(args []string) (string, []string) {
	root := ""
	var rest []string
	for i := 0; i < len(args); i++ {
		a := strings.TrimPrefix(strings.TrimPrefix(args[i], "-"), "-")
		switch {
		case args[i] != a && a == "root" && i+1 < len(args):
			root = args[i+1]
			i++
		case args[i] != a && strings.HasPrefix(a, "root="):
			root = strings.TrimPrefix(a, "root=")
		default:
			rest = append(rest, args[i])
		}
	}
	return root, rest
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

// planVerb is `lanes plan add FILE | status [NAME] | results NAME`.
func planVerb(r bench.Root, pos []string, jsonOut, dryRun, cancel, keep bool, ro bench.ResultsOptions) error {
	if len(pos) == 0 {
		return fmt.Errorf("plan: want add FILE | pause|resume|remove NAME | skip NAME ITEM | status [NAME] | results NAME")
	}
	switch pos[0] {
	case "add":
		if len(pos) != 2 {
			return fmt.Errorf("plan add: want FILE")
		}
		if dryRun {
			return bench.PlanDryRun(r, pos[1], jsonOut, os.Stdout)
		}
		return bench.PlanAdd(r, pos[1], os.Stdout)
	case "status":
		name := ""
		if len(pos) == 2 {
			name = pos[1]
		} else if len(pos) > 2 {
			return fmt.Errorf("plan status: want [NAME]")
		}
		return bench.PlanStatus(r, name, jsonOut, os.Stdout)
	case "results":
		if len(pos) != 2 {
			return fmt.Errorf("plan results: want NAME")
		}
		return bench.PlanResults(r, pos[1], ro, os.Stdout, os.Stderr)
	case "pause", "resume", "remove", "unskip":
		want := 2
		if pos[0] == "unskip" {
			want = 3
		}
		if len(pos) != want {
			return fmt.Errorf("plan %s: want NAME%s", pos[0], map[bool]string{true: " ITEM"}[want == 3])
		}
		switch pos[0] {
		case "pause":
			return bench.PlanControl(r, pos[1], plan.PausePlan)
		case "resume":
			return bench.PlanControl(r, pos[1], plan.ResumePlan)
		case "remove":
			if keep {
				return bench.PlanControl(r, pos[1], func(d string) error { return plan.RemovePlan(d, cancel) })
			}
			return bench.PlanRemove(r, pos[1], false, os.Stdout)
		}
		return bench.PlanControl(r, pos[1], func(d string) error { return plan.UnskipItem(d, pos[2]) })
	case "skip":
		if len(pos) != 3 {
			return fmt.Errorf("plan skip: want NAME ITEM")
		}
		return bench.PlanControl(r, pos[1], func(d string) error { return plan.SkipItem(d, pos[2], cancel) })
	}
	return fmt.Errorf("plan: unknown subcommand %q (want add, status, results, pause, resume, remove, skip, unskip)", pos[0])
}

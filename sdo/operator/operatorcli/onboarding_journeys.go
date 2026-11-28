package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// OnboardingJourneysUsage documents "sdo-operator onboarding journeys".
const OnboardingJourneysUsage = `usage: sdo-operator onboarding journeys --repo DIR --namespace NS [--only ID,ID] [--alter] [--soak 5m]
                                       [--plan | --quick --only ID] [--verbose | --json]
                                       [--kubeconfig FILE] [--operator-root DIR] [--seed FILE]

  Runs the probes in DIR's working tree (no commit needed) against the healthy application, through
  port forwards. The report starts with the checklist of the user journeys .sdo/goal.md lists, in
  the goal's order, one line each:
    missing-map      no operation of the map names it (journey: ID)
    missing-probe    an operation of it has no probe a health-probe workload schedules, or writes
                     with only call probes
    failing          a probe of it is red in this run
    unchecked        a probe of it was not judged in this run (--only, --plan); never passed
    passed           every probe of it judged, none red; says which optional items did not run
  with how soon a full failure of the journey is noticed (each operation by its soonest probe, the
  journey by its latest operation), computed from the workloads' schedule and SDO's confirmation,
  what its probes cost and what sampling the slowest at the 10 s floor would add. Then one line per
  scenario, with details for red checks. --verbose shows every finding:
    budget           requests/s planned and left under the cap, per workload
    scheduled        a workload runs it; how long until a full failure is confirmed
    passes           three runs in a row pass
    cleanup          its cleanup succeeds; from the steps alone (also --plan): a cleanup reads its
                     data back gone, and an idempotent write names no new object (Unique)
    concurrent       a second prober running it at once does not make it fail
    latency-bound    its maxLatency leaves room over the slowest healthy run
    altered-answers  (--alter) it fails when a read answers empty, short, changed or as in an
                     earlier run; the answers are altered on their way back, nothing is broken
    healthy-run      (--soak, for people re-checking an installed fix) each selected scenario that
                     passed its checks, sampled on a schedule. It stops early once every scenario
                     is red or has 10 samples, none failed. An onboarding session refuses it: the
                     installed controller soaks the export after the session.
  --only selects exact IDs or, for unmatched filters, ID substrings. --plan builds the probes and prints the
  checklist, schedule and budget without a cluster: nothing is sent.
  Probe cadence targets detection within 30s by default. Set a different target in the human
  goal with a line such as Detection target: 45s. SDO uses the target for an unset sampleInterval
  and defaults an unset ratePerSecond to one iteration per scenario per interval.
  --quick runs memory check --replay off (using its publication environment), then runs one iteration
  of the exact scenario named by --only, including setup and cleanup, and prints its step trace.
  Use it to iterate on a probe before the full checks. A rejected memory check stops before any requests
  and prints its own diagnostics (MemoryCheckReport with --json).
  Every other mode also runs memory check --replay off, so no separate memory check is needed: each
  blocking error is a red memory-check line, notes are counted (--verbose lists them), and a rejection
  does not stop the probes. Without SDO_ACTOR, SDO_PHASE and SDO_BASE_COMMIT it prints one line saying it was skipped.
  Exit 0 when nothing is red and
  no journey is missing or failing, 1 otherwise, 2 for usage or infrastructure errors.`

func onboardingJourneys(ctx context.Context, args []string, stdout, stderr io.Writer) (int, error) {
	ctx, stop := processio.TerminationContext(ctx)
	defer stop()
	flags := flag.NewFlagSet("onboarding journeys", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repo := flags.String("repo", "", "application repository")
	namespace := flags.String("namespace", "", "application namespace")
	kubeconfig := flags.String("kubeconfig", "", "kubeconfig file")
	only := flags.String("only", "", "comma-separated scenario IDs or substrings")
	alter := flags.Bool("alter", false, "also alter each answer")
	soak := flags.Duration("soak", 0, "also run passing selected scenarios this long")
	asJSON := flags.Bool("json", false, "print the report as JSON")
	verbose := flags.Bool("verbose", false, "print every finding and progress")
	quick := flags.Bool("quick", false, "validate memory and run one iteration of the selected scenario")
	plan := flags.Bool("plan", false, "read the schedule only: send nothing")
	operatorRoot := flags.String("operator-root", operatorRootDefault(processio.Environment), "operator source tree")
	seed := flags.String("seed", "", "probe-identity seed file (default: .git/sdo-probe-identity/seed in DIR)")
	if err := flags.Parse(args); err != nil {
		return 0, err
	}
	if flags.NArg() != 0 || *namespace == "" || !filepath.IsAbs(*repo) {
		return 0, errors.New("journeys requires --repo (absolute path) and --namespace")
	}
	if *soak != 0 && onboardingSession(processio.Environment) {
		return 0, errors.New("--soak is not needed in an onboarding session: after this session SDO installs the export, soaks its probes and sends one follow-up if one fails; commit and finish")
	}
	if *quick && (*only == "" || strings.Contains(*only, ",") || *plan || *alter || *soak != 0) {
		return 0, errors.New("--quick requires --only with one exact scenario ID and cannot combine with --plan, --alter or --soak")
	}
	slog.SetLogLoggerLevel(slog.LevelWarn) // the build's stage lines are not findings
	if *quick {
		checkArgs := []string{"check", "--repo", *repo, "--replay", "off"}
		if *asJSON {
			checkArgs = append(checkArgs, "--json")
		}
		var checked bytes.Buffer
		code := RunMemory(ctx, checkArgs, func(key string) string {
			if key == "SDO_OPERATOR_ROOT" {
				return *operatorRoot
			}
			return processio.Environment(key)
		}, &checked, stderr)
		if code != exitClean {
			fmt.Fprint(stdout, checked.String())
			return code, nil
		}
		if *verbose && !*asJSON {
			fmt.Fprint(stdout, checked.String())
		}
	}
	var memoryResults []journeycheck.Result
	var memoryCheck *journeycheck.MemoryCheck
	if !*quick {
		memoryResults, memoryCheck = journeysMemoryCheck(ctx, *repo, *operatorRoot)
	}
	if *seed == "" {
		*seed = filepath.Join(*repo, ".git", "sdo-probe-identity", "seed")
	}
	var parts []string
	if *only != "" {
		parts = strings.Split(*only, ",")
	}
	var given []appmap.GivenJourney
	var applicationMap appmap.Map
	var mapErr string
	if !*quick {
		var err error
		given, applicationMap, mapErr, err = journeyGoalAndMap(*repo)
		if err != nil {
			return 0, err
		}
	}
	diagnostics, err := memory.CandidateDiagnostics(*repo)
	if err != nil {
		return 0, err
	}
	workspace, err := builder.BuildWorkspace(ctx, builder.Config{Replay: "off", ApplicationRoot: *repo, DiagnosticsRoot: diagnostics, OperatorRoot: *operatorRoot, CompileOnly: true})
	if err != nil {
		return reportCompile(stdout, err)
	}
	defer workspace.Close()
	binary, err := workspace.BuildJourneyCheck(ctx)
	if err != nil {
		return reportCompile(stdout, err)
	}
	target, err := layout.ReadGoalDetectionTarget(*repo)
	if err != nil {
		return 0, err
	}
	options, err := json.Marshal(journeycheck.Options{
		Namespace: *namespace, SeedPath: *seed, Only: parts, Alter: *alter, Soak: *soak,
		Plan: *plan, Once: *quick, Verbose: *verbose, DetectionTarget: target,
	})
	if err != nil {
		return 0, err
	}
	var output bytes.Buffer
	if err := hostio.RunProgram(ctx, binary, *kubeconfig, bytes.NewReader(options), &output, stderr); err != nil {
		return 0, fmt.Errorf("run the probes: %w", err)
	}
	var report journeycheck.Report
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		return 0, fmt.Errorf("read the probe report: %w", err)
	}
	if !*quick {
		if mapErr != "" {
			report.Results = append([]journeycheck.Result{{Item: "application-map", Subject: "all", Status: journeycheck.Red, Detail: mapErr}}, report.Results...)
		}
		report.Results = append(memoryResults, report.Results...)
		report.Memory = memoryCheck
		report.AddChecklist(given, applicationMap)
	}
	if *asJSON {
		data, err := json.Marshal(report)
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(stdout, string(data))
	} else if *verbose {
		fmt.Fprint(stdout, report.VerboseText())
	} else {
		fmt.Fprint(stdout, report.Text())
	}
	if report.Red() {
		return exitDiagnostics, nil
	}
	return exitClean, nil
}

// onboardingSession reports whether the command runs in an onboarding
// agent's session (the initial health-judge actor), where the installed
// controller soaks the export after the session.
func onboardingSession(getenv func(string) string) bool {
	return getenv(memoryActorEnv) == string(validate.HealthJudge) && getenv(memoryPhaseEnv) == string(validate.InitialPhase)
}

// journeysMemoryCheck runs the shared memory check (replay off, as --quick
// does) and returns its blocking errors as red results and the rest as a
// MemoryCheck, so one journeys run reports both validators. A rejection never
// stops the probes. Without the publication environment (SDO_ACTOR, SDO_PHASE
// and SDO_BASE_COMMIT, set in an onboarding session) the check cannot run, as
// under --quick; here that is one line saying so, not a failure.
func journeysMemoryCheck(ctx context.Context, repo, operatorRoot string) ([]journeycheck.Result, *journeycheck.MemoryCheck) {
	getenv := func(key string) string {
		if key == "SDO_OPERATOR_ROOT" {
			return operatorRoot
		}
		return processio.Environment(key)
	}
	var unset []string
	for _, key := range []string{memoryActorEnv, memoryPhaseEnv, "SDO_BASE_COMMIT"} {
		if getenv(key) == "" {
			unset = append(unset, key)
		}
	}
	if len(unset) > 0 {
		return nil, &journeycheck.MemoryCheck{Skipped: strings.Join(unset, ", ") + " not set (they are set in an onboarding session)"}
	}
	var checked, failed bytes.Buffer
	code := RunMemory(ctx, []string{"check", "--repo", repo, "--replay", "off", "--json"}, getenv, &checked, &failed)
	red := func(detail string) []journeycheck.Result {
		return []journeycheck.Result{{Item: journeycheck.ItemMemoryCheck, Subject: "all", Status: journeycheck.Red, Detail: detail}}
	}
	report := &contracts.MemoryCheckReport{}
	if code != exitClean && code != exitDiagnostics || protojson.Unmarshal(checked.Bytes(), report) != nil {
		detail := strings.TrimSpace(failed.String())
		if detail == "" {
			detail = strings.TrimSpace(checked.String())
		}
		return red(fmt.Sprintf("memory check did not finish (exit %d): %s", code, detail)), nil
	}
	var results []journeycheck.Result
	for _, d := range report.GetDiagnostics() {
		results = append(results, red(protoDiagnosticLine(d))...)
	}
	notes := &journeycheck.MemoryCheck{}
	for _, gap := range report.GetCoverageGaps() {
		notes.Notes = append(notes.Notes, "coverage_gap "+protoDiagnosticLine(gap))
	}
	for _, note := range report.GetMitigationReplayNotes() {
		notes.Notes = append(notes.Notes, "program "+note.GetProgramId()+strings.TrimPrefix(replayCaseLine(note), " "))
	}
	return results, notes
}

// protoDiagnosticLine is diagnosticLine of a MemoryCheckReport diagnostic.
func protoDiagnosticLine(d *contracts.MemoryDiagnostic) string {
	return diagnosticLine(validate.Diagnostic{Rule: d.GetRule(), Path: d.GetPath(), Line: int(d.GetLine()), Message: d.GetMessage(), Hint: d.GetHint()})
}

// reportCompile prints a build failure of the probes as a red item.
func reportCompile(stdout io.Writer, err error) (int, error) {
	fmt.Fprintf(stdout, "red compiles all: %s\n1 red\n", err)
	return exitDiagnostics, nil
}

// journeyGoalAndMap reads the user journeys .sdo/goal.md lists and the
// application map of .sdo/arch.md from the working tree. A map that is
// missing or does not parse is reported (mapErr) and leaves every journey
// unmapped; a goal whose journeys do not parse is an error, since the
// checklist would silently lose them.
func journeyGoalAndMap(repo string) (given []appmap.GivenJourney, m appmap.Map, mapErr string, err error) {
	if given, err = goalJourneys(repo); err != nil {
		return nil, m, "", err
	}
	arch, err := os.ReadFile(filepath.Join(repo, ".sdo", "arch.md"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, m, "", err
	}
	region, found, err := appmap.Find(string(arch))
	switch {
	case err != nil:
		return given, m, ".sdo/arch.md: " + err.Error(), nil
	case !found:
		return given, m, ".sdo/arch.md has no application map, so no journey is mapped", nil
	}
	if m, err = appmap.Parse(region.YAML); err != nil {
		return given, appmap.Map{}, ".sdo/arch.md application map: " + err.Error(), nil
	}
	return given, m, "", nil
}

// goalJourneys reads the journeys of the working tree's .sdo/goal.md; none
// when there is no goal. Any other read or parse failure is an error, never
// an empty list, so no check passes for want of the journeys it should judge.
func goalJourneys(repo string) ([]appmap.GivenJourney, error) {
	goal, err := os.ReadFile(filepath.Join(repo, ".sdo", "goal.md"))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, nil
	case err != nil:
		return nil, err
	}
	given, err := appmap.ParseGoalJourneys(goal)
	if err != nil {
		return nil, fmt.Errorf(".sdo/goal.md: %w", err)
	}
	return given, nil
}

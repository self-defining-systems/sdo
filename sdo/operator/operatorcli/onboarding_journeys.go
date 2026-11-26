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
	"time"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// OnboardingJourneysUsage documents "sdo-operator onboarding journeys".
const OnboardingJourneysUsage = `usage: sdo-operator onboarding journeys --repo DIR --namespace NS [--only ID,ID] [--alter] [--soak 5m]
                                       [--plan] [--kubeconfig FILE] [--json] [--operator-root DIR] [--seed FILE]

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
  finding, the first red first:
    budget           requests/s planned and left under the cap, per workload
    scheduled        a workload runs it; how long until a full failure is confirmed
    passes           three runs in a row pass
    cleanup          its cleanup succeeds
    concurrent       a second prober running it at once does not make it fail
    latency-bound    its maxLatency leaves room over the slowest healthy run
    altered-answers  (--alter) it fails when a read answers empty, short, changed or as in an
                     earlier run; the answers are altered on their way back, nothing is broken
    healthy-run      (--soak, when nothing is red) every probe at its own schedule, no failure
  --only keeps scenarios whose ID contains one of the texts. --plan builds the probes and prints the
  checklist, schedule and budget without a cluster: nothing is sent. Exit 0 when nothing is red and
  no journey is missing or failing, 1 otherwise, 2 for usage or infrastructure errors.`

func onboardingJourneys(ctx context.Context, args []string, stdout, stderr io.Writer) (int, error) {
	ctx, stop := processio.TerminationContext(ctx)
	defer stop()
	flags := flag.NewFlagSet("onboarding journeys", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repo := flags.String("repo", "", "application repository")
	namespace := flags.String("namespace", "", "application namespace")
	kubeconfig := flags.String("kubeconfig", "", "kubeconfig file")
	only := flags.String("only", "", "comma-separated scenario ID parts")
	alter := flags.Bool("alter", false, "also alter each answer")
	soak := flags.Duration("soak", 0, "also run every probe at its schedule this long")
	asJSON := flags.Bool("json", false, "print the report as JSON")
	plan := flags.Bool("plan", false, "read the schedule only: send nothing")
	operatorRoot := flags.String("operator-root", operatorRootDefault(processio.Environment), "operator source tree")
	seed := flags.String("seed", "", "probe-identity seed file (default: .git/sdo-probe-identity/seed in DIR)")
	if err := flags.Parse(args); err != nil {
		return 0, err
	}
	if flags.NArg() != 0 || *namespace == "" || !filepath.IsAbs(*repo) {
		return 0, errors.New("journeys requires --repo (absolute path) and --namespace")
	}
	slog.SetLogLoggerLevel(slog.LevelWarn) // the build's stage lines are not findings
	if *seed == "" {
		*seed = filepath.Join(*repo, ".git", "sdo-probe-identity", "seed")
	}
	var parts []string
	if *only != "" {
		parts = strings.Split(*only, ",")
	}
	given, applicationMap, mapErr, err := journeyGoalAndMap(*repo)
	if err != nil {
		return 0, err
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
	options, err := json.Marshal(map[string]any{"Namespace": *namespace, "SeedPath": *seed, "Only": parts, "Alter": *alter, "Soak": time.Duration(*soak), "Plan": *plan})
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
	if mapErr != "" {
		report.Results = append([]journeycheck.Result{{Item: "application-map", Subject: "all", Status: journeycheck.Red, Detail: mapErr}}, report.Results...)
	}
	report.AddChecklist(given, applicationMap)
	if *asJSON {
		data, err := json.Marshal(report)
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(stdout, string(data))
	} else {
		fmt.Fprint(stdout, report.Text())
	}
	if report.Red() {
		return exitDiagnostics, nil
	}
	return exitClean, nil
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

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
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// OnboardingJourneysUsage documents "sdo-operator onboarding journeys".
const OnboardingJourneysUsage = `usage: sdo-operator onboarding journeys --repo DIR --namespace NS [--only ID,ID] [--alter] [--soak 5m]
                                       [--kubeconfig FILE] [--json] [--operator-root DIR] [--seed FILE]

  Runs the probes in DIR's working tree (no commit needed) against the healthy application, through
  port forwards, and reports one line per finding, the first red first:
    budget           requests/s planned and left under the cap, per workload
    scheduled        a workload runs it; how long until a full failure is confirmed
    passes           three runs in a row pass
    cleanup          its cleanup succeeds
    concurrent       a second prober running it at once does not make it fail
    latency-bound    its maxLatency leaves room over the slowest healthy run
    altered-answers  (--alter) it fails when a read answers empty, short, changed or as in an
                     earlier run; the answers are altered on their way back, nothing is broken
    healthy-run      (--soak, when nothing is red) every probe at its own schedule, no failure
  --only keeps scenarios whose ID contains one of the texts. Exit 0 when nothing is red, 1 when
  something is, 2 for usage or infrastructure errors.`

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
	options, err := json.Marshal(map[string]any{"Namespace": *namespace, "SeedPath": *seed, "Only": parts, "Alter": *alter, "Soak": time.Duration(*soak)})
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
	if *asJSON {
		fmt.Fprintln(stdout, strings.TrimSpace(output.String()))
	} else {
		fmt.Fprint(stdout, report.Text())
	}
	if report.Reds() > 0 {
		return exitDiagnostics, nil
	}
	return exitClean, nil
}

// reportCompile prints a build failure of the probes as a red item.
func reportCompile(stdout io.Writer, err error) (int, error) {
	fmt.Fprintf(stdout, "red compiles all: %s\n1 red\n", err)
	return exitDiagnostics, nil
}

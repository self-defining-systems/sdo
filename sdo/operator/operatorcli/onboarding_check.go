package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/onboardcheck"
	"sdo.dev/operator/onboardcheck/kubebreak"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus"
	"sigs.k8s.io/yaml"
)

// OnboardingUsage documents "sdo-operator onboarding".
const OnboardingUsage = `usage: sdo-operator onboarding check --repo DIR --namespace NS --prometheus URL|NS/SERVICE:PORT
                                    [--kubeconfig FILE] [--context NAME] [--requests 5]
                                    [--settle 90s] [--json]
                                    [--quiet-window 10m] [--quiet-interval 1m]
                                    [--break] [--break-bound 60s] [--break-mode scale|network]
                                    [--cluster-network] [--operator-root DIR] [--telemetry-environment FILE] [--settings FILE]

  check  Run the onboarding check against a running environment: the metric
         items, then quiet-when-healthy, then (with --break) fails-under-break.
         quiet-when-healthy evaluates the memory's detectors (test-request
         scenarios and the request-metrics detector included) against the live
         application every --quiet-interval for --quiet-window (0 skips) and is
         red if any raises, naming the detector, scenario and step.
         fails-under-break breaks, one at a time, each component on an
         operation's path (scale the workload to zero; with --break-mode network
         a deny-ingress NetworkPolicy when the CNI enforces it, else scale to
         zero, said in the report), waits up to --break-bound for raises that name
         the component or its operations, and restores it. An operation is green
         only when a raise names it or one of its probe scenarios; a generic
         detector that names just the component leaves it red, and the report
         says which detectors noticed. It is red (stopping) if a restore cannot
         be verified. Detectors that dial in-cluster Service names cannot be judged
         from the host running the check and are reported as a note, unless
         --cluster-network says this process resolves them (the test-request
         scenarios need it too; a scenario that never succeeded is red). Run it only on a target the agent owns, with no users.
         Each live item reports its duration.
         The metric items: SDO builds each operation's request and error series from
         the application map's entries (metric, route, method and status labels),
         sends --requests requests to every operation's entry through the API
         server's Service proxy, and verifies each series carries the operation's
         route and rises by at least what SDO sent. It also verifies that every
         exposed entry has an entries record, that the entry metric rose by what
         SDO sent to it, and that every route value that moved is declared.
         Refuses a repository with uncommitted changes; the report is headed by
         the HEAD commit. --prometheus is a URL, or NS/SERVICE:PORT reached
         through the API server. Background-work operations are not driven.
         Exit 0 when nothing is red, 1 when an item is red, 2 for usage or
         infrastructure errors.`

// RunOnboarding runs "sdo-operator onboarding <args>" and returns the exit code.
func RunOnboarding(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Fprintln(stdout, OnboardingUsage)
		return exitClean
	}
	if args[0] != "check" {
		fmt.Fprintf(stderr, "unknown onboarding command %q\n%s\n", args[0], OnboardingUsage)
		return exitInfra
	}
	code, err := onboardingCheck(ctx, args[1:], stdout)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, OnboardingUsage)
		return exitClean
	}
	if err != nil {
		fmt.Fprintln(stderr, "sdo-operator onboarding:", err)
		return exitInfra
	}
	return code
}

type promAdapter struct{ client prometheus.Client }

func (p promAdapter) Query(ctx context.Context, text string) ([]metrics.Series, error) {
	result, err := p.client.Query(ctx, telemetry.Query{Kind: telemetry.KindMetrics, Text: text}, processio.Now())
	if err != nil {
		return nil, err
	}
	return result.Series, nil
}

// detectorEvaluator reads the memory's compiled detectors, which a single
// long-running evaluation keeps judging the live application. Evaluate returns
// the first verdict judged after the call, so a break made just before it is
// seen.
type detectorEvaluator struct{ watch *deployment.Watcher }

func (e detectorEvaluator) Evaluate(ctx context.Context) (onboardcheck.Verdict, error) {
	verdict, err := e.watch.Next(ctx, processio.Now())
	if err != nil {
		return onboardcheck.Verdict{}, err
	}
	out := onboardcheck.Verdict{Status: string(verdict.Status)}
	for _, f := range verdict.Findings {
		if f.Status != sdk.FindingActive {
			continue
		}
		resources := []string{f.PrimaryResource.Name}
		for _, r := range f.RelatedResources {
			resources = append(resources, r.Name)
		}
		out.Raised = append(out.Raised, onboardcheck.Raise{DetectorID: f.DetectorID, Rule: f.RuleID, Summary: f.Summary, Evidence: f.Evidence, Resources: resources, Fingerprint: sdk.FindingFingerprint(f)})
	}
	for _, state := range verdict.DetectorStates {
		if state.Status == "pending" {
			out.Pending = append(out.Pending, state.DetectorID+": "+state.Reason)
		}
	}
	for _, u := range verdict.Unavailable {
		if reason, outside := strings.CutPrefix(u.Reason, evaluation.OutsideClusterReason); outside {
			out.Skipped = append(out.Skipped, u.DetectorID+": "+reason)
			continue
		}
		out.Unavailable = append(out.Unavailable, u.DetectorID+": "+u.Reason)
	}
	if verdict.Status == evaluation.Unavailable && len(out.Unavailable) == 0 {
		out.Status = "clear"
		if len(out.Raised) > 0 {
			out.Status = "firing"
		}
	}
	return out, nil
}

// httpSender sends requests to an operation's entry Service through the proxy.
type httpSender struct {
	cluster   *processio.KubeCluster
	namespace string
}

func (s httpSender) Send(ctx context.Context, op appmap.Operation, n int) (int, error) {
	if op.Entry.Method == "" || op.Entry.Method == appmap.MethodExec {
		return 0, nil
	}
	url := s.cluster.ServiceURL(s.namespace, op.Entry.Service, op.Entry.Port, op.Entry.Path)
	sent := 0
	for i := 0; i < n; i++ {
		req, err := http.NewRequestWithContext(ctx, op.Entry.Method, url, nil)
		if err != nil {
			return sent, err
		}
		resp, err := s.cluster.Do(req)
		if err != nil {
			return sent, fmt.Errorf("%s %s: %w", op.Entry.Method, url, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		sent++
	}
	return sent, nil
}

func onboardingCheck(ctx context.Context, args []string, stdout io.Writer) (int, error) {
	// An interrupt cancels the check but never skips a restore: a broken
	// component is undone on a context that outlives the cancellation.
	ctx, stop := processio.TerminationContext(ctx)
	defer stop()
	flags := flag.NewFlagSet("onboarding check", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repo := flags.String("repo", "", "application repository (git root) holding .sdo/arch.md")
	namespace := flags.String("namespace", "", "application namespace")
	promTarget := flags.String("prometheus", "", "Prometheus URL, or NS/SERVICE:PORT through the API server")
	kubeconfig := flags.String("kubeconfig", "", "kubeconfig file")
	kubeContext := flags.String("context", "", "kubeconfig context")
	requests := flags.Int("requests", 5, "requests sent to each operation")
	settle := flags.Duration("settle", 90*time.Second, "wait for scrapes after sending")
	asJSON := flags.Bool("json", false, "print the report as JSON")
	quietWindow := flags.Duration("quiet-window", 10*time.Minute, "quiet-when-healthy window; 0 skips the item")
	quietInterval := flags.Duration("quiet-interval", time.Minute, "wait between detector evaluations in the quiet window")
	breakItem := flags.Bool("break", false, "run fails-under-break: briefly break each component on an operation's path (only on a target without users)")
	breakBound := flags.Duration("break-bound", 60*time.Second, "least time to wait for a detector to notice each break (at least the time the memory's own probes need to judge a failure)")
	breakMode := flags.String("break-mode", kubebreak.ModeScale, "how to break: scale (to zero) or network (deny-ingress NetworkPolicy; scale when the CNI does not enforce it)")
	operatorRoot := flags.String("operator-root", operatorRootDefault(processio.Environment), "operator source tree detectors are built from")
	clusterNetwork := flags.Bool("cluster-network", false, "this process resolves and dials in-cluster Service names (it runs in a pod or on the cluster network); without it, detectors that dial Service names are not judged")
	telemetryEnv := flags.String("telemetry-environment", "", "telemetry environment document the detector evaluation reads")
	settingsFile := flags.String("settings", "", "operator configuration file the detector evaluation runs under")
	if err := flags.Parse(args); err != nil {
		return 0, err
	}
	if flags.NArg() != 0 || *namespace == "" || *promTarget == "" || !filepath.IsAbs(*repo) {
		return 0, errors.New("check requires --repo (absolute path), --namespace and --prometheus")
	}
	commit, err := processio.AcceptedHead(ctx, *repo)
	if err != nil {
		return 0, fmt.Errorf("%w (commit the map first: the report is tagged with the commit)", err)
	}
	doc, err := os.ReadFile(filepath.Join(*repo, ".sdo", "arch.md"))
	if err != nil {
		return 0, err
	}
	region, found, err := appmap.Find(string(doc))
	if err != nil || !found {
		return 0, fmt.Errorf("application map in .sdo/arch.md: found=%v err=%v", found, err)
	}
	m, err := appmap.Parse(region.YAML)
	if err != nil {
		return 0, fmt.Errorf("application map: %w", err)
	}
	cluster, err := processio.OpenKubeCluster(*kubeconfig, *kubeContext)
	if err != nil {
		return 0, err
	}
	base := *promTarget
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		ns, rest, ok := strings.Cut(base, "/")
		name, port, ok2 := strings.Cut(rest, ":")
		number, convErr := strconv.Atoi(port)
		if !ok || !ok2 || convErr != nil {
			return 0, fmt.Errorf("--prometheus %q is neither a URL nor NS/SERVICE:PORT", base)
		}
		base = cluster.ServiceURL(ns, name, number, "")
	}
	exposed, err := cluster.ExposedServices(ctx, *namespace)
	if err != nil {
		return 0, fmt.Errorf("list exposed services: %w", err)
	}
	var exposures []onboardcheck.Exposure
	for _, e := range exposed {
		exposures = append(exposures, onboardcheck.Exposure{Service: e.Service, Via: e.Via})
	}
	config := onboardcheck.Config{
		Map: m, Commit: commit,
		Prom:     promAdapter{prometheus.Client{BaseURL: base, HTTPClient: cluster}},
		Sender:   httpSender{cluster: cluster, namespace: *namespace},
		Exposed:  exposures,
		Requests: *requests, Settle: *settle, Poll: 5 * time.Second, Wait: processio.Wait,
		Now: processio.Now,
	}
	if *quietWindow > 0 || *breakItem {
		probe := deployment.AcceptedDetectorProbe{Settings: *settingsFile, TelemetryEnvironment: *telemetryEnv, Kubeconfig: *kubeconfig, OutsideCluster: !*clusterNetwork, Host: true}
		fmt.Fprintln(os.Stderr, "building the memory's detectors ...")
		tree, cleanupTree, err := preparedTree(ctx, *repo, *telemetryEnv)
		if err != nil {
			return 0, fmt.Errorf("prepare the memory as the controller will run it: %w", err)
		}
		defer cleanupTree()
		prepared, err := probe.Prepare(ctx, tree, *operatorRoot, *namespace, deployment.HealthScope{})
		if err != nil {
			return 0, fmt.Errorf("build detectors for the live items: %w", err)
		}
		defer prepared.Close()
		fmt.Fprintln(os.Stderr, "warming the detectors (test-request scenarios need their sample window) ...")
		watch := prepared.Watch(ctx, 5*time.Second)
		defer watch.Close()
		eval := detectorEvaluator{watch: watch}
		if *quietWindow > 0 {
			config.Quiet = &onboardcheck.QuietConfig{Eval: eval, Window: *quietWindow, Interval: *quietInterval}
		}
		if *breakItem {
			// Wait for the memory's own tests to judge a failure: their sampling
			// and holds set the time, not the flag's default.
			horizon := probeDetectionHorizon(*repo)
			bound := max(*breakBound, horizon)
			breaker, err := kubebreak.New(ctx, kubebreak.Config{Client: cluster.Kube(), Namespace: *namespace, Mode: *breakMode, Bound: 2 * bound, Interval: 5 * time.Second, Wait: processio.Wait})
			if err != nil {
				return 0, err
			}
			config.Break = &onboardcheck.BreakConfig{Eval: eval, Breaker: breaker, Bound: bound, Horizon: horizon, Interval: 5 * time.Second}
		}
	}
	report, err := onboardcheck.Run(ctx, config)
	if err != nil {
		return 0, err
	}
	if *asJSON {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return 0, err
		}
		fmt.Fprintln(stdout, string(encoded))
	} else {
		fmt.Fprint(stdout, report.Text())
	}
	if !report.Clean() {
		return exitDiagnostics, nil
	}
	return exitClean, nil
}

// probeDetectionHorizon is the longest time the repository's health-probe
// workloads can take to raise a failing scenario (zero without any).
func probeDetectionHorizon(repo string) time.Duration {
	files, _ := filepath.Glob(filepath.Join(repo, ".sdo", "diagnostics", "traffic", "workloads", "*.yaml"))
	var longest time.Duration
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		raw, err := yaml.YAMLToJSON(data)
		if err != nil {
			continue
		}
		if workload, err := traffic.ParseWorkloadJSON(raw); err == nil {
			longest = max(longest, workload.DetectionHorizon())
		}
	}
	return longest
}

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
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/kubereads"
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
                                    [--settle 90s] [--json] [--report FILE]
                                    [--quick] [--quiet-window 10m] [--quiet-interval 1m] [--quiet-evaluations 3]
                                    [--break] [--break-bound 60s] [--break-mode auto|scale|network]
                                    [--cluster-network] [--operator-root DIR] [--telemetry-environment FILE] [--settings FILE]

  check  Run the onboarding check against a running environment: the metric
         items with quiet-when-healthy observing alongside, then (with --break)
         fails-under-break. Progress lines go to stderr; the report is headed
         by its mode.
         --quick is the fast loop (about 2-3 minutes after the detectors build):
         the metric items and a short smoke where every detector judges at
         least once. It never runs --break and its report is never clean (it
         says "mode: quick"); exit 0 means no red item. The full check is the
         gate for "clean".
         The quiet window ends as soon as every detector has judged
         --quiet-evaluations (3) times past its own clearing horizon; the derived
         length is printed when the first verdict arrives, and --quiet-window
         is only the cap (a detector short of its judgements at the cap is red).
         quiet-when-healthy evaluates the memory's detectors (test-request
         scenarios and the request-metrics detector included) against the live
         application every --quiet-interval for --quiet-window (0 skips) and is
         red if any raises, naming the detector, scenario and step.
         fails-under-break breaks each component on an operation's path
         (components that share no operation are broken together in waves, and the components behind the entries are first broken together once, then alone only where their pooled break leaves it unclear which one an operation followed; a
         raise still has to name the operation or component) (by default a deny-ingress NetworkPolicy when the CNI
         enforces it, else scale the workload to zero; --break-mode scale
         always scales; the mode that ran is said in the report), waits up to --break-bound for raises that name
         the component or its operations, and restores it. An operation is green
         only when a failure signal (failing requests or a failing probe step)
         names it or one of its probe scenarios; a generic detector that names
         just the component, or an absence-only raise, leaves it red, and the report
         says which detectors noticed. It is red (stopping) if a restore cannot
         be verified. Detectors that dial in-cluster Service names cannot be judged
         from the host running the check and are reported as a note, unless
         --cluster-network says this process resolves them (the test-request
         scenarios need it too; a scenario that never succeeded is red). Run it only on a target the agent owns, with no users.
         With --break the worker-quiet break (a worker's egress denied except DNS:
         it stays Running and Ready) and the data-wrong break (a text column of
         the oldest rows of the most-read table of a postgres store overwritten,
         restored by primary key) run after the component breaks; each is judged
         like them (a raise must name an operation it hurts) and is always
         restored, also when the check is interrupted.
         probes-do-no-harm measures the application's request, error and job
         failure counters for --harm-window before the memory's probes start
         (overlapping the detector build) and with them running (the quiet
         window, at least --harm-window); it is red when the probes raise
         failures of an operation or job class, or are a large share of the
         entry's requests, naming them (0 skips).
         operations-observed, latency-per-operation and background-covered read
         the application's own metrics: a declared route that never appears
         (no reason excuses it), an operation without a request-duration series,
         most of an entry's real traffic on undeclared routes, and background
         job classes no operation triggers (unattributed background work) are red.
         The report ends with a journey table (one row per operation: a mapping,
         b errors, c latency, d probe, e detection, f map, g traces); clean needs
         every journey complete on a to f, which includes a --break run.
         given-journeys (when goal.md lists journeys) is red for a listed journey no
         operation serves and for an operation naming an unlisted one.
         routes-assigned runs the map's routes.list command in its component and
         reports each listed route no operation covers and nothing excludes.
         Each live item reports its duration. The check prints its expected
         duration when it starts and takes minutes (about 20 with --break): run it
         in the background and read the report when it finishes; --report FILE
         also writes the report there.
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
//
// The evaluation (and with it the memory's probes) starts on the first call, so
// the check can measure the application before any probe has run.
type detectorEvaluator struct{ lazy *lazyWatch }

// lazyWatch starts the detector evaluation once, when first needed.
type lazyWatch struct {
	once     sync.Once
	prepared *deployment.PreparedProbe
	ctx      context.Context
	watch    *deployment.Watcher
}

func (l *lazyWatch) get() *deployment.Watcher {
	l.once.Do(func() {
		fmt.Fprintln(os.Stderr, "warming the detectors (test-request scenarios need their sample window) ...")
		l.watch = l.prepared.Watch(l.ctx, 5*time.Second)
	})
	return l.watch
}

func (l *lazyWatch) Close() {
	if l.watch != nil {
		l.watch.Close()
	}
}

func (e detectorEvaluator) Evaluate(ctx context.Context) (onboardcheck.Verdict, error) {
	verdict, err := e.lazy.get().Next(ctx, processio.Now())
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
		operation, _ := f.Metadata["operation"].(string)
		out.Raised = append(out.Raised, onboardcheck.Raise{Operation: operation, DetectorID: f.DetectorID, Rule: f.RuleID, Summary: f.Summary, Evidence: f.Evidence, Resources: resources, Fingerprint: sdk.FindingFingerprint(f)})
	}
	for _, p := range verdict.Progress {
		out.Progress = append(out.Progress, onboardcheck.DetectorProgress{DetectorID: p.DetectorID, Interval: p.Interval, Horizon: p.Horizon, Evaluations: p.Evaluations, PastHorizon: p.PastHorizon})
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
	reportFile := flags.String("report", "", "also write the report to this file")
	quietWindow := flags.Duration("quiet-window", 10*time.Minute, "quiet-when-healthy window; 0 skips the item")
	quick := flags.Bool("quick", false, "fast iteration: the metric items and a short detector smoke; never clean, never breaks")
	quietEvaluations := flags.Int("quiet-evaluations", onboardcheck.DefaultQuietEvaluations, "judgements each detector must make past its own horizon for the quiet window to end")
	quietInterval := flags.Duration("quiet-interval", time.Minute, "wait between detector evaluations in the quiet window")
	harmWindow := flags.Duration("harm-window", time.Minute, "probes-do-no-harm: how long to measure the application with the memory's probes paused (before they start) and at least with them running; 0 skips the item")
	breakItem := flags.Bool("break", false, "run fails-under-break: briefly break each component on an operation's path (only on a target without users)")
	breakBound := flags.Duration("break-bound", 60*time.Second, "least time to wait for a detector to notice each break (at least the time the memory's own probes need to judge a failure)")
	breakMode := flags.String("break-mode", kubebreak.ModeAuto, "how to break: auto (deny-ingress NetworkPolicy when the CNI enforces it, else scale to zero), network (the same) or scale (to zero)")
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
	if *quick && *breakItem {
		return 0, errors.New("--quick never runs --break: the full check is the gate")
	}
	if *quick {
		*quietWindow = min(*quietWindow, 2*time.Minute)
	}
	fmt.Fprintln(os.Stderr, startNotice(*quick, *settle, *quietWindow, *breakItem, max(*breakBound, probeDetectionHorizon(*repo)), onboardcheck.PlannedBreakWaves(m), *reportFile))
	var given []appmap.GivenJourney
	if goal, err := os.ReadFile(filepath.Join(*repo, ".sdo", "goal.md")); err == nil {
		if given, err = appmap.ParseGoalJourneys(goal); err != nil {
			return 0, err
		}
	}
	config := onboardcheck.Config{
		Map: m, Commit: commit, Journeys: given,
		Prom:     promAdapter{prometheus.Client{BaseURL: base, HTTPClient: cluster}},
		Sender:   httpSender{cluster: cluster, namespace: *namespace},
		Exposed:  exposures,
		Routes:   routeLister{cluster: cluster, namespace: *namespace},
		Requests: *requests, Settle: *settle, Poll: 5 * time.Second, Wait: processio.Wait,
		Now: processio.Now, Quick: *quick,
		Progress: func(line string) { fmt.Fprintln(os.Stderr, processio.Now().Format("15:04:05"), line) },
	}
	var paused [2]onboardcheck.HarmSample
	pausedWindow := make(chan error, 1)
	if *harmWindow > 0 && *quietWindow > 0 {
		// Measured while the detectors build, before any probe has run.
		go func() {
			var err error
			if paused[0], err = onboardcheck.SampleHarm(ctx, config.Prom, m, processio.Now()); err != nil {
				pausedWindow <- err
				return
			}
			if err = processio.Wait(ctx, *harmWindow); err == nil {
				paused[1], err = onboardcheck.SampleHarm(ctx, config.Prom, m, processio.Now())
			}
			pausedWindow <- err
		}()
	}
	if *quietWindow > 0 || *breakItem {
		probe := deployment.AcceptedDetectorProbe{Settings: *settingsFile, TelemetryEnvironment: *telemetryEnv, Kubeconfig: *kubeconfig, OutsideCluster: !*clusterNetwork, Host: true}
		fmt.Fprintln(os.Stderr, "building the memory's detectors ...")
		tree, cleanupTree, err := preparedTree(ctx, *repo, *telemetryEnv)
		if err != nil {
			return 0, fmt.Errorf("prepare the memory as the controller will run it: %w", err)
		}
		defer cleanupTree()
		config.NoDetectors = detectorCount(tree) == 0
		prepared, err := probe.Prepare(ctx, tree, *operatorRoot, *namespace, deployment.HealthScope{})
		if err != nil {
			return 0, fmt.Errorf("build detectors for the live items: %w", err)
		}
		defer prepared.Close()
		lazy := &lazyWatch{prepared: prepared, ctx: ctx}
		defer lazy.Close()
		eval := detectorEvaluator{lazy: lazy}
		if *harmWindow > 0 && *quietWindow > 0 && !config.NoDetectors {
			// The probes have not started: this is the application without them.
			if err := <-pausedWindow; err != nil {
				return 0, fmt.Errorf("measure the application with the probes paused: %w", err)
			}
			config.Harm = &onboardcheck.HarmConfig{PausedStart: paused[0], PausedEnd: paused[1], MinRunning: *harmWindow}
		}
		if *quietWindow > 0 {
			config.Quiet = &onboardcheck.QuietConfig{Eval: eval, Window: *quietWindow, Interval: *quietInterval, Evaluations: *quietEvaluations}
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
			config.Break = &onboardcheck.BreakConfig{Eval: eval, Breaker: breaker, Bound: bound, Horizon: horizon, Interval: 5 * time.Second, Faults: genericFaults(m, breaker, cluster)}
		}
	}
	report, err := onboardcheck.Run(ctx, config)
	if err != nil {
		return 0, err
	}
	var rendered string
	if *asJSON {
		encoded, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return 0, err
		}
		rendered = string(encoded) + "\n"
	} else {
		rendered = report.Text()
	}
	fmt.Fprint(stdout, rendered)
	if *reportFile != "" {
		if err := os.WriteFile(*reportFile, []byte(rendered), 0o600); err != nil {
			return 0, fmt.Errorf("write --report: %w", err)
		}
	}
	if !report.NoRed() {
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

// expectedDuration is an upper bound for one check: the metric items run
// alongside the quiet window (whose cap is the window), and with --break each
// wave costs a break, a restore and the wait for detectors to clear. A quick run
// is the longer of the settle and its short smoke.
func expectedDuration(settle, quiet time.Duration, withBreak bool, bound time.Duration, waves int) time.Duration {
	total := max(settle, quiet)
	if withBreak {
		total += time.Duration(waves) * (bound + 30*time.Second)
	}
	return total
}

// startNotice is the line printed when a check starts: how long it takes and
// where the report goes.
func startNotice(quick bool, settle, quiet time.Duration, withBreak bool, bound time.Duration, waves int, reportFile string) string {
	mode := "full"
	if quick {
		mode = "quick"
	}
	where := "the report prints to stdout when the check finishes"
	if reportFile != "" {
		where += " and is also written to " + reportFile
	}
	return fmt.Sprintf("onboarding check (%s): expected at most %s (the quiet window ends early once every detector has judged enough); run it in the background and keep working; %s",
		mode, expectedDuration(settle, quiet, withBreak, bound, waves).Round(time.Second), where)
}

// genericFaults are the breaks that keep every component up: each worker with
// its egress denied, and the authoritative store with rows made wrong.
func genericFaults(m appmap.Map, breaker *kubebreak.Breaker, cluster *processio.KubeCluster) []onboardcheck.Fault {
	workers, store := onboardcheck.FaultTargets(m)
	var faults []onboardcheck.Fault
	for _, w := range workers {
		faults = append(faults, breaker.WorkerQuiet(w, onboardcheck.OperationsThrough(m, w.Name)))
	}
	if store != nil {
		faults = append(faults, breaker.DataWrong(*store, onboardcheck.OperationsThrough(m, store.Name), podExec(cluster)))
	}
	return faults
}

// podExec runs a command in a pod container and returns its standard output.
func podExec(cluster *processio.KubeCluster) kubebreak.Exec {
	exec := kubereads.SPDYExec{Client: cluster.Kube(), Config: cluster.Config()}
	return func(ctx context.Context, namespace, pod, container string, command []string, stdin string) (string, error) {
		var out strings.Builder
		err := exec.Stream(ctx, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: namespace, Name: pod}, container, command, stdin, &out)
		return out.String(), err
	}
}

// routeLister runs the map's route-list command in a running pod of the
// component's workload.
type routeLister struct {
	cluster   *processio.KubeCluster
	namespace string
}

func (r routeLister) List(ctx context.Context, c appmap.Component, list appmap.RouteList) ([]string, error) {
	kind, name, ok := strings.Cut(c.Workload, "/")
	if !ok {
		return nil, fmt.Errorf("component %s workload %q is not Kind/name", c.Name, c.Workload)
	}
	apps := r.cluster.Kube().AppsV1()
	var selector *metav1.LabelSelector
	var err error
	switch kind {
	case "Deployment":
		var d *appsv1.Deployment
		if d, err = apps.Deployments(r.namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
			selector = d.Spec.Selector
		}
	case "StatefulSet":
		var d *appsv1.StatefulSet
		if d, err = apps.StatefulSets(r.namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
			selector = d.Spec.Selector
		}
	case "DaemonSet":
		var d *appsv1.DaemonSet
		if d, err = apps.DaemonSets(r.namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
			selector = d.Spec.Selector
		}
	default:
		err = fmt.Errorf("unsupported workload kind %s", kind)
	}
	if err != nil {
		return nil, err
	}
	pods, err := r.cluster.Kube().CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(selector)})
	if err != nil {
		return nil, err
	}
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		out, err := podExec(r.cluster)(ctx, r.namespace, p.Name, list.Container, list.Command, "")
		if err != nil {
			return nil, fmt.Errorf("%w: %s", err, strings.TrimSpace(out))
		}
		return strings.Split(out, "\n"), nil
	}
	return nil, fmt.Errorf("no running pod of %s", c.Workload)
}

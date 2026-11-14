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

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/onboardcheck"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus"
)

// OnboardingUsage documents "sdo-operator onboarding".
const OnboardingUsage = `usage: sdo-operator onboarding check --repo DIR --namespace NS --prometheus URL|NS/SERVICE:PORT
                                    [--kubeconfig FILE] [--context NAME] [--requests 5]
                                    [--settle 90s] [--json]

  check  Run the metric items of the onboarding check against a running
         environment. SDO builds each operation's request and error series from
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
	report, err := onboardcheck.Run(ctx, onboardcheck.Config{
		Map: m, Commit: commit,
		Prom:     promAdapter{prometheus.Client{BaseURL: base, HTTPClient: cluster}},
		Sender:   httpSender{cluster: cluster, namespace: *namespace},
		Exposed:  exposures,
		Requests: *requests, Settle: *settle, Poll: 5 * time.Second, Wait: processio.Wait,
	})
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

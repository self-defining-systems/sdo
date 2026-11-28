package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/safefs"
)

const OnboardingInitUsage = `usage: sdo-operator onboarding init --repo DIR --namespace NS [--entry SERVICE:PORT]
                                    [--kubeconfig FILE] [--context NAME]

  List live Service ports and scaffold the map and one Go probe per journey
  in .sdo/goal.md. The entry is the sole exposed Service port, else one no other
  component calls; it names the other ports, or pass --entry. Edit routes and assertions before scheduling the probes.
  It maps the request counter (entries, observe metrics) of each Service whose pods SDO's Prometheus
  scrapes (prometheus.io annotations), reading their metrics once through the API server.
  Run before authoring operations, probes or generators. No app requests are sent.`

const initGenerator = layout.TrafficGeneratorsDir + "/generators.go"

// initCallWorkload is the call probes' workload; the map's probe lines place
// the scenarios in it, so it lists none.
const initCallWorkload = layout.TrafficWorkloadsDir + "/probe-call.yaml"

const initCallWorkloadYAML = `apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: probe-call
purpose: health-probe
timeout: 5s
iterationTimeout: 20s
slo: {window: 4, minSamples: 1, maxAge: 10m, maxErrorRate: 1, maxTimeoutRate: 1}
`

type initEntry struct {
	service string
	port    int
}

func (e initEntry) String() string { return fmt.Sprintf("%s:%d", e.service, e.port) }

func onboardingInit(ctx context.Context, args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("onboarding init", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repo := flags.String("repo", "", "application repository")
	namespace := flags.String("namespace", "", "application namespace")
	entry := flags.String("entry", "", "Service and port from the target namespace")
	kubeconfig := flags.String("kubeconfig", "", "kubeconfig file")
	kubeContext := flags.String("context", "", "kubeconfig context")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *namespace == "" || !filepath.IsAbs(*repo) {
		return errors.New("init requires --repo (absolute path) and --namespace")
	}
	cluster, err := processio.OpenKubeCluster(*kubeconfig, *kubeContext)
	if err != nil {
		return err
	}
	services, err := cluster.Kube().CoreV1().Services(*namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list Services in %s: %w", *namespace, err)
	}
	var candidates, exposed []initEntry
	for _, service := range services.Items {
		if service.Name == "kubernetes" && service.Namespace == "default" {
			continue
		}
		for _, port := range service.Spec.Ports {
			if (port.Protocol != "" && port.Protocol != corev1.ProtocolTCP) || strings.Contains(port.Name, "metrics") {
				continue
			}
			candidates = append(candidates, initEntry{service.Name, int(port.Port)})
		}
	}
	if *entry == "" && len(candidates) > 1 {
		// Port discovery needs only the Service list. Exposure is a hint
		// when the caller also has permission to list Ingresses and routes.
		if entries, err := cluster.ExposedServices(ctx, *namespace); err == nil {
			for _, candidate := range candidates {
				if slices.ContainsFunc(entries, func(entry processio.ExposedService) bool { return entry.Service == candidate.service }) {
					exposed = append(exposed, candidate)
				}
			}
		}
		if len(exposed) == 1 {
			*entry = exposed[0].String()
		}
	}
	scrape := func(service string) ([]byte, string) {
		return scrapeServiceMetrics(ctx, cluster.Kube(), *namespace, service)
	}
	note, err := writeOnboardingScaffold(*repo, candidates, *entry, scrape)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Scaffolded .sdo/arch.md and %s with %s. Fill in routes and assertions, then try one probe.\n", initGenerator, note)
	return nil
}

// selectInitEntry picks the scaffold's entry: the requested one, the sole
// candidate, or else the candidates whose component no other component calls
// and that hold no data (the seeded map's entry candidates), first by name.
// others names the rest, so the author can move an operation's entry.
func selectInitEntry(candidates []initEntry, requested string, m appmap.Map) (entry initEntry, others string, err error) {
	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if requested != "" && candidate.String() == requested {
			return candidate, "", nil
		}
		names = append(names, candidate.String())
	}
	slices.Sort(names)
	if len(names) == 0 {
		return initEntry{}, "", errors.New("no non-metrics TCP Service ports found; check --namespace")
	}
	if requested != "" {
		return initEntry{}, "", fmt.Errorf("--entry %s is not a Service port; choose from %s", requested, strings.Join(names, ", "))
	}
	called, stores, owner := map[string]bool{}, map[string]bool{}, map[string]string{}
	for _, edge := range m.Edges {
		called[edge.To] = true
	}
	for _, component := range m.Components {
		for _, service := range component.Services {
			owner[service] = component.Name
		}
		stores[component.Name] = component.HoldsData || component.Data != nil
	}
	pool := slices.Clone(candidates)
	if entries := slices.DeleteFunc(slices.Clone(candidates), func(c initEntry) bool {
		name, known := owner[c.service]
		return !known || called[name] || stores[name]
	}); len(entries) > 0 {
		pool = entries
	}
	slices.SortFunc(pool, func(a, b initEntry) int { return strings.Compare(a.String(), b.String()) })
	entry = pool[0]
	var rest []string
	for _, name := range names {
		if name != entry.String() {
			rest = append(rest, name)
		}
	}
	return entry, strings.Join(rest, ", "), nil
}

// writeOnboardingScaffold writes the map's operations and probes and one Go
// stub per goal journey, and says which entry it chose.
func writeOnboardingScaffold(repo string, candidates []initEntry, requested string, scrape metricsScraper) (string, error) {
	journeys, err := goalJourneys(repo)
	if err != nil {
		return "", err
	}
	if len(journeys) == 0 {
		return "", errors.New(".sdo/goal.md lists no journeys")
	}
	const archPath = ".sdo/arch.md"
	arch, err := os.ReadFile(filepath.Join(repo, archPath))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	region, found, err := appmap.Find(string(arch))
	if err != nil {
		return "", err
	}
	m := appmap.Map{Version: appmap.Version}
	if found {
		m, err = appmap.Parse(region.YAML)
		if err != nil {
			return "", err
		}
	}
	if len(m.Operations) != 0 || len(m.Probes) != 0 {
		return "", errors.New("the application map already has operations or probes; continue editing it directly")
	}
	files, err := os.ReadDir(filepath.Join(repo, layout.TrafficGeneratorsDir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".go") {
			return "", errors.New("the checkout already has traffic generators; continue editing them directly")
		}
	}

	entry, others, err := selectInitEntry(candidates, requested, m)
	if err != nil {
		return "", err
	}
	var callPath []string
	for _, component := range m.Components {
		if slices.Contains(component.Services, entry.service) {
			callPath = []string{component.Name}
			break
		}
	}
	var source strings.Builder
	source.WriteString("package generators\n\nimport \"sdo.dev/operator/sdk/traffic\"\n\nfunc Scenarios() traffic.Catalog {\nreturn traffic.Catalog{\n")
	for i, journey := range journeys {
		id := fmt.Sprintf("journey-%d", i+1)
		m.Operations = append(m.Operations, appmap.Operation{
			ID: id, Journey: journey.ID, Path: callPath,
			Entry: appmap.Entry{Service: entry.service, Port: entry.port},
		})
		m.Probes = append(m.Probes, appmap.Probe{Scenario: id, Kind: appmap.KindCall, Operations: []string{id}})
		fmt.Fprintf(&source, `{
ID: %q,
Description: %q,
Target: traffic.Target{Service: %q, Port: %d},
SideEffect: traffic.SideEffectRead,
// TODO: Set dependencies and side effect. Replace this endpoint with the journey's real steps.
Steps: []traffic.Step{{Name: "todo", Endpoint: traffic.GET("/{replace-with-journey-route}", nil)}},
},
`, id, journey.What, entry.service, entry.port)
	}
	source.WriteString("}\n}\n")
	generator, err := format.Source([]byte(source.String()))
	if err != nil {
		return "", err
	}
	mapped := mapRequestMetrics(&m, scrape)
	header := ""
	if found {
		header = appmap.Header(region.YAML)
	}
	body, err := appmap.Render(m, header)
	if err != nil {
		return "", err
	}
	block := appmap.BeginMarker + "\n```yaml\n" + body + "```\n" + appmap.EndMarker + "\n"
	document := strings.TrimRight(string(arch), "\n") + "\n\n" + block
	if found {
		document = strings.Replace(string(arch), region.Block, block, 1)
	} else if len(arch) == 0 {
		document = block
	}
	if err := safefs.CreateNewFile(repo, initGenerator, generator, 0o644); err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(repo, initCallWorkload)); errors.Is(err, fs.ErrNotExist) {
		if err := safefs.CreateNewFile(repo, initCallWorkload, []byte(initCallWorkloadYAML), 0o644); err != nil {
			return "", errors.Join(err, safefs.Remove(repo, initGenerator))
		}
	}
	if err := safefs.WriteFile(repo, archPath, []byte(document), 0o644); err != nil {
		return "", errors.Join(err, safefs.Remove(repo, initGenerator))
	}
	note := "entry " + entry.String()
	if len(mapped) > 0 {
		note += "; request metrics mapped: " + strings.Join(mapped, ", ")
	}
	if others != "" {
		note += "; other Service ports: " + others + " (move an operation's entry if one fits better)"
	}
	return note, nil
}

// metricsScraper returns the Prometheus exposition one of a Service's pods
// serves where SDO's Prometheus scrapes it, and that place in a few words;
// nil when none does.
type metricsScraper func(service string) (exposition []byte, where string)

// mapRequestMetrics adds an entries record (appmap.RequestCounter) and an
// observe metrics pointer for each component Service whose scraped metrics
// carry a request counter, and returns the Services mapped. A counter name two
// Services share needs a selector only the author knows, so neither is mapped.
func mapRequestMetrics(m *appmap.Map, scrape metricsScraper) []string {
	if scrape == nil {
		return nil
	}
	type found struct {
		component int
		entry     appmap.MetricEntry
		where     string
	}
	var all []found
	names := map[string]int{}
	for i, component := range m.Components {
		for _, service := range component.Services {
			if _, has := m.EntryFor(service); has {
				continue
			}
			exposition, where := scrape(service)
			if entry, ok := appmap.RequestCounter(service, exposition); ok {
				all = append(all, found{i, entry, where})
				names[entry.Metric]++
			}
		}
	}
	var mapped []string
	for _, f := range all {
		if names[f.entry.Metric] > 1 {
			continue
		}
		m.Entries = append(m.Entries, f.entry)
		component := &m.Components[f.component]
		if component.Observe == nil {
			component.Observe = &appmap.Observe{}
		}
		if component.Observe.Metrics == "" {
			component.Observe.Metrics = fmt.Sprintf("%s, counter %s", f.where, f.entry.Metric)
		}
		mapped = append(mapped, f.entry.Service)
	}
	return mapped
}

// scrapeTimeout bounds one metrics read through the API server.
const scrapeTimeout = 5 * time.Second

// scrapeServiceMetrics reads, through the API server's pod proxy, the
// metrics of the first running pod of the Service that SDO's Prometheus
// scrapes (annotations prometheus.io/scrape, port and path).
func scrapeServiceMetrics(ctx context.Context, client kubernetes.Interface, namespace, service string) ([]byte, string) {
	svc, err := client.CoreV1().Services(namespace).Get(ctx, service, metav1.GetOptions{})
	if err != nil || len(svc.Spec.Selector) == 0 {
		return nil, ""
	}
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(svc.Spec.Selector).String()})
	if err != nil {
		return nil, ""
	}
	for _, pod := range pods.Items {
		annotations := pod.Annotations
		port := annotations["prometheus.io/port"]
		if pod.Status.Phase != corev1.PodRunning || annotations["prometheus.io/scrape"] != "true" || port == "" {
			continue
		}
		path := annotations["prometheus.io/path"]
		if !strings.HasPrefix(path, "/") {
			path = "/metrics"
		}
		read, cancel := context.WithTimeout(ctx, scrapeTimeout)
		body, err := client.CoreV1().Pods(namespace).ProxyGet("http", pod.Name, port, path, nil).DoRaw(read)
		cancel()
		if err == nil {
			return body, fmt.Sprintf("pod :%s%s", port, path)
		}
	}
	return nil, ""
}

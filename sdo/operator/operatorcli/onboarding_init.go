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

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/safefs"
)

const OnboardingInitUsage = `usage: sdo-operator onboarding init --repo DIR --namespace NS [--entry SERVICE:PORT]
                                    [--kubeconfig FILE] [--context NAME]

  List live Service ports and scaffold the map and one Go probe per journey
  in .sdo/goal.md. Prefer a sole exposed entry, or pass --entry when several
  ports are plausible. Edit routes and assertions before scheduling the probes.
  Run before authoring operations, probes or generators. No app requests are sent.`

const initGenerator = layout.TrafficGeneratorsDir + "/generators.go"

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
	if err := writeOnboardingScaffold(*repo, candidates, *entry); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Scaffolded .sdo/arch.md and %s. Fill in routes and assertions, then schedule one probe.\n", initGenerator)
	return nil
}

func selectInitEntry(candidates []initEntry, requested string) (initEntry, error) {
	if requested == "" && len(candidates) == 1 {
		return candidates[0], nil
	}
	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.String() == requested {
			return candidate, nil
		}
		names = append(names, candidate.String())
	}
	if len(names) == 0 {
		return initEntry{}, errors.New("no non-metrics TCP Service ports found; check --namespace")
	}
	slices.Sort(names)
	return initEntry{}, fmt.Errorf("choose --entry SERVICE:PORT from %s", strings.Join(names, ", "))
}

func writeOnboardingScaffold(repo string, candidates []initEntry, requested string) error {
	entry, err := selectInitEntry(candidates, requested)
	if err != nil {
		return err
	}
	journeys, err := goalJourneys(repo)
	if err != nil {
		return err
	}
	if len(journeys) == 0 {
		return errors.New(".sdo/goal.md lists no journeys")
	}
	const archPath = ".sdo/arch.md"
	arch, err := os.ReadFile(filepath.Join(repo, archPath))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	region, found, err := appmap.Find(string(arch))
	if err != nil {
		return err
	}
	m := appmap.Map{Version: appmap.Version}
	if found {
		m, err = appmap.Parse(region.YAML)
		if err != nil {
			return err
		}
	}
	if len(m.Operations) != 0 || len(m.Probes) != 0 {
		return errors.New("the application map already has operations or probes; continue editing it directly")
	}
	files, err := os.ReadDir(filepath.Join(repo, layout.TrafficGeneratorsDir))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".go") {
			return errors.New("the checkout already has traffic generators; continue editing them directly")
		}
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
		return err
	}
	body, err := yaml.Marshal(m)
	if err != nil {
		return err
	}
	block := appmap.BeginMarker + "\n```yaml\n" + string(body) + "```\n" + appmap.EndMarker + "\n"
	document := strings.TrimRight(string(arch), "\n") + "\n\n" + block
	if found {
		document = strings.Replace(string(arch), region.Block, block, 1)
	} else if len(arch) == 0 {
		document = block
	}
	if err := safefs.CreateNewFile(repo, initGenerator, generator, 0o644); err != nil {
		return err
	}
	if err := safefs.WriteFile(repo, archPath, []byte(document), 0o644); err != nil {
		return errors.Join(err, safefs.Remove(repo, initGenerator))
	}
	return nil
}

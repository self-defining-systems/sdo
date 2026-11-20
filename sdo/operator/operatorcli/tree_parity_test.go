package operatorcli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/telemetry"
)

const parityWorkload = "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: probe-read\npurpose: health-probe\nratePerSecond: 0.2\nscenarios:\n  - {id: a}\n"

func parityRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	workloads := filepath.Join(repo, ".sdo", "diagnostics", "traffic", "workloads")
	if err := os.MkdirAll(workloads, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workloads, "probe-read.yaml"), []byte(parityWorkload), 0o644); err != nil {
		t.Fatal(err)
	}
	archive := "# Architecture\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents: []\nedges: []\noperations:\n  - {id: list-items, entry: {service: web, port: 80, method: GET, path: /items}, route: \"GET /items\", path: [web]}\nentries:\n  - {service: web, metric: http_requests_total, selector: {job: web}, labels: {route: route, method: method, status: code}, errors: \"5..\"}\nprobes: []\n```\n<!-- sdo:application-map:end -->\n"
	if err := os.WriteFile(filepath.Join(repo, ".sdo", "arch.md"), []byte(archive), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x"}} {
		if _, err := processio.Git(context.Background(), append([]string{"-C", repo}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	return repo
}

func parityEnvironment(t *testing.T) (telemetry.Environment, string) {
	t.Helper()
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion, ApplicationNamespace: "shop", ControlNamespace: "sdo-ctl", Recipes: []telemetry.Recipe{{Name: "prometheus"}},
		Provided: []telemetry.Binding{{ID: "prom", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Default: true, Endpoint: telemetry.Endpoint{URL: "http://sdo-prometheus.sdo-ctl.svc:9090"}}}}
	data, err := env.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "environment.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return env, path
}

func detectorIDs(t *testing.T, root string) []string {
	t.Helper()
	var ids []string
	_ = filepath.WalkDir(filepath.Join(root, ".sdo", "diagnostics", "detectors"), func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "manifest.yaml" {
			ids = append(ids, filepath.Base(filepath.Dir(path)))
		}
		return nil
	})
	slices.Sort(ids)
	return ids
}

// Memory check, the onboarding check, publication and install all prepare the
// tree from the same environment, so they install the same detector set, and
// request metrics and metrics alerts are in it when a range source is provided.
func TestTreePreparationIsTheSameOnEveryPath(t *testing.T) {
	env, envPath := parityEnvironment(t)
	registry := telemetryRegistry()
	// Onboarding check.
	checkTree, cleanup, err := preparedTree(context.Background(), parityRepo(t), envPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	want := detectorIDs(t, checkTree)
	for _, id := range []string{"request-metrics", "metrics-alerts", "traffic-probe-read"} {
		if !slices.Contains(want, id) {
			t.Fatalf("onboarding check tree lacks %s: %v", id, want)
		}
	}

	// Publication and memory check: the policy the broker holds, carried to
	// the agent's check as JSON.
	brokerTelemetry, err := environmentTelemetry(env, registry)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(brokerTelemetry.Policy)
	if err != nil {
		t.Fatal(err)
	}
	checkPolicy, err := memoryTelemetryPolicy(func(string) string { return string(encoded) }, "shop")
	if err != nil {
		t.Fatal(err)
	}
	for name, policy := range map[string]telemetry.Policy{"publication": brokerTelemetry.Policy, "memory check": checkPolicy} {
		root := parityRepo(t)
		if err := validate.PrepareHealthTree(root, policy, registry); err != nil {
			t.Fatal(err)
		}
		if got := detectorIDs(t, root); !slices.Equal(got, want) {
			t.Fatalf("%s detectors = %v, want %v", name, got, want)
		}
	}

	// Install.
	repo := parityRepo(t)
	if _, err := prepareInstallTree(context.Background(), repo, brokerTelemetry); err != nil {
		t.Fatal(err)
	}
	if got := detectorIDs(t, repo); !slices.Equal(got, want) {
		t.Fatalf("install detectors = %v, want %v", got, want)
	}
}

// An onboarding memory that was never published through the broker reaches the
// controller with its detectors; preparing twice adds no commit.
func TestInstallPreparesAnUnpreparedMemoryOnce(t *testing.T) {
	env, _ := parityEnvironment(t)
	derived, err := environmentTelemetry(env, telemetryRegistry())
	if err != nil {
		t.Fatal(err)
	}
	repo := parityRepo(t)
	first, err := prepareInstallTree(context.Background(), repo, derived)
	if err != nil {
		t.Fatal(err)
	}
	if detectorCount(repo) == 0 {
		t.Fatal("an unprepared onboarding memory has no detectors after install preparation")
	}
	body, err := processio.Git(context.Background(), "-C", repo, "log", "-1", "--format=%B")
	if err != nil || strings.Contains(body, "SDO-Validation") || !strings.Contains(body, "SDO-Origin: install-existing") {
		t.Fatalf("the generic-detector commit lacks SDO provenance: %q (%v)", body, err)
	}
	head, err := acceptedHead(context.Background(), repo)
	if err != nil || head != first {
		t.Fatalf("head = %q, %v; prepare returned %q", head, err, first)
	}
	second, err := prepareInstallTree(context.Background(), repo, derived)
	if err != nil || second != first {
		t.Fatalf("a prepared tree was committed again: %q then %q (%v)", first, second, err)
	}
}

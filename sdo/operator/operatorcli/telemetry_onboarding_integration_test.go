//go:build integration

package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/install"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
)

// onboardingTestRegistry is the operator's registry plus a metrics adapter
// that serves no alerts, so a profile can provide a source without them (the
// built-in Prometheus adapter always advertises alerts).
func onboardingTestRegistry() *telemetry.Registry {
	registry := telemetrysources.Registry()
	err := telemetry.Register(registry, telemetry.Adapter[struct{}]{
		Name: "plain-metrics", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Dialect: "synthetic metrics", Caps: []telemetry.Capability{telemetry.CapRange},
		Validate: func(struct{}, telemetry.Endpoint) error { return nil },
		Open:     func(struct{}, telemetry.Endpoint, telemetry.Env) (telemetry.Client, error) { return nil, nil },
	})
	if err != nil {
		panic(err)
	}
	return registry
}

// TestMain runs this test binary as the broker's validation subprocess when
// invoked as one, exactly as the operator's main does.
func TestMain(m *testing.M) {
	broker.RunValidationChild(onboardingTestRegistry())
	os.Exit(m.Run())
}

var assuranceSeeds = filepath.Join("..", "..", "..", "benchmarks", "sregym", "experiments", "assurance", "seeds")

// firstAssuranceSeed is a checked-in judge-accepted memory: the first seed
// directory holding an operational_memory tree.
func firstAssuranceSeed(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(assuranceSeeds)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		root := filepath.Join(assuranceSeeds, entry.Name(), "operational_memory")
		if _, err := os.Stat(root); err == nil {
			return root
		}
	}
	t.Fatal("no assurance seed holds an operational_memory tree")
	return ""
}

// The onboarding invariant behind the live onboarding failures: for every
// built-in telemetry profile, and a profile that provides a source without
// alerts, the health judge's proposal (its detectors and probes, plus Go's
// generic bootstrap) is accepted by the exact broker the lifecycle judge path
// uses, through its real validation subprocess, with no telemetry diagnostic;
// and a valid judge-authored binding is accepted the same way. Before, the
// subprocess ran without the policy and registry: the Go-installed alerts
// detector was unbound and any binding failed "no adapter registry".
// Each case builds and tests the diagnostics module (about 20 s), so the
// test runs in the nightly tier.
func TestOnboardingBrokerAcceptsTheJudgesProposalUnderEveryProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles the diagnostics module once per profile (about 15 s each); nightly CI runs it without -short")
	}
	onboardingSeed := firstAssuranceSeed(t)
	plain := telemetry.Profile{Name: "plain", Mode: telemetry.ModeOpen, Sources: []telemetry.ProfileSource{{
		ID: "plain", Adapter: "plain-metrics", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: "http://metrics.sdo-ctl.svc:9090", Default: true,
	}}}
	designerSource := "version: 1\nsources:\n  - id: app-metrics\n    kinds: [metrics]\n    adapter: prometheus\n    endpoint: http://app-prometheus.shop.svc:9090\n"
	for _, tc := range []struct {
		name    string
		profile telemetry.Profile
		sources string
		alerts  bool
	}{
		{name: "none", profile: mustBuiltin(t, telemetrysources.NoneProfileName, "sdo-ctl")},
		{name: "prometheus-default", profile: mustBuiltin(t, telemetrysources.DefaultProfileName, "sdo-ctl"), alerts: true},
		{name: "provided source without alerts", profile: plain},
		{name: "prometheus-default with a judge-authored binding", profile: mustBuiltin(t, telemetrysources.DefaultProfileName, "sdo-ctl"), sources: designerSource, alerts: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			root := filepath.Join(t.TempDir(), "accepted")
			judged := t.TempDir()
			copyDir(t, onboardingSeed, filepath.Join(root, layout.Dir))
			for _, rel := range []string{layout.DiagnosticsDir, layout.PlaybooksDir} {
				if err := os.MkdirAll(filepath.Dir(filepath.Join(judged, rel)), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(root, rel), filepath.Join(judged, rel)); err != nil {
					t.Fatal(err)
				}
			}
			goMod := filepath.Join(root, layout.DiagnosticsGoModFile)
			if err := os.MkdirAll(filepath.Dir(goMod), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(filepath.Join(judged, layout.DiagnosticsGoModFile), goMod); err != nil {
				t.Fatal(err)
			}
			testutil.InitGit(t, root)
			testutil.Git(t, root, "add", ".")
			testutil.Git(t, root, "commit", "-qm", "human goal, deployer architecture, and diagnostics module")

			operatorRoot, err := filepath.Abs("..")
			if err != nil {
				t.Fatal(err)
			}
			config := OperateConfig{
				ApplicationRoot: root, WorktreeRoot: filepath.Join(t.TempDir(), "workspaces"),
				Install:   install.Config{TelemetryProfile: tc.profile, BackendRecipes: telemetrysources.Recipes(), Replay: "off"},
				Lifecycle: lifecycle.Config{Namespace: "shop"},
			}
			repository, err := onboardingBroker(config, "sdo-ctl", (broker.SubprocessValidator{OperatorRoot: operatorRoot}).Validator(), sdk.ObservationScope{}, onboardingTestRegistry())
			if err != nil {
				t.Fatal(err)
			}
			proposal, err := repository.CreateProposalForPhase(ctx, "judge", broker.InitialPhase)
			if err != nil {
				t.Fatal(err)
			}
			copyDir(t, judged, proposal.Path)
			if tc.sources != "" {
				path := filepath.Join(proposal.Path, layout.TelemetrySourcesFile)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(tc.sources), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			candidate, err := repository.StageCandidate(ctx, "judge", broker.HealthJudge, broker.InitialPhase)
			if err != nil {
				diagnostics, _ := broker.RejectionDiagnostics(err)
				var lines []string
				for _, d := range diagnostics {
					lines = append(lines, d.GetRule()+" "+d.GetPath()+": "+d.GetMessage())
				}
				t.Fatalf("the judge's proposal was rejected: %v\n%s", err, strings.Join(lines, "\n"))
			}
			alerts := testutil.Git(t, candidate.Path, "ls-tree", "-r", "--name-only", candidate.Commit, "--", layout.DetectorDir(layout.ClassHealth, memory.MetricsAlertsDetectorID))
			if installed := strings.TrimSpace(alerts) != ""; installed != tc.alerts {
				t.Fatalf("alerts detector installed = %v, want %v (only where an effective source serves alerts)", installed, tc.alerts)
			}
		})
	}
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	if err := os.MkdirAll(to, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", from+"/.", to).CombinedOutput(); err != nil {
		t.Fatalf("copy %s: %v: %s", from, err, out)
	}
}

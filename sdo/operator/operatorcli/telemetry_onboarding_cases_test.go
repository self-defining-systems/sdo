package operatorcli

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/contracts/settings"
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

// runOnboardingCases stages the health judge's proposal under every profile on
// the exact broker the lifecycle judge path builds, with validator as its
// compile validator: the real validation subprocess (nightly) or a stub that
// resolves telemetry the way the subprocess would without compiling (PR tier).
func runOnboardingCases(t *testing.T, validator broker.Validator) {
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

			config := OperateConfig{
				ApplicationRoot: root, WorktreeRoot: filepath.Join(t.TempDir(), "workspaces"),
				Install:   install.Config{TelemetryProfile: tc.profile, BackendRecipes: telemetrysources.Recipes(), Settings: replayOff()},
				Lifecycle: lifecycle.Config{Namespace: "shop"},
			}
			repository, err := onboardingBroker(config, "sdo-ctl", validator, sdk.ObservationScope{}, onboardingTestRegistry())
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
	err := filepath.WalkDir(from, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
	if err != nil {
		t.Fatalf("copy %s: %v", from, err)
	}
}

func replayOff() *settings.Config {
	cfg := settings.Default()
	cfg.Agent.Model, cfg.Learning.Replay = "m", "off"
	return &cfg
}

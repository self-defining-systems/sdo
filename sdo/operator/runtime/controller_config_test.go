package runtime

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Stage 3 of the proto single-source-of-truth track
// (seam 3): the launcher populates one typed
// ControllerConfig instead of hand-mirroring the controller's argv flags. These
// tests fence the two halves of that seam — protojson load + protovalidate, and
// the single config->flags expansion — so a launcher that sets a field the
// controller does not define fails loud at the contract, not ten minutes into a
// run with "unrecognized arguments" (the --closeout-state-gate drift class).

func fullControllerConfig() *contractsv1alpha1.ControllerConfig {
	syntheticTraffic := false
	cleanHelpers := false
	stateBaseline := false
	return &contractsv1alpha1.ControllerConfig{
		Namespace:                  "hotel-reservation",
		ControlNamespace:           "sdo-system",
		AppRoot:                    "/workspace/app",
		Application:                "hotel-reservation",
		SourceCommit:               "abc123",
		DeployedCommit:             "def456",
		DeployedManifestPaths:      []string{"kubernetes"},
		Dispatcher:                 "/usr/local/bin/responder",
		DispatcherMode:             "job",
		DispatcherArgs:             []string{"--model", "luna"},
		ResponderImage:             "registry/responder:latest",
		AgentRegistrationDirectory: "/etc/sdo/agents",
		ResponderAgentId:           "custom-responder",
		RepositoryPvc:              "app-repo",
		RepositoryMountPath:        "/workspace",
		RepositoryPvcSubpath:       "repo",
		ResponderCredentialsSecret: "codex-credentials",
		ResponderEnv:               []string{"FOO=bar", "BAZ=qux"},
		BrokerWorktreeRoot:         "/worktrees",
		FiringTelemetryPath:        "/telemetry/firings.jsonl",
		Duration:                   durationpb.New(0),
		ResponseTimeout:            durationpb.New(1800000000000), // 30m
		VerificationTimeout:        durationpb.New(120000000000),  // 2m
		MaxFollowUps:               3,
		MaxResponseLoops:           4,
		ProgramFastPath:            true,
		ProgramTrustThreshold:      3,
		FollowUpCooldown:           durationpb.New(30000000000), // 30s
		RepairPolicy:               "recorded-actions",
		LeaseName:                  "sdo-controller",
		LeaseDuration:              durationpb.New(60000000000), // 60s
		Identity:                   "controller-0",
		ExitAfterClosure:           true,
		RestartAfterClosure:        false,
		MaintenanceConfigmap:       "sdo-maintenance",
		MaintenancePollInterval:    durationpb.New(1000000000), // 1s
		ResumeSyncTimeout:          durationpb.New(60000000000),
		SyntheticTraffic:           &syntheticTraffic,
		SyntheticTrafficWarmup:     durationpb.New(5000000000),
		ProberBinary:               "/workspace/prober",
		ProberImage:                "registry/prober:latest",
		ProberUrl:                  "http://prober:8080",
		CleanResponderHelpers:      &cleanHelpers,
		StateBaseline:              &stateBaseline,
	}
}

func TestLoadControllerConfigRoundTrips(t *testing.T) {
	config := fullControllerConfig()
	raw, err := protojson.Marshal(config)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	loaded, err := LoadControllerConfig(path)
	if err != nil {
		t.Fatalf("LoadControllerConfig: %v", err)
	}
	if !proto.Equal(config, loaded) {
		t.Fatalf("config did not round-trip:\n got: %v\nwant: %v", loaded, config)
	}
}

func TestLoadControllerConfigRejectsInvalid(t *testing.T) {
	cases := map[string]func(*contractsv1alpha1.ControllerConfig){
		"empty namespace":     func(c *contractsv1alpha1.ControllerConfig) { c.Namespace = "" },
		"empty app_root":      func(c *contractsv1alpha1.ControllerConfig) { c.AppRoot = "" },
		"empty dispatcher":    func(c *contractsv1alpha1.ControllerConfig) { c.Dispatcher = "" },
		"bad dispatcher_mode": func(c *contractsv1alpha1.ControllerConfig) { c.DispatcherMode = "cluster" },
		"bad repair_policy":   func(c *contractsv1alpha1.ControllerConfig) { c.RepairPolicy = "yolo" },
		"negative follow_ups": func(c *contractsv1alpha1.ControllerConfig) { c.MaxFollowUps = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			config := fullControllerConfig()
			mutate(config)
			raw, err := protojson.Marshal(config)
			if err != nil {
				t.Fatalf("marshal config: %v", err)
			}
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, raw, 0o644); err != nil {
				t.Fatalf("write config: %v", err)
			}
			if _, err := LoadControllerConfig(path); err == nil {
				t.Fatalf("expected %s to be rejected by protovalidate", name)
			}
		})
	}
}

// assertFlagsDefined is the drift guard: it feeds the generated args straight
// into the controller's real entry point (RunWithOptions, whose flag set is the
// one source of flag semantics) and asserts the flags are all recognized. A
// flag the mapping emits that run.go does not define surfaces here as the Go
// flag package's "flag provided but not defined" error rather than as a pod
// crash in production.
//
// We never run the controller: we append the two mutually-exclusive closure
// flags, so once flag parsing accepts every generated flag the controller
// returns immediately at the exit/restart conflict check — before any git or
// kube work. If instead the mapping emitted an undefined flag, flag parsing
// fails first with "flag provided but not defined", which is what we assert
// against.
func assertFlagsDefined(t *testing.T, args []string) {
	t.Helper()
	guarded := append(append([]string{}, args...), "--exit-after-closure", "--restart-after-closure")
	var stderr bytes.Buffer
	err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: guarded, Stderr: &stderr})
	if err == nil {
		t.Fatalf("expected the closure-conflict early return, got nil\nargs: %v", guarded)
	}
	if strings.Contains(err.Error(), "flag provided but not defined") ||
		strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("mapping emitted a flag run.go does not define: %v\nstderr: %s\nargs: %v", err, stderr.String(), guarded)
	}
}

func TestControllerConfigFlagsAreAllDefined(t *testing.T) {
	assertFlagsDefined(t, controllerConfigToArgs(fullControllerConfig()))

	minimal := &contractsv1alpha1.ControllerConfig{
		Namespace:      "hotel-reservation",
		AppRoot:        "/workspace/app",
		Dispatcher:     "/usr/local/bin/responder",
		DispatcherMode: "local",
		RepairPolicy:   "commit",
	}
	assertFlagsDefined(t, controllerConfigToArgs(minimal))
}

// TestControllerConfigExpandsValues checks the per-field expansion forms: set
// values appear with the right flag, optional bools set false emit
// `--flag=false` (so they survive the controller's true defaults), repeated
// fields expand once per element, and durations use Go duration syntax.
func TestControllerConfigExpandsValues(t *testing.T) {
	args := controllerConfigToArgs(fullControllerConfig())
	pairs := argPairs(args)

	wantValue := map[string]string{
		"--namespace":          "hotel-reservation",
		"--control-namespace":  "sdo-system",
		"--app-root":           "/workspace/app",
		"--dispatcher-mode":    "job",
		"--repair-policy":      "recorded-actions",
		"--max-follow-ups":     "3",
		"--max-response-loops": "4",
		"--program-trust-threshold": "3",
		"--response-timeout":   "30m0s",
		"--lease-duration":     "1m0s",
	}
	for flag, want := range wantValue {
		if got := pairs[flag]; got != want {
			t.Fatalf("%s: got %q want %q\nargs: %v", flag, got, want, args)
		}
	}

	// Optional bools set false must be emitted as `--flag=false`.
	for _, flag := range []string{"--synthetic-traffic=false", "--clean-responder-helpers=false", "--state-baseline=false"} {
		if !contains(args, flag) {
			t.Fatalf("expected %q in args: %v", flag, args)
		}
	}
	// exit-after-closure true is a bare switch.
	if !contains(args, "--exit-after-closure") {
		t.Fatalf("expected --exit-after-closure in args: %v", args)
	}
	if !contains(args, "--program-fast-path") {
		t.Fatalf("expected --program-fast-path in args: %v", args)
	}
	// Repeated fields expand once per element as --flag=value.
	for _, flag := range []string{"--dispatcher-arg=--model", "--dispatcher-arg=luna", "--responder-env=FOO=bar", "--responder-env=BAZ=qux", "--deployed-manifest-path=kubernetes"} {
		if !contains(args, flag) {
			t.Fatalf("expected %q in args: %v", flag, args)
		}
	}
}

// TestControllerConfigOmitsUnsetFields proves the non-zero controller defaults
// survive a config that does not set them: an unset optional bool or duration
// emits no flag, so the controller's own flag default applies.
func TestControllerConfigOmitsUnsetFields(t *testing.T) {
	minimal := &contractsv1alpha1.ControllerConfig{
		Namespace:      "hotel-reservation",
		AppRoot:        "/workspace/app",
		Dispatcher:     "/usr/local/bin/responder",
		DispatcherMode: "local",
		RepairPolicy:   "commit",
	}
	for _, arg := range controllerConfigToArgs(minimal) {
		if strings.HasPrefix(arg, "--synthetic-traffic") ||
			strings.HasPrefix(arg, "--clean-responder-helpers") ||
			strings.HasPrefix(arg, "--state-baseline") ||
			strings.HasPrefix(arg, "--response-timeout") ||
			strings.HasPrefix(arg, "--verification-timeout") ||
			strings.HasPrefix(arg, "--lease-duration") {
			t.Fatalf("unset field must not be emitted, got %q", arg)
		}
	}
}

// TestLoadControllerConfigRejectsUnknownField is the regression fixture for the
// --closeout-state-gate drift bug: the launcher handed the controller a flag
// (--closeout-state-gate) the controller binary did not define, so every
// controller pod died with "unrecognized arguments" minutes into a run. Under
// seam 3 the launcher no longer names flags; it populates one ControllerConfig.
// A field the launcher writes that the controller does not define is now an
// unknown field in the mounted protojson, which LoadControllerConfig rejects at
// startup (protojson is strict by default), loud and immediate instead of a
// late crash. This locks that failure mode shut.
func TestLoadControllerConfigRejectsUnknownField(t *testing.T) {
	raw := []byte(`{
		"namespace": "hotel-reservation",
		"app_root": "/workspace/app",
		"dispatcher": "/usr/local/bin/responder",
		"dispatcher_mode": "job",
		"repair_policy": "commit",
		"closeout_state_gate": true
	}`)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadControllerConfig(path)
	if err == nil {
		t.Fatal("expected an unknown config field to be rejected at startup")
	}
	if !strings.Contains(err.Error(), "closeout_state_gate") {
		t.Fatalf("error should name the unknown field, got: %v", err)
	}
}

func contains(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

// argPairs collapses the `--flag value` pairs of the generated args into a map
// for value assertions. It only interprets the space-separated form the str/dur
// helpers emit; the `--flag=value` forms (repeated, optional bool) are checked
// directly against the slice.
func argPairs(args []string) map[string]string {
	pairs := map[string]string{}
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "--") && !strings.Contains(args[i], "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
			pairs[args[i]] = args[i+1]
			i++
		}
	}
	return pairs
}

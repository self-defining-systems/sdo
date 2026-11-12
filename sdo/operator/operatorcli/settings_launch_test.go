package operatorcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
)

func configFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "overlay.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A file's values reach the install and are carried as the resolved config; the environment overrides the file; a
// flag that is explicitly set overrides both, so the pre-config flags keep working unchanged.
func TestHostResolvesDefaultsThenFilesThenEnvironmentThenExplicitFlags(t *testing.T) {
	file := configFile(t, "response:\n  timeout: 20m\n  max_loops: 4\nmode: frozen\nlearning:\n  healthy_baseline: true\n")
	launch, err := ParseHostLaunchEnv(hostArgs("--config", file), []string{"SDO_RESPONSE_MAX_LOOPS=6"})
	if err != nil {
		t.Fatal(err)
	}
	got := launch.Install
	if got.TimeoutSeconds != 1200 || got.MaxResponseLoops != 6 || !got.FrozenMemory || !got.HealthyBaseline {
		t.Fatalf("file and environment did not reach the install: %+v", got)
	}
	if got.Settings == nil || time.Duration(got.Settings.Response.Timeout) != 20*time.Minute || got.Settings.Response.MaxLoops != 6 {
		t.Fatalf("the resolved config is not carried: %+v", got.Settings)
	}
	if got.Settings.Agent.Model != "m" {
		t.Fatalf("an explicit --model overlays the config: %q", got.Settings.Agent.Model)
	}
	flagged, err := ParseHostLaunchEnv(hostArgs("--config", file, "--response-timeout", "7m", "--frozen-memory=false"), []string{"SDO_RESPONSE_TIMEOUT=9m"})
	if err != nil {
		t.Fatal(err)
	}
	if flagged.Install.TimeoutSeconds != 420 || flagged.Install.FrozenMemory || flagged.Install.Settings.Mode != settings.ModeOperate {
		t.Fatalf("explicit flags must win over file and environment: %+v", flagged.Install.Settings)
	}
}

func TestHostWithoutConfigKeepsTheProductionDefaults(t *testing.T) {
	launch, err := ParseHostLaunchEnv(hostArgs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := settings.Default()
	want.Agent.Model = "m"
	got := *launch.Install.Settings
	if !reflect.DeepEqual(got, want) || launch.MaxAttempts != 3 {
		t.Fatalf("defaults differ:\n got %+v\nwant %+v", got, want)
	}
}

func TestHostRefusesStartingWhileARenamedEnvironmentNameIsSet(t *testing.T) {
	for _, name := range []string{"SDO_CODE_PATH", "SDO_RESPONDER_MENU"} {
		_, err := ParseHostLaunchEnv(hostArgs(), []string{name + "=go"})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("host must refuse %s, got %v", name, err)
		}
		_, err = ParseInstallExistingLaunchEnv(existingLaunchArgs(), []string{name + "=go"})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("install-existing must refuse %s, got %v", name, err)
		}
	}
}

func TestHostRejectsUnknownKeysAndMistypedEnvironment(t *testing.T) {
	if _, err := ParseHostLaunchEnv(hostArgs("--config", configFile(t, "response:\n  timout: 1m\n")), nil); err == nil {
		t.Error("an unknown key must fail the launch")
	}
	if _, err := ParseHostLaunchEnv(hostArgs(), []string{"SDO_RESPONSE_TIMOUT=1m"}); err == nil {
		t.Error("a mistyped environment name must fail the launch")
	}
}

func TestInstallExistingResolvesTheSameWay(t *testing.T) {
	file := configFile(t, "response:\n  timeout: 20m\nmode: watch-only\nevidence:\n  host_path: /opt/sdo-evidence\n  run_id: lane-1\n")
	launch, err := ParseInstallExistingLaunchEnv(existingLaunchArgs("--config", file), []string{"SDO_AGENT_EFFORT=high"})
	if err != nil {
		t.Fatal(err)
	}
	got := launch.Install
	if got.TimeoutSeconds != 1200 || !got.FrozenMemory || !got.WatchOnly || got.AgentEffort != "high" {
		t.Fatalf("install-existing did not resolve the config: %+v", got)
	}
	if !strings.Contains(got.EvidenceSink, "lane-1") || !strings.Contains(got.EvidenceSink, "/opt/sdo-evidence") {
		t.Fatalf("evidence sink = %q", got.EvidenceSink)
	}
	if _, err := ParseInstallExistingLaunchEnv(existingLaunchArgs("--watch-only"), nil); err == nil || !strings.Contains(err.Error(), "frozen") {
		t.Fatalf("--watch-only without --frozen-memory stays an error: %v", err)
	}
}

func TestConfigPrintShowsTheResolvedDocument(t *testing.T) {
	file := configFile(t, "response:\n  timeout: 20m\nagent:\n  model: m\n")
	var out, errOut bytes.Buffer
	code := RunConfig(context.Background(), []string{"print", "--config", file}, []string{"SDO_RESPONSE_MAX_LOOPS=3"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	for _, want := range []string{"timeout: 20m0s", "max_loops: 3", "repair_policy: commit", "verification_timeout: 5m0s"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("print lacks %q:\n%s", want, out.String())
		}
	}
	out.Reset()
	if code := RunConfig(context.Background(), []string{"print"}, nil, &out, &errOut); code == 0 || !strings.Contains(errOut.String(), "agent.model") {
		t.Errorf("a config that does not validate still prints but exits non-zero naming the key: code %d %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "max_loops: 10") {
		t.Error("print shows the document even when it is incomplete")
	}
}

func TestOperateAndControllerReadTheMountedConfig(t *testing.T) {
	cfg := settings.Default()
	cfg.Agent.Model = "m"
	cfg.Response.Timeout = settings.Duration(45 * time.Minute)
	document, err := cfg.YAML()
	if err != nil {
		t.Fatal(err)
	}
	path := configFile(t, string(document))
	got, err := loadMountedSettings(path)
	if err != nil || got == nil || time.Duration(got.Response.Timeout) != 45*time.Minute {
		t.Fatalf("mounted config = %+v, %v", got, err)
	}
	missing, err := loadMountedSettings(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil || missing != nil {
		t.Fatalf("an absent mount is no config: %v %v", missing, err)
	}
	if _, err := loadMountedSettings(configFile(t, "bogus: 1\n")); err == nil {
		t.Fatal("a mounted config with an unknown key must be refused")
	}
}

func TestControllerRecordsItsConfigBesideItsOtherRuntimeEvidence(t *testing.T) {
	cfg := settings.Default()
	cfg.Agent.Model = "m"
	document, err := cfg.YAML()
	if err != nil {
		t.Fatal(err)
	}
	mount := t.TempDir()
	if err := publishResolvedSettings(configFile(t, string(document)), mount); err != nil {
		t.Fatal(err)
	}
	recorded, err := os.ReadFile(filepath.Join(mount, ".sdo-runtime", "config.yaml"))
	if err != nil || string(recorded) != string(document) {
		t.Fatalf("recorded config = %q, %v", recorded, err)
	}
	empty := t.TempDir()
	if err := publishResolvedSettings(filepath.Join(empty, "absent.yaml"), empty); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(empty, ".sdo-runtime")); !os.IsNotExist(err) {
		t.Fatal("a controller without a mounted config records nothing")
	}
}

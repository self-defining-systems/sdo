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

// A file's values reach the install as the resolved config and the environment overrides the file; no knob has a
// flag of its own.
func TestHostResolvesDefaultsThenFilesThenEnvironment(t *testing.T) {
	file := configFile(t, "response:\n  timeout: 20m\n  max_loops: 4\nmode: frozen\nlearning:\n  healthy_baseline: true\n")
	launch, err := ParseHostLaunchEnv(hostArgs("--config", file), []string{"SDO_RESPONSE_MAX_LOOPS=6"})
	if err != nil {
		t.Fatal(err)
	}
	got := launch.Install.Settings
	if got == nil || time.Duration(got.Response.Timeout) != 20*time.Minute || got.Response.MaxLoops != 6 || got.Mode != settings.ModeFrozen || !got.Learning.HealthyBaseline {
		t.Fatalf("file and environment did not reach the install: %+v", got)
	}
	if got.Agent.Model != "m" {
		t.Fatalf("the earlier --config file's model is kept: %q", got.Agent.Model)
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
	if !reflect.DeepEqual(got, want) {
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
	if time.Duration(got.Settings.Response.Timeout) != 20*time.Minute || got.Settings.Mode != settings.ModeWatchOnly || got.Settings.Agent.Effort != "high" {
		t.Fatalf("install-existing did not resolve the config: %+v", got.Settings)
	}
	if !strings.Contains(got.EvidenceSink, "lane-1") || !strings.Contains(got.EvidenceSink, "/opt/sdo-evidence") {
		t.Fatalf("evidence sink = %q", got.EvidenceSink)
	}
}

// The policy flags are gone from every parser: a stale invocation fails at parse time instead of being ignored.
func TestDeletedPolicyFlagsAreUnknown(t *testing.T) {
	for _, flag := range [][]string{
		{"--response-timeout", "7m"}, {"--verification-timeout", "7m"}, {"--model", "m"}, {"--agent-effort", "high"},
		{"--repair-policy", "commit"}, {"--frozen-memory"}, {"--watch-only"}, {"--max-response-loops", "3"},
		{"--replay", "off"}, {"--healthy-baseline"}, {"--responder-env", "A=1"}, {"--code-path", "go"},
		{"--build-timeout", "7m"}, {"--telemetry-source", "a=http://b"}, {"--allow-registry", "ghcr.io"}, {"--evidence-sink-host-path", "/e"},
	} {
		if _, err := ParseHostLaunchEnv(hostArgs(flag...), nil); err == nil {
			t.Errorf("host accepted %v", flag)
		}
		if _, err := ParseInstallExistingLaunchEnv(existingLaunchArgs(flag...), nil); err == nil {
			t.Errorf("install-existing accepted %v", flag)
		}
		if _, err := ParseOperateLaunch(operateArgs(flag...)); err == nil {
			t.Errorf("operate accepted %v", flag)
		}
		if _, err := ParseControllerLaunch(controllerLaunchArgs(flag...)); err == nil {
			t.Errorf("controller accepted %v", flag)
		}
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

// The operate and controller Jobs read the mounted sdo-config and nothing else: a missing or invalid mount fails the
// launch instead of running defaults.
func TestOperateAndControllerRequireTheMountedConfig(t *testing.T) {
	withMountedSettings(t, func(c *settings.Config) { c.Response.Timeout = settings.Duration(45 * time.Minute) })
	operate, err := ParseOperateLaunch(operateArgs())
	if err != nil || time.Duration(operate.Install.Settings.Response.Timeout) != 45*time.Minute {
		t.Fatalf("operate did not read the mount: %+v, %v", operate.Install.Settings, err)
	}
	_, _, policy, err := parseControllerLaunch(controllerLaunchArgs())
	if err != nil || time.Duration(policy.Response.Timeout) != 45*time.Minute {
		t.Fatalf("controller did not read the mount: %+v, %v", policy, err)
	}
	for name, path := range map[string]string{"absent": filepath.Join(t.TempDir(), "absent.yaml"), "invalid": configFile(t, "bogus: 1\n")} {
		prior := mountedSettings
		mountedSettings = path
		if _, err := ParseOperateLaunch(operateArgs()); err == nil || !strings.Contains(err.Error(), "sdo-config") {
			t.Errorf("operate with an %s mount: %v", name, err)
		}
		if _, err := ParseControllerLaunch(controllerLaunchArgs()); err == nil || !strings.Contains(err.Error(), "sdo-config") {
			t.Errorf("controller with an %s mount: %v", name, err)
		}
		mountedSettings = prior
	}
}

// The compiled child reads the parent's record of the configuration through ControllerConfig.settings_path, and the
// launch carries the code path the configuration selects.
func TestControllerChildReadsTheParentsRecord(t *testing.T) {
	withMountedSettings(t, func(c *settings.Config) {
		c.Response.MaxLoops = 3
		c.Agent.CodePathOverrides = `{"response_stages":["agent"]}`
	})
	run, _, policy, err := parseControllerLaunch(controllerLaunchArgs())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := run.Controller.GetSettingsPath(), filepath.Join("/workspace", ".sdo-runtime", "config.yaml"); got != want {
		t.Fatalf("settings_path = %q, want %q", got, want)
	}
	if stages := run.Controller.GetCodePath().GetOverrides().GetResponseStages(); len(stages) != 1 || stages[0] != "agent" {
		t.Fatalf("code path = %v", run.Controller.GetCodePath())
	}
	mount := t.TempDir()
	if err := publishResolvedSettings(policy, mount); err != nil {
		t.Fatal(err)
	}
	recorded, err := settings.Read(resolvedSettingsPath(mount))
	if err != nil || recorded.Response.MaxLoops != 3 || !reflect.DeepEqual(recorded, policy) {
		t.Fatalf("recorded config = %+v, %v", recorded, err)
	}
}

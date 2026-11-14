package settings_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
)

func writeFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultsAreValidOnceAModelIsChosen(t *testing.T) {
	cfg := settings.Default()
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "agent.model") {
		t.Fatalf("a config without a model must be refused, got %v", err)
	}
	cfg.Agent.Model = "m"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults with a model must validate: %v", err)
	}
	if cfg.Response.RepairPolicy != "commit" || time.Duration(cfg.Response.VerificationTimeout) != 5*time.Minute {
		t.Fatalf("production defaults changed: repair_policy=%q verification_timeout=%v", cfg.Response.RepairPolicy, cfg.Response.VerificationTimeout)
	}
}

func TestResolveAppliesDefaultsThenFilesInOrderThenEnvironment(t *testing.T) {
	first := writeFile(t, "response:\n  timeout: 10m\n  max_loops: 4\nagent:\n  model: from-file\n")
	second := writeFile(t, "response:\n  timeout: 20m\n")
	cfg, err := settings.Resolve([]string{first, second}, []string{"SDO_RESPONSE_MAX_LOOPS=7", "UNRELATED=1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := time.Duration(cfg.Response.Timeout); got != 20*time.Minute {
		t.Errorf("a later file overrides an earlier one: timeout = %v", got)
	}
	if cfg.Response.MaxLoops != 7 {
		t.Errorf("the environment overrides every file: max_loops = %d", cfg.Response.MaxLoops)
	}
	if cfg.Agent.Model != "from-file" {
		t.Errorf("a key no later source sets keeps the earlier value: model = %q", cfg.Agent.Model)
	}
	if cfg.Learning.ReflectionGuidance != settings.Default().Learning.ReflectionGuidance {
		t.Errorf("an untouched key keeps its default")
	}
}

func TestResolveRejectsUnknownKeysAndMistypedEnvironmentNames(t *testing.T) {
	if _, err := settings.Resolve([]string{writeFile(t, "response:\n  timout: 10m\n")}, nil); err == nil || !strings.Contains(err.Error(), "timout") {
		t.Errorf("an unknown YAML key must be an error naming it, got %v", err)
	}
	if _, err := settings.Resolve([]string{writeFile(t, "nonsense: 1\n")}, nil); err == nil {
		t.Error("an unknown top-level key must be an error")
	}
	_, err := settings.Resolve(nil, []string{"SDO_RESPONSE_TIMOUT=10m"})
	if err == nil || !strings.Contains(err.Error(), "SDO_RESPONSE_TIMOUT") || !strings.Contains(err.Error(), "SDO_RESPONSE_TIMEOUT") {
		t.Errorf("a mistyped name must be an error that suggests the closest field, got %v", err)
	}
	if _, err := settings.Resolve(nil, []string{"SDO_RESPONSE_TIMEOUT=soon"}); err == nil {
		t.Error("an unparsable value must be an error")
	}
	for _, transport := range []string{"SDO_AGENT_GUIDANCE=go", "SDO_AGENT_SCRIPT_STATE=x", "SDO_DEPLOYMENT_MODEL=m", "SDO_TELEMETRY_POLICY=x", "SDO_CODE_PATH=go"} {
		if _, err := settings.Resolve(nil, []string{transport}); err != nil {
			t.Errorf("worker transport name %s shares a prefix but is not a knob: %v", transport, err)
		}
	}
}

func TestEnvironmentNamesAreDerivedFromTheYAMLTags(t *testing.T) {
	cfg, err := settings.Resolve(nil, []string{
		"SDO_MODE=watch-only", "SDO_AGENT_PROVIDER=claude", "SDO_AGENT_EFFORT=high", "SDO_AGENT_REASONING_SUMMARY=concise",
		"SDO_RESPONSE_STANDBY=false", `SDO_AGENT_EXTRA_ENV=["A=1","B=2"]`,
		`SDO_DEPLOYMENT_POLICY={"scope_blocking":true}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != settings.ModeWatchOnly || cfg.Agent.Provider != "claude" || cfg.Agent.Effort != "high" || cfg.Agent.ReasoningSummary != "concise" || cfg.Response.Standby || len(cfg.Agent.ExtraEnv) != 2 || !cfg.Deployment.Policy.ScopeBlocking {
		t.Fatalf("env not applied: %+v", cfg)
	}
}

func TestRenamedHostEnvironmentNamesAreRefusedWithTheNewName(t *testing.T) {
	for old, replacement := range map[string]string{"SDO_CODE_PATH": "SDO_AGENT_CODE_PATH", "SDO_RESPONDER_MENU": "SDO_RESPONSE_MENU"} {
		err := settings.RefuseRenamedEnvironment([]string{old + "=1"})
		if err == nil || !strings.Contains(err.Error(), old) || !strings.Contains(err.Error(), replacement) {
			t.Errorf("%s must be refused naming %s, got %v", old, replacement, err)
		}
	}
	if err := settings.RefuseRenamedEnvironment([]string{"SDO_AGENT_CODE_PATH=go", "PATH=/bin"}); err != nil {
		t.Error(err)
	}
}

func TestValidateIsTheOnePlaceValuesAreChecked(t *testing.T) {
	cases := map[string]func(*settings.Config){
		"mode":                          func(c *settings.Config) { c.Mode = "sideways" },
		"agent.provider":                func(c *settings.Config) { c.Agent.Provider = "gemini" },
		"agent.effort":                  func(c *settings.Config) { c.Agent.Effort = "extreme" },
		"agent.reasoning_summary":       func(c *settings.Config) { c.Agent.ReasoningSummary = "verbose" },
		"response.timeout":              func(c *settings.Config) { c.Response.Timeout = 0 },
		"response.verification_timeout": func(c *settings.Config) { c.Response.VerificationTimeout = 0 },
		"response.max_loops":            func(c *settings.Config) { c.Response.MaxLoops = 0 },
		"response.repair_policy":        func(c *settings.Config) { c.Response.RepairPolicy = "yolo" },
		"learning.replay":               func(c *settings.Config) { c.Learning.Replay = "maybe" },
		"learning.reflection_guidance":  func(c *settings.Config) { c.Learning.ReflectionGuidance = "other" },
		"force_reflection":              func(c *settings.Config) { c.Mode = settings.ModeFrozen; c.Learning.ForceReflection = true },
		"onboarding.sandboxes":          func(c *settings.Config) { c.Onboarding.Sandboxes = 0 },
		"deployment.max_attempts":       func(c *settings.Config) { c.Deployment.MaxAttempts = 0 },
		"build_timeout":                 func(c *settings.Config) { c.Deployment.BuildTimeout = settings.Duration(2 * time.Hour) },
		"evidence":                      func(c *settings.Config) { c.Evidence.HostPath, c.Evidence.Volume = "/a", "{}" },
		"evidence.run_id":               func(c *settings.Config) { c.Evidence.RunID = "r" },
		"telemetry.sources":             func(c *settings.Config) { c.Telemetry.Sources = []string{"no-equals"} },
		"agent.extra_env":               func(c *settings.Config) { c.Agent.ExtraEnv = []string{"1BAD=x"} },
		"deployment.policy":             func(c *settings.Config) { c.Deployment.Policy.GrantedNamespaces = []string{"x"} },
	}
	for want, mutate := range cases {
		cfg := settings.Default()
		cfg.Agent.Model = "m"
		mutate(&cfg)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Validate must refuse with %q, got %v", want, err)
		}
	}
}

func TestAConfigRoundTripsThroughItsYAML(t *testing.T) {
	cfg := settings.Default()
	cfg.Agent.Model = "m"
	cfg.Mode = settings.ModeExternal
	cfg.Response.Timeout = settings.Duration(45 * time.Minute)
	cfg.Telemetry.Sources = []string{"a=http://x"}
	data, err := cfg.YAML()
	if err != nil {
		t.Fatal(err)
	}
	back, err := settings.Read(writeFile(t, string(data)))
	if err != nil {
		t.Fatal(err)
	}
	again, err := back.YAML()
	if err != nil || string(again) != string(data) {
		t.Fatalf("round trip changed the document:\n%s\n---\n%s\n%v", data, again, err)
	}
	if !strings.Contains(string(data), "timeout: 45m0s") {
		t.Fatalf("durations are Go duration strings:\n%s", data)
	}
}

func TestEnvironmentNamesTableCoversEveryField(t *testing.T) {
	names := settings.Fields()
	if len(names) < 30 {
		t.Fatalf("only %d fields documented", len(names))
	}
	for _, f := range names {
		if f.Doc == "" {
			t.Errorf("%s has no doc tag", f.Key)
		}
		if !strings.HasPrefix(f.Env, "SDO_") {
			t.Errorf("%s env = %q", f.Key, f.Env)
		}
	}
}

// The one validator covers what the parsers and the installer used to check: the stage budget, the code-path
// overrides and worker variables another setting owns.
func TestValidateCoversTheRetiredParserChecks(t *testing.T) {
	for name, mutate := range map[string]func(*settings.Config){
		"response.stage_budget":     func(c *settings.Config) { c.Response.StageBudget = 0 },
		"agent.code_path_overrides": func(c *settings.Config) { c.Agent.CodePathOverrides = `{"bogus":1}` },
		"the installer":             func(c *settings.Config) { c.Agent.ExtraEnv = []string{"SDO_OBSERVATION_CEILING={}"} },
		"the controller":            func(c *settings.Config) { c.Agent.ExtraEnv = []string{"SDO_CONTROLLER_STATE=x"} },
		"is not NAME=VALUE":         func(c *settings.Config) { c.Agent.ExtraEnv = []string{"lower=x"} },
	} {
		cfg := settings.Default()
		cfg.Agent.Model = "m"
		mutate(&cfg)
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	cfg := settings.Default()
	cfg.Agent.CodePath, cfg.Agent.CodePathOverrides = "shell", `{"response_stages":["agent"]}`
	selection, err := cfg.Agent.CodePathSelection()
	if err != nil || selection.GetName() != "shell" || len(selection.GetOverrides().GetResponseStages()) != 1 {
		t.Fatalf("code path selection = %v, %v", selection, err)
	}
	if plain, err := settings.Default().Agent.CodePathSelection(); err != nil || plain != nil {
		t.Fatalf("the default go code path selects nothing: %v, %v", plain, err)
	}
}

// Reflection resumes the responder's session; no file or environment variable
// selects a fresh one, and no layer's default does.
func TestNoSettingSelectsAFreshReflectionSession(t *testing.T) {
	if _, err := settings.Resolve([]string{writeFile(t, "learning:\n  reflection_session: fresh\n")}, nil); err == nil || !strings.Contains(err.Error(), "reflection_session") {
		t.Errorf("learning.reflection_session must be an unknown key, got %v", err)
	}
	if _, err := settings.Resolve(nil, []string{"SDO_LEARNING_REFLECTION_SESSION=fresh"}); err == nil {
		t.Error("SDO_LEARNING_REFLECTION_SESSION must be rejected")
	}
}

// The detection section holds the operator timing that decides when a held condition opens an incident; its
// defaults are the production values and a benchmark profile overrides them by file or environment.
func TestDetectionDefaultsFileAndEnvironment(t *testing.T) {
	def := settings.Default().Detection
	if time.Duration(def.SelfHealHold) != 5*time.Minute || time.Duration(def.StabilityWindow) != 15*time.Second || time.Duration(def.DriftBatchWindow) != 20*time.Second {
		t.Fatalf("detection defaults = %+v", def)
	}
	file := filepath.Join(t.TempDir(), "bench.yaml")
	if err := os.WriteFile(file, []byte("detection:\n  self_heal_hold: 60s\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := settings.Resolve([]string{file}, []string{"SDO_AGENT_MODEL=m"})
	if err != nil || time.Duration(cfg.Detection.SelfHealHold) != time.Minute {
		t.Fatalf("file: %v %+v", err, cfg.Detection)
	}
	cfg, err = settings.Resolve([]string{file}, []string{"SDO_AGENT_MODEL=m", "SDO_DETECTION_SELF_HEAL_HOLD=90s"})
	if err != nil || time.Duration(cfg.Detection.SelfHealHold) != 90*time.Second {
		t.Fatalf("environment must win over the file: %v %+v", err, cfg.Detection)
	}
	for _, mutate := range []func(*settings.Config){
		func(c *settings.Config) { c.Detection.SelfHealHold = 0 },
		func(c *settings.Config) { c.Detection.StabilityWindow = 0 },
		func(c *settings.Config) { c.Detection.DriftBatchWindow = 0 },
	} {
		bad := settings.Default()
		bad.Agent.Model = "m"
		mutate(&bad)
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "detection.") {
			t.Fatalf("a non-positive detection value must be refused, got %v", err)
		}
	}
}

// Only the detection section can change what an accepted detector decides, so only it is bound into an
// operability report's subject.
func TestDetectionDigestBindsOnlyTheDetectionSection(t *testing.T) {
	base := settings.Default()
	base.Agent.Model = "m"
	digest := base.DetectionDigest()
	if len(digest) != 64 {
		t.Fatalf("digest = %q, want sha256 hex", digest)
	}
	other := base
	other.Response.Timeout = settings.Duration(time.Hour)
	other.Agent.Effort = "high"
	if other.DetectionDigest() != digest {
		t.Fatal("a response or agent value must not change the detection digest")
	}
	other.Detection.SelfHealHold = settings.Duration(time.Minute)
	if other.DetectionDigest() == digest {
		t.Fatal("a 60s hold must not share the 5m digest")
	}
}

// The start-up bound decides when a new install's incidents may open, not what a detector decides, so it is a
// response setting: 30 min by default, overridable by file and environment, 0s turns the period off, and only a
// negative value is refused (it is not tied to the self-heal hold).
func TestStartupBoundDefaultEnvironmentAndValidation(t *testing.T) {
	if got := time.Duration(settings.Default().Response.StartupBound); got != 30*time.Minute {
		t.Fatalf("response.startup_bound default = %v, want 30m", got)
	}
	cfg, err := settings.Resolve(nil, []string{"SDO_AGENT_MODEL=m", "SDO_RESPONSE_STARTUP_BOUND=0s"})
	if err != nil || cfg.Response.StartupBound != 0 {
		t.Fatalf("0s from the environment turns the period off: %v %v", err, cfg.Response.StartupBound)
	}
	cfg, err = settings.Resolve([]string{writeFile(t, "response:\n  startup_bound: 2m\n")}, []string{"SDO_AGENT_MODEL=m"})
	if err != nil || time.Duration(cfg.Response.StartupBound) != 2*time.Minute {
		t.Fatalf("a bound shorter than the self-heal hold is valid: %v %v", err, cfg.Response.StartupBound)
	}
	bad := settings.Default()
	bad.Agent.Model = "m"
	bad.Response.StartupBound = settings.Duration(-time.Second)
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "response.startup_bound") {
		t.Fatalf("a negative bound must be refused, got %v", err)
	}
}

// After a learned playbook applied and Recovered did not hold, the agent is dispatched by default; a profile
// can withhold it (escalate) or let the playbook keep waiting first (wait).
func TestAfterPlaybookDefaultsFileEnvironmentAndValidation(t *testing.T) {
	def := settings.Default().Response
	if def.AfterPlaybook != settings.AfterPlaybookAgent || time.Duration(def.AfterPlaybookWait) != 5*time.Minute {
		t.Fatalf("after_playbook defaults = %q %v", def.AfterPlaybook, def.AfterPlaybookWait)
	}
	file := writeFile(t, "response:\n  after_playbook: wait\n  after_playbook_wait: 3m\n")
	cfg, err := settings.Resolve([]string{file}, []string{"SDO_AGENT_MODEL=m"})
	if err != nil || cfg.Response.AfterPlaybook != settings.AfterPlaybookWait || time.Duration(cfg.Response.AfterPlaybookWait) != 3*time.Minute {
		t.Fatalf("file: %v %+v", err, cfg.Response)
	}
	cfg, err = settings.Resolve([]string{file}, []string{"SDO_AGENT_MODEL=m", "SDO_RESPONSE_AFTER_PLAYBOOK=escalate"})
	if err != nil || cfg.Response.AfterPlaybook != settings.AfterPlaybookEscalate {
		t.Fatalf("environment must win over the file: %v %+v", err, cfg.Response)
	}
	for _, mutate := range []func(*settings.Config){
		func(c *settings.Config) { c.Response.AfterPlaybook = "never" },
		func(c *settings.Config) { c.Response.AfterPlaybookWait = 0 },
	} {
		bad := settings.Default()
		bad.Agent.Model = "m"
		mutate(&bad)
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "response.after_playbook") {
			t.Fatalf("an invalid after_playbook value must be refused, got %v", err)
		}
	}
}

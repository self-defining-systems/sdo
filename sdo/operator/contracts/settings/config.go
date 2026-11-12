package settings

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Mode is how the controller treats incidents.
type Mode string

const (
	// ModeOperate detects, responds and learns.
	ModeOperate Mode = "operate"
	// ModeFrozen detects, responds and closes, but keeps closure evidence private and learns nothing.
	ModeFrozen Mode = "frozen"
	// ModeWatchOnly detects, opens, verifies and closes incidents but never responds (frozen memory).
	ModeWatchOnly Mode = "watch-only"
	// ModeExternal accepts external alerts only and closes on responder completion (frozen memory).
	ModeExternal Mode = "external"
)

// Modes lists every valid Mode.
var Modes = []Mode{ModeOperate, ModeFrozen, ModeWatchOnly, ModeExternal}

// AgentEfforts are the reasoning efforts a worker accepts; sdo/contracts/agent_env.json lists the same set for the
// Python workers, and a test keeps the two equal.
var AgentEfforts = []string{"low", "medium", "high", "xhigh", "max"}

// ReasoningSummaries are the reasoning summary modes of the Codex agent CLI; sdo/contracts/agent_env.json lists the same
// set, and a test keeps the two equal.
var ReasoningSummaries = []string{"auto", "concise", "detailed", "none"}

// DefaultLifecycleTimeout is the limit of one lifecycle (detection designer) session.
const DefaultLifecycleTimeout = 900 * time.Second

// DefaultMemoryCheckTimeout bounds one `sdo-operator memory check` run inside a worker.
const DefaultMemoryCheckTimeout = 300 * time.Second

// Duration is a time.Duration that is written and read as a Go duration string.
type Duration time.Duration

func (d Duration) String() string { return time.Duration(d).String() }

// MarshalYAML writes the Go duration string.
func (d Duration) MarshalYAML() (any, error) { return d.String(), nil }

// UnmarshalYAML reads a Go duration string such as "90s" or "5m".
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: a duration is a string such as \"90s\"", node.Line)
	}
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", node.Line, err)
	}
	*d = Duration(parsed)
	return nil
}

// Config is the operator's timing and policy configuration: how long SDO waits and how it responds. App memory
// (.sdo/) decides what counts as broken; nothing here changes a detector's verdict.
type Config struct {
	Mode       Mode       `yaml:"mode" doc:"how incidents are handled: operate (respond and learn), frozen (respond, learn nothing), watch-only (detect and verify, never respond) or external (external alerts only, close on responder completion)"`
	Agent      Agent      `yaml:"agent"`
	Response   Response   `yaml:"response"`
	Detection  Detection  `yaml:"detection"`
	Learning   Learning   `yaml:"learning"`
	Deployment Deployment `yaml:"deployment"`
	Evidence   Evidence   `yaml:"evidence"`
	Telemetry  Telemetry  `yaml:"telemetry"`
}

// Agent selects and bounds the agent workers.
type Agent struct {
	Provider           string   `yaml:"provider" doc:"agent provider of every worker: codex or claude"`
	Model              string   `yaml:"model" doc:"agent model of every worker (required)"`
	Effort             string   `yaml:"effort" doc:"reasoning effort of every worker: low, medium, high, xhigh or max"`
	ReasoningSummary   string   `yaml:"reasoning_summary" doc:"reasoning summaries Codex workers record in their rollouts: auto, concise, detailed or none (auto and concise record readable summaries)"`
	LifecycleTimeout   Duration `yaml:"lifecycle_timeout" doc:"limit of one lifecycle (detection designer) session"`
	MemoryCheckTimeout Duration `yaml:"memory_check_timeout" doc:"limit of one memory check a worker runs on its checkout"`
	CodePath           string   `yaml:"code_path" doc:"programming-model plugin set: go or shell"`
	CodePathOverrides  string   `yaml:"code_path_overrides" doc:"CodePathOverrides as a JSON object (for example {\"response_stages\":[\"agent\"]}); empty selects the code path's own seams"`
	ExtraEnv           []string `yaml:"extra_env" doc:"extra NAME=VALUE environment of responder workers"`
}

// Detection is the operator timing detectors and closure apply. It never says what is broken (accepted memory does);
// it is stamped into every detector observation and bound into the operability report's subject (DetectionDigest).
type Detection struct {
	SelfHealHold     Duration `yaml:"self_heal_hold" doc:"how long a Kubernetes condition without a give-up signal must hold before it opens an incident; carried in every detector observation and bound into the operability report"`
	StabilityWindow  Duration `yaml:"stability_window" doc:"how long the opening alerts must stay clear after SDO's last write settled before the incident counts as recovered"`
	DriftBatchWindow Duration `yaml:"drift_batch_window" doc:"batch window for unexplained drift"`
}

// DefaultSelfHealHold is written here, not referenced from the SDK, because the contracts module cannot depend on
// the SDK; a test in the operator module keeps it equal to sdk.DefaultSelfHealHold.
const DefaultSelfHealHold = 5 * time.Minute

// DetectionDigest is the sha256 of the canonical detection section: the settings that change what an accepted
// detector decides. Operability report subjects carry it, so a report made under one hold never binds to another.
func (c Config) DetectionDigest() string {
	data, err := yaml.Marshal(c.Detection)
	if err != nil {
		panic(fmt.Sprintf("settings: marshal detection: %v", err))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Response bounds and shapes the incident response.
type Response struct {
	Timeout              Duration `yaml:"timeout" doc:"how long the responder may work on one incident; the reflection deadline derives from it"`
	VerificationTimeout  Duration `yaml:"verification_timeout" doc:"how long the independent verification of a repair may take"`
	MaxLoops             int      `yaml:"max_loops" doc:"mitigation rounds the responder agent may use per incident (the automatic stage's one apply does not count)"`
	DiagnosisDeadline    Duration `yaml:"diagnosis_deadline" doc:"shared deadline of candidate diagnosis in the automatic stage"`
	StageBudget          Duration `yaml:"stage_budget" doc:"scheduling bound of one automatic-stage pass; a pass re-run on the responder's loop may take twice this"`
	RepairPolicy         string   `yaml:"repair_policy" doc:"the evidence the controller requires for a responder repair: commit (a source repair is committed through the broker; production) or recorded-actions (live cluster repairs are recorded as actions without a source commit; benchmark runs)"`
	Standby              bool     `yaml:"standby" doc:"pre-start a responder worker; false forces cold Jobs"`
	Menu                 bool     `yaml:"menu" doc:"offer the responder its tool menu"`
	BuiltinRestoreAuto   bool     `yaml:"builtin_restore_auto" doc:"let the automatic stage apply the built-in source-restore playbook (put a component's non-SDO departures from accepted source back) when its diagnosis matches; off, it is only offered like any playbook and the responder decides"`
	Permissive           bool     `yaml:"permissive" doc:"broad lane repair permissions excluding Secret and credential APIs"`
	PermissiveNamespaces []string `yaml:"permissive_namespaces" doc:"additional lane namespaces the permissive responder may repair"`
	StartupBound         Duration `yaml:"startup_bound" doc:"how long a new install waits for SDO to see its application healthy (2 min with no opener firing) before incidents open anyway; until then no incident opens. 0s turns the start-up period off"`
}

// Learning governs what the operator keeps from incidents.
type Learning struct {
	Replay             string `yaml:"replay" doc:"replay and corpus capture: on or off"`
	ReflectionGuidance string `yaml:"reflection_guidance" doc:"the agent's learning instructions: baseline learns from each confirmed cause; generalize compares related incident signatures and parameterizes a shared playbook; generalize-spec favors predicates visible in resource spec or status. None changes the detector SDK or the validator"`
	ForceReflection    bool   `yaml:"force_reflection" doc:"always dispatch reflection after a verified closure (not with a frozen mode)"`
	HealthyBaseline    bool   `yaml:"healthy_baseline" doc:"opt-in detector gate: a proposal that changes incident detector source is replayed against healthy snapshots a runner recorded with sdo-operator baseline-write (one sdktest.Snapshot JSON per file, outside the accepted repository); a detector that fires on one fails validation, and a missing baseline fails closed. It never reads benchmark verdicts"`
	LateFindings       string `yaml:"late_findings" doc:"late findings: off, or pull (the controller publishes a bounded incident-specific report that a responder fetches with sdo-operator late-findings --incident-id ID; the broker checks the claim against the controller-owned report)"`
}

// Deployment bounds deployment attempts and the policy they run under.
type Deployment struct {
	MaxAttempts    int              `yaml:"max_attempts" doc:"maximum deployment attempts"`
	BuildTimeout   Duration         `yaml:"build_timeout" doc:"deadline of one source image build"`
	BuildBudget    Duration         `yaml:"build_budget" doc:"deadline of all source image builds of one deployment attempt"`
	RolloutTimeout Duration         `yaml:"rollout_timeout" doc:"how long to wait for each applied workload to finish rolling out"`
	Policy         DeploymentPolicy `yaml:"policy" doc:"install-time deployment policy as a JSON object (registry allowlist, scope blocking and grants)"`
}

// DeploymentPolicy is the install-time deployment policy.
type DeploymentPolicy struct {
	AllowedRegistries   []string `yaml:"allowed_registries" json:"allowed_registries"`
	ScopeBlocking       bool     `yaml:"scope_blocking" json:"scope_blocking"`
	GrantedNamespaces   []string `yaml:"granted_namespaces" json:"granted_namespaces"`
	GrantedClusterKinds []string `yaml:"granted_cluster_kinds" json:"granted_cluster_kinds"`
}

// Evidence is the run's evidence sink.
type Evidence struct {
	HostPath string `yaml:"host_path" doc:"node directory that receives run evidence (writable by uid 65532); outlives the control namespace"`
	Volume   string `yaml:"volume" doc:"evidence sink as a Kubernetes VolumeSource JSON; excludes host_path"`
	RunID    string `yaml:"run_id" doc:"run directory inside the evidence sink (empty: the control namespace)"`
}

// Telemetry is the application telemetry the operator provides.
type Telemetry struct {
	Profile string   `yaml:"profile" doc:"application telemetry profile: a built-in name (prometheus-default, none), a YAML or JSON file, or an inline JSON document"`
	Sources []string `yaml:"sources" doc:"operator-provided external Prometheus-compatible metrics sources as ID=URL; the first becomes the default when the profile has none"`
}

// Default is the only place operator defaults are written.
func Default() Config {
	return Config{
		Mode: ModeOperate,
		Agent: Agent{
			Provider: "codex", Effort: "medium", ReasoningSummary: "auto", LifecycleTimeout: Duration(DefaultLifecycleTimeout),
			MemoryCheckTimeout: Duration(DefaultMemoryCheckTimeout), CodePath: "go",
		},
		Response: Response{
			Timeout: Duration(30 * time.Minute), VerificationTimeout: Duration(5 * time.Minute), MaxLoops: 10,
			DiagnosisDeadline: Duration(10 * time.Second), StageBudget: Duration(45 * time.Second), RepairPolicy: "commit",
			Standby: true, Menu: true, StartupBound: Duration(30 * time.Minute),
		},
		Detection: Detection{
			SelfHealHold: Duration(DefaultSelfHealHold), StabilityWindow: Duration(15 * time.Second), DriftBatchWindow: Duration(20 * time.Second),
		},
		Learning: Learning{Replay: "on", ReflectionGuidance: "baseline", LateFindings: "off"},
		Deployment: Deployment{
			MaxAttempts: 3, BuildTimeout: Duration(DefaultBuildTimeout), BuildBudget: Duration(DefaultBuildBudget),
			RolloutTimeout: Duration(DefaultRolloutTimeout),
		},
		Telemetry: Telemetry{Profile: "prometheus-default"},
	}
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ownedWorkerEnvironment are worker variables another setting or the installer owns; agent.extra_env cannot set them.
var ownedWorkerEnvironment = map[string]string{
	"SDO_LATE_FINDINGS":       "learning.late_findings",
	"SDO_OBSERVATION_CEILING": "the installer",
}

// Validate is the only place a configuration value is checked: ranges, enums and cross-field rules.
func (c Config) Validate() error {
	var problems []error
	bad := func(key, format string, args ...any) {
		problems = append(problems, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
	}
	oneOf := func(key, value string, allowed ...string) {
		if !slices.Contains(allowed, value) {
			bad(key, "%q is not one of %s", value, strings.Join(allowed, ", "))
		}
	}
	positive := func(key string, d Duration) {
		if d <= 0 {
			bad(key, "%s must be positive", d)
		}
	}
	if !slices.Contains(Modes, c.Mode) {
		bad("mode", "%q is not one of %v", c.Mode, Modes)
	}
	oneOf("agent.provider", c.Agent.Provider, "codex", "claude")
	if strings.TrimSpace(c.Agent.Model) == "" {
		bad("agent.model", "a model is required")
	}
	oneOf("agent.effort", c.Agent.Effort, AgentEfforts...)
	oneOf("agent.reasoning_summary", c.Agent.ReasoningSummary, ReasoningSummaries...)
	positive("agent.lifecycle_timeout", c.Agent.LifecycleTimeout)
	positive("agent.memory_check_timeout", c.Agent.MemoryCheckTimeout)
	oneOf("agent.code_path", c.Agent.CodePath, "go", "shell")
	if _, err := c.Agent.CodePathSelection(); err != nil {
		bad("agent.code_path_overrides", "%v", err)
	}
	for _, value := range c.Agent.ExtraEnv {
		name, content, found := strings.Cut(value, "=")
		if !found || !envName.MatchString(name) || strings.ContainsAny(content, "\r\n") {
			bad("agent.extra_env", "%q is not NAME=VALUE", value)
		}
		if owner, owned := ownedWorkerEnvironment[name]; owned {
			bad("agent.extra_env", "%s is set by %s", name, owner)
		}
	}
	if c.Response.Timeout < Duration(time.Second) {
		bad("response.timeout", "%s must be at least 1s", c.Response.Timeout)
	}
	if c.Response.VerificationTimeout < Duration(time.Second) {
		bad("response.verification_timeout", "%s must be at least 1s", c.Response.VerificationTimeout)
	}
	if c.Response.MaxLoops < 1 {
		bad("response.max_loops", "%d must be at least 1", c.Response.MaxLoops)
	}
	positive("response.diagnosis_deadline", c.Response.DiagnosisDeadline)
	positive("response.stage_budget", c.Response.StageBudget)
	if c.Response.StartupBound < 0 {
		bad("response.startup_bound", "%s must not be negative", c.Response.StartupBound)
	}
	oneOf("response.repair_policy", c.Response.RepairPolicy, "commit", "recorded-actions")
	positive("detection.self_heal_hold", c.Detection.SelfHealHold)
	positive("detection.stability_window", c.Detection.StabilityWindow)
	positive("detection.drift_batch_window", c.Detection.DriftBatchWindow)
	oneOf("learning.replay", c.Learning.Replay, "on", "off")
	oneOf("learning.reflection_guidance", c.Learning.ReflectionGuidance, "baseline", "generalize", "generalize-spec")
	oneOf("learning.late_findings", c.Learning.LateFindings, "off", "pull")
	if c.Mode != ModeOperate && c.Learning.ForceReflection {
		bad("learning.force_reflection", "conflicts with mode %s, which keeps memory frozen", c.Mode)
	}
	if c.Deployment.MaxAttempts < 1 {
		bad("deployment.max_attempts", "%d must be at least 1", c.Deployment.MaxAttempts)
	}
	positive("deployment.build_timeout", c.Deployment.BuildTimeout)
	positive("deployment.build_budget", c.Deployment.BuildBudget)
	positive("deployment.rollout_timeout", c.Deployment.RolloutTimeout)
	if c.Deployment.BuildTimeout > c.Deployment.BuildBudget {
		bad("deployment.build_timeout", "%s cannot exceed the build budget %s", c.Deployment.BuildTimeout, c.Deployment.BuildBudget)
	}
	if policy := c.Deployment.Policy; !policy.ScopeBlocking && (len(policy.GrantedNamespaces) > 0 || len(policy.GrantedClusterKinds) > 0) {
		bad("deployment.policy", "grants require scope_blocking")
	}
	if c.Evidence.HostPath != "" && c.Evidence.Volume != "" {
		bad("evidence", "use one of host_path and volume")
	}
	if c.Evidence.HostPath == "" && c.Evidence.Volume == "" && c.Evidence.RunID != "" {
		bad("evidence.run_id", "requires an evidence sink (host_path or volume)")
	}
	for _, source := range c.Telemetry.Sources {
		if id, endpoint, ok := strings.Cut(source, "="); !ok || id == "" || endpoint == "" {
			bad("telemetry.sources", "%q must be ID=URL", source)
		}
	}
	return errors.Join(problems...)
}

// YAML is the canonical document of the configuration.
func (c Config) YAML() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := yaml.NewEncoder(&buffer)
	encoder.SetIndent(2)
	if err := encoder.Encode(c); err != nil {
		return nil, err
	}
	return buffer.Bytes(), encoder.Close()
}

// overlayYAML applies a document onto the configuration: keys the document sets replace, keys it omits stay.
// Unknown keys are an error.
func overlayYAML(c *Config, data []byte) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(c); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// Read loads a resolved configuration file (the sdo-config ConfigMap's document): defaults, then the file, validated.
func Read(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg := Default()
	if err := overlayYAML(&cfg, data); err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, cfg.Validate()
}

// Resolve layers the sources: Default(), then each file in order, then the SDO_<SECTION>_<KEY> variables of environ
// (KEY=VALUE lines, as os.Environ). It does not validate: callers apply their own last overrides, then call Validate.
func Resolve(files []string, environ []string) (Config, error) {
	cfg := Default()
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := overlayYAML(&cfg, data); err != nil {
			return Config{}, fmt.Errorf("config %s: %w", path, err)
		}
	}
	if err := overlayEnvironment(&cfg, environ); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

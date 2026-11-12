package operatorcli

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
)

// resolveSettings is the entry of host and install-existing: Default(), each --config file in order, the
// SDO_<SECTION>_<KEY> environment, and finally the pre-config flags the caller set explicitly, which still win so
// existing invocations keep their meaning. The renamed host environment names are refused first.
func resolveSettings(files []string, environ []string, flags *flag.FlagSet) (settings.Config, error) {
	if err := settings.RefuseRenamedEnvironment(environ); err != nil {
		return settings.Config{}, err
	}
	cfg, err := settings.Resolve(files, environ)
	if err != nil {
		return settings.Config{}, err
	}
	if err := overlayLegacyFlags(&cfg, flags); err != nil {
		return settings.Config{}, err
	}
	return cfg, cfg.Validate()
}

// overlayLegacyFlags applies the flags that were explicitly set. A flag left at its default changes nothing, so the
// resolved config's value (file, environment or default) stands.
func overlayLegacyFlags(cfg *settings.Config, flags *flag.FlagSet) error {
	var frozen, watch, external, modeSet bool
	var problem error
	flags.Visit(func(f *flag.Flag) {
		value := f.Value.String()
		duration := func(target *settings.Duration) {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				problem = errors.Join(problem, fmt.Errorf("--%s: %w", f.Name, err))
				return
			}
			*target = settings.Duration(parsed)
		}
		flagBool := func() bool { parsed, _ := strconv.ParseBool(value); return parsed }
		list := func() []string { return []string(*f.Value.(*repeatable)) }
		switch f.Name {
		case "code-path":
			setIfNotEmpty(&cfg.Agent.CodePath, value)
		case "code-path-overrides":
			cfg.Agent.CodePathOverrides = value
		case "model":
			cfg.Agent.Model = value
		case "agent-provider":
			cfg.Agent.Provider = value
		case "agent-effort":
			setIfNotEmpty(&cfg.Agent.Effort, value)
		case "lifecycle-timeout":
			if value != "0s" {
				duration(&cfg.Agent.LifecycleTimeout)
			}
		case "replay":
			setIfNotEmpty(&cfg.Learning.Replay, value)
		case "repair-policy":
			cfg.Response.RepairPolicy = value
		case "reflection-session":
			cfg.Learning.ReflectionSession = value
		case "reflection-guidance":
			cfg.Learning.ReflectionGuidance = value
		case "late-findings":
			cfg.Learning.LateFindings = value
		case "force-reflection":
			cfg.Learning.ForceReflection = flagBool()
		case "healthy-baseline":
			cfg.Learning.HealthyBaseline = flagBool()
		case "permissive-responder":
			cfg.Response.Permissive = flagBool()
		case "responder-namespace":
			cfg.Response.PermissiveNamespaces = list()
		case "responder-env":
			cfg.Agent.ExtraEnv = list()
		case "responder-standby":
			cfg.Response.Standby = flagBool()
		case "max-response-loops":
			cfg.Response.MaxLoops, _ = strconv.Atoi(value)
		case "response-timeout":
			duration(&cfg.Response.Timeout)
		case "verification-timeout":
			duration(&cfg.Response.VerificationTimeout)
		case "build-timeout":
			duration(&cfg.Deployment.BuildTimeout)
		case "build-budget":
			duration(&cfg.Deployment.BuildBudget)
		case "rollout-timeout":
			duration(&cfg.Deployment.RolloutTimeout)
		case "max-attempts":
			cfg.Deployment.MaxAttempts, _ = strconv.Atoi(value)
		case "frozen-memory":
			frozen, modeSet = flagBool(), true
		case "watch-only":
			watch, modeSet = flagBool(), true
		case "external-incidents":
			external, modeSet = flagBool(), true
		case "evidence-sink-host-path":
			cfg.Evidence.HostPath = value
		case "evidence-sink-volume":
			cfg.Evidence.Volume = value
		case "evidence-run-id":
			cfg.Evidence.RunID = value
		case "telemetry-profile":
			cfg.Telemetry.Profile = value
		case "telemetry-source":
			cfg.Telemetry.Sources = list()
		case "allow-registry":
			cfg.Deployment.Policy.AllowedRegistries = splitList(value)
		case "scope-blocking":
			cfg.Deployment.Policy.ScopeBlocking = flagBool()
		case "grant-namespace":
			cfg.Deployment.Policy.GrantedNamespaces = splitList(value)
		case "grant-cluster-kind":
			cfg.Deployment.Policy.GrantedClusterKinds = splitList(value)
		}
	})
	if problem != nil {
		return problem
	}
	if !modeSet {
		return nil
	}
	switch {
	case watch && external:
		return errors.New("watch-only conflicts with external incidents")
	case (watch || external) && !frozen:
		return errors.New("watch-only and external incidents require frozen memory")
	case watch:
		cfg.Mode = settings.ModeWatchOnly
	case external:
		cfg.Mode = settings.ModeExternal
	case frozen:
		cfg.Mode = settings.ModeFrozen
	default:
		cfg.Mode = settings.ModeOperate
	}
	return nil
}

func setIfNotEmpty(target *string, value string) {
	if value != "" {
		*target = value
	}
}

func splitList(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// applySettings projects the resolved configuration onto the installer's configuration: the one place a knob
// reaches install.Config, so the ConfigMap the Jobs mount and the argv they still carry cannot disagree.
func applySettings(c *install.Config, cfg settings.Config, controlNamespace string) error {
	codePath, err := parseCodePathSelection(cfg.Agent.CodePath, cfg.Agent.CodePathOverrides)
	if err != nil {
		return err
	}
	if cfg.Agent.CodePath == "go" && cfg.Agent.CodePathOverrides == "" {
		codePath = nil
	}
	policy, err := deploymentpolicy.FromFlags(deploymentpolicy.Flags{
		AllowedRegistries: strings.Join(cfg.Deployment.Policy.AllowedRegistries, ","), ScopeBlocking: cfg.Deployment.Policy.ScopeBlocking,
		GrantedNamespaces: strings.Join(cfg.Deployment.Policy.GrantedNamespaces, ","), GrantedClusterKinds: strings.Join(cfg.Deployment.Policy.GrantedClusterKinds, ","),
	})
	if err != nil {
		return err
	}
	encodedPolicy, err := deploymentpolicy.Format(policy)
	if err != nil {
		return err
	}
	profile, err := resolveTelemetry(cfg.Telemetry.Profile, cfg.Telemetry.Sources, controlNamespace)
	if err != nil {
		return err
	}
	sink, err := resolveEvidenceSink(cfg.Evidence, controlNamespace)
	if err != nil {
		return err
	}
	standby := cfg.Response.Standby
	frozen := cfg.Mode != settings.ModeOperate
	c.Settings = &cfg
	c.CodePath, c.DeploymentPolicy, c.TelemetryProfile, c.EvidenceSink = codePath, encodedPolicy, profile, sink
	c.Model, c.AgentProvider, c.AgentEffort, c.LifecycleTimeout = cfg.Agent.Model, cfg.Agent.Provider, cfg.Agent.Effort, time.Duration(cfg.Agent.LifecycleTimeout)
	c.ResponderEnv = cfg.Agent.ExtraEnv
	c.Replay, c.ReflectionSession, c.ReflectionGuidance, c.ForceReflection = cfg.Learning.Replay, cfg.Learning.ReflectionSession, cfg.Learning.ReflectionGuidance, cfg.Learning.ForceReflection
	c.HealthyBaseline, c.LateFindings = cfg.Learning.HealthyBaseline, cfg.Learning.LateFindings
	c.FrozenMemory, c.WatchOnly, c.ExternalIncidents = frozen, cfg.Mode == settings.ModeWatchOnly, cfg.Mode == settings.ModeExternal
	c.RepairPolicy, c.MaxResponseLoops, c.ResponderStandby, c.ResponderMenu = cfg.Response.RepairPolicy, cfg.Response.MaxLoops, &standby, cfg.Response.Menu
	c.PermissiveResponder, c.ResponderNamespaces = cfg.Response.Permissive, cfg.Response.PermissiveNamespaces
	c.TimeoutSeconds, c.VerificationTimeoutSeconds = int(time.Duration(cfg.Response.Timeout).Seconds()), int(time.Duration(cfg.Response.VerificationTimeout).Seconds())
	c.BuildTimeoutSeconds, c.BuildBudgetSeconds = int(time.Duration(cfg.Deployment.BuildTimeout).Seconds()), int(time.Duration(cfg.Deployment.BuildBudget).Seconds())
	c.RolloutTimeoutSeconds = int(time.Duration(cfg.Deployment.RolloutTimeout).Seconds())
	c.BackendRecipes = telemetrysources.Recipes()
	return nil
}

// mountedSettings is where the Jobs mount the sdo-config ConfigMap.
var mountedSettings = install.SettingsFile()

// loadMountedSettings reads the resolved configuration a Job was started with; nil when the Job was started without
// one (an install that predates the ConfigMap).
func loadMountedSettings(path string) (*settings.Config, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	cfg, err := settings.Read(path)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

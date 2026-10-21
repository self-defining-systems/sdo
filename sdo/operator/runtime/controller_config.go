package runtime

import (
	"fmt"
	"os"
	"strconv"

	"google.golang.org/protobuf/types/known/durationpb"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// LaunchConfigFlag is the controller entry point's single-argument launch mode:
// `controller --config <path>` reads one typed ControllerConfig (protojson) the
// launcher mounted, instead of a hand-mirrored argv flag list. A field the
// launcher sets that the controller does not define is then a build error on
// the shared proto, not an "unrecognized arguments" crash ten minutes into a
// run (the --closeout-state-gate drift class).
const LaunchConfigFlag = "--config"

// LoadControllerConfig reads and validates a protojson ControllerConfig.
func LoadControllerConfig(path string) (*contractsv1alpha1.ControllerConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read controller config: %w", err)
	}
	config := &contractsv1alpha1.ControllerConfig{}
	if err := wire.Unmarshal(raw, config); err != nil {
		return nil, fmt.Errorf("controller config %s: %w", path, err)
	}
	return config, nil
}

// controllerConfigToArgs expands a ControllerConfig into the controller's flag
// arguments. It is the single mapping between the shared config schema and the
// controller's own flags; the launcher never names a flag. Unset fields are
// omitted so the flag defaults apply, which is how the controller's non-zero
// defaults (e.g. --response-timeout 30m) survive a config that does not set
// them. Optional bools are emitted as `--flag=<bool>` only when present, so an
// omitted synthetic_traffic/clean_responder_helpers/state_baseline keeps its
// true default.
func controllerConfigToArgs(config *contractsv1alpha1.ControllerConfig) []string {
	var args []string
	str := func(flag, value string) {
		if value != "" {
			args = append(args, flag, value)
		}
	}
	dur := func(flag string, value *durationpb.Duration) {
		if value != nil {
			args = append(args, flag, value.AsDuration().String())
		}
	}
	repeated := func(flag string, values []string) {
		for _, value := range values {
			args = append(args, flag+"="+value)
		}
	}
	boolFlag := func(flag string, set bool) {
		if set {
			args = append(args, flag)
		}
	}
	optBool := func(flag string, value *bool) {
		if value != nil {
			args = append(args, flag+"="+strconv.FormatBool(*value))
		}
	}

	str("--namespace", config.GetNamespace())
	str("--control-namespace", config.GetControlNamespace())
	str("--app-root", config.GetAppRoot())
	str("--application", config.GetApplication())
	str("--source-commit", config.GetSourceCommit())
	str("--deployed-commit", config.GetDeployedCommit())
	repeated("--deployed-manifest-path", config.GetDeployedManifestPaths())

	str("--dispatcher-mode", config.GetDispatcherMode())
	str("--responder-image", config.GetResponderImage())
	str("--agent-registration-directory", config.GetAgentRegistrationDirectory())
	str("--responder-agent-id", config.GetResponderAgentId())
	str("--repository-pvc", config.GetRepositoryPvc())
	str("--repository-mount-path", config.GetRepositoryMountPath())
	str("--repository-pvc-subpath", config.GetRepositoryPvcSubpath())
	str("--responder-credentials-secret", config.GetResponderCredentialsSecret())
	repeated("--responder-env", config.GetResponderEnv())

	str("--firing-telemetry-path", config.GetFiringTelemetryPath())

	dur("--duration", config.GetDuration())
	dur("--response-timeout", config.GetResponseTimeout())
	dur("--verification-timeout", config.GetVerificationTimeout())
	if config.GetMaxFollowUps() > 0 {
		args = append(args, "--max-follow-ups", strconv.Itoa(int(config.GetMaxFollowUps())))
	}
	if config.GetMaxResponseLoops() > 0 {
		args = append(args, "--max-response-loops", strconv.Itoa(int(config.GetMaxResponseLoops())))
	}
	boolFlag("--program-fast-path", config.GetProgramFastPath())
	if config.GetProgramTrustThreshold() > 0 {
		args = append(args, "--program-trust-threshold", strconv.Itoa(int(config.GetProgramTrustThreshold())))
	}
	dur("--diagnosis-deadline", config.GetDiagnosisDeadline())
	dur("--drift-sweep-interval", config.GetDriftSweepInterval())
	dur("--program-min-duration", config.GetProgramMinDuration())
	dur("--drift-batch-window", config.GetDriftBatchWindow())
	if config.GetDriftDispatchesPerHour() > 0 {
		args = append(args, "--drift-dispatches-per-hour", strconv.Itoa(int(config.GetDriftDispatchesPerHour())))
	}
	if config.GetDriftTokensPerHour() > 0 {
		args = append(args, "--drift-tokens-per-hour", strconv.FormatInt(config.GetDriftTokensPerHour(), 10))
	}
	dur("--diagnosis-settle-delay", config.GetDiagnosisSettleDelay())
	dur("--follow-up-cooldown", config.GetFollowUpCooldown())
	str("--repair-policy", config.GetRepairPolicy())

	str("--lease-name", config.GetLeaseName())
	dur("--lease-duration", config.GetLeaseDuration())
	str("--identity", config.GetIdentity())

	boolFlag("--exit-after-closure", config.GetExitAfterClosure())
	boolFlag("--restart-after-closure", config.GetRestartAfterClosure())
	boolFlag("--force-reflection", config.GetForceReflection())
	str("--maintenance-configmap", config.GetMaintenanceConfigmap())
	dur("--maintenance-poll-interval", config.GetMaintenancePollInterval())
	dur("--resume-sync-timeout", config.GetResumeSyncTimeout())
	optBool("--synthetic-traffic", config.SyntheticTraffic)
	dur("--synthetic-traffic-warmup", config.GetSyntheticTrafficWarmup())
	str("--prober-binary", config.GetProberBinary())
	str("--prober-image", config.GetProberImage())
	str("--prober-url", config.GetProberUrl())
	optBool("--clean-responder-helpers", config.CleanResponderHelpers)
	optBool("--state-baseline", config.StateBaseline)

	return args
}

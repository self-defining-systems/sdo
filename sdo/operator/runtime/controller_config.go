package runtime

import (
	"fmt"
	"os"

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
	if err := wire.UnmarshalDurable(raw, config); err != nil {
		return nil, fmt.Errorf("controller config %s: %w", path, err)
	}
	return config, nil
}

// controllerConfigToArgs expands a ControllerConfig's wiring into the
// controller's flag arguments. It is the single mapping between the shared
// config schema and the controller's own flags; the launcher never names a
// flag. Timing and policy are not flags: the controller reads them from
// settings_path. Unset fields are omitted so the flag defaults apply.
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

	str("--namespace", config.GetNamespace())
	str("--control-namespace", config.GetControlNamespace())
	str("--app-root", config.GetAppRoot())
	str("--application", config.GetApplication())
	str("--source-commit", config.GetSourceCommit())
	str("--deployed-commit", config.GetDeployedCommit())

	str("--responder-image", config.GetResponderImage())
	str("--agent-registration-directory", config.GetAgentRegistrationDirectory())
	str("--responder-agent-id", config.GetResponderAgentId())
	str("--repository-pvc", config.GetRepositoryPvc())
	str("--repository-mount-path", config.GetRepositoryMountPath())
	str("--repository-pvc-subpath", config.GetRepositoryPvcSubpath())
	str("--responder-credentials-secret", config.GetResponderCredentialsSecret())

	str("--firing-telemetry-path", config.GetFiringTelemetryPath())

	dur("--duration", config.GetDuration())
	dur("--closure-stability-window", config.GetClosureStabilityWindow())
	dur("--self-heal-hold", config.GetSelfHealHold())
	str("--memory-host-binary", config.GetMemoryHostBinary())
	str("--memory-host-digest", config.GetMemoryHostDigest())
	dur("--drift-batch-window", config.GetDriftBatchWindow())

	str("--identity", config.GetIdentity())

	if config.GetExitAfterClosure() {
		args = append(args, "--exit-after-closure")
	}
	str("--maintenance-configmap", config.GetMaintenanceConfigmap())
	str("--toolchain-image", config.GetToolchainImage())
	str("--script-image", config.GetScriptImage())
	str("--telemetry-environment", config.GetTelemetryEnvironmentFile())

	return args
}

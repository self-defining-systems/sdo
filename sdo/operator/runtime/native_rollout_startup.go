package runtime

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// completeNativeRolloutHandoffs runs only in the newly launched controller,
// after it has acquired the Lease and completed its first detector evaluation.
// The trusted builder supplies binary and tree provenance in ControllerConfig.
func completeNativeRolloutHandoffs(ctx context.Context, native NativeIncidentBroker, launch *contractsv1alpha1.ControllerConfig, appRoot string) error {
	handoffs, err := native.PendingControllerRollouts()
	if err != nil || len(handoffs) == 0 {
		return err
	}
	if launch == nil || launch.DiagnosticsFingerprint == "" || launch.ControllerArtifactDigest == "" || launch.ControllerRevision == "" {
		return errors.New("pending controller rollout requires trusted build provenance")
	}
	command := exec.CommandContext(ctx, "git", "-C", appRoot, "rev-parse", "HEAD")
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("verify accepted controller revision: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if strings.TrimSpace(string(output)) != launch.ControllerRevision {
		return errors.New("controller build revision differs from accepted repository HEAD")
	}
	fingerprint, err := native.Repository.DiagnosticsFingerprint(ctx, launch.ControllerRevision)
	if err != nil {
		return fmt.Errorf("verify accepted diagnostics tree: %w", err)
	}
	if fingerprint != launch.DiagnosticsFingerprint {
		return errors.New("controller build fingerprint differs from accepted diagnostics tree")
	}
	for _, handoff := range handoffs {
		if handoff.Expectation.ReflectionCommit != launch.ControllerRevision || handoff.Expectation.AfterDetectorFingerprint != fingerprint {
			return fmt.Errorf("controller revision does not satisfy rollout for incident %s", handoff.Expectation.IncidentID)
		}
		if err := native.RecordControllerRollout(ControllerRolloutReceipt{
			Expectation:                 handoff.Expectation,
			BuildArtifact:               launch.ControllerArtifactDigest,
			ControllerRevision:          launch.ControllerRevision,
			ObservedDetectorFingerprint: fingerprint,
			Success:                     true,
			VerifiedAt:                  time.Now().UTC(),
		}); err != nil {
			return fmt.Errorf("record controller rollout for incident %s: %w", handoff.Expectation.IncidentID, err)
		}
	}
	return nil
}

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/operability"
)

// PolicyViolation is an error that says the install-time deployment policy
// refuses a deployment (deployment.PolicyError). The lifecycle recognizes it
// by this interface because the deployer package imports the lifecycle.
type PolicyViolation interface {
	error
	PolicyChecks() []operability.Check
}

// PolicyRefusedError is the typed terminal hold of an accepted deployment that
// violates the deployment policy in force: a restart under a tightened policy.
// Retrying cannot change it, so it is never an anonymous error that ends a
// Job or crash-loops a controller: the owner is the operator (relax the policy
// or advance the deployment), the cause is "policy", and the controller keeps
// running without deploying. It is an *OperabilityError, so the terminal
// report, termination message and operability ConfigMap carry it unchanged.
type PolicyRefusedError struct {
	// Violations are the policy's own failed checks.
	Violations []operability.Check
	*OperabilityError
}

func (e *PolicyRefusedError) Error() string {
	details := make([]string, 0, len(e.Violations))
	for _, c := range e.Violations {
		details = append(details, c.ID+": "+c.Detail)
	}
	return "policy: the accepted deployment violates the install-time deployment policy; relax the policy or advance the deployment (" + strings.Join(details, "; ") + ")"
}

func (e *PolicyRefusedError) Unwrap() error { return e.OperabilityError }

// NewPolicyRefusedError builds the typed hold for commit's deployment.
func NewPolicyRefusedError(commit string, violations []operability.Check) *PolicyRefusedError {
	lines := make([]string, 0, len(violations))
	var paths []string
	for _, c := range violations {
		lines = append(lines, c.ID+": "+c.Detail)
		paths = append(paths, c.Paths...)
	}
	if len(paths) > 8 {
		paths = paths[:8]
	}
	return &PolicyRefusedError{Violations: violations,
		OperabilityError: terminalStop(operability.OwnerOperator, operability.O6, "policy", operability.CausePolicy, commit, "accepted deployment violates the deployment policy in force: "+strings.Join(lines, "; "), paths)}
}

// refusedByPolicy maps a policy violation to the typed hold; every other error
// is returned unchanged.
func refusedByPolicy(err error, commit string) error {
	var violation PolicyViolation
	if errors.As(err, &violation) {
		return NewPolicyRefusedError(commit, violation.PolicyChecks())
	}
	return err
}

// PolicyChecker verifies that the recorded accepted deployment satisfies the
// deployment policy in force, without touching the cluster.
type PolicyChecker interface {
	CheckAcceptedPolicy(ctx context.Context, commit, namespace string, recorded DeploymentReceipt) error
}

// AcceptedPolicyHold is the check every controller start runs, refresh or not:
// it returns the typed hold when the accepted deployment violates the policy in
// force, nil when it does not or nothing was deployed, and an error only for a
// failure of the check itself. The check binds to the commit the deployment
// evidence is recorded at, not to the accepted HEAD: memory commits (the
// judge's, a reflection's) follow the deployment and change the tree.
func AcceptedPolicyHold(ctx context.Context, accepted interface {
	AcceptedSourceEvidence(context.Context) (broker.AcceptedSource, bool, error)
}, checker PolicyChecker, namespace string) (*PolicyRefusedError, error) {
	source, found, err := accepted.AcceptedSourceEvidence(ctx)
	if err != nil {
		return nil, fmt.Errorf("accepted deployment evidence: %w", err)
	}
	if !found {
		return nil, nil
	}
	commit := source.Evidence.AcceptedCommit
	if commit == "" {
		return nil, errors.New("accepted deployment evidence names no accepted commit")
	}
	err = checker.CheckAcceptedPolicy(ctx, commit, namespace, deploymentFromBroker(source.Evidence))
	var refused *PolicyRefusedError
	if err == nil {
		return nil, nil
	}
	if converted, ok := refusedByPolicy(err, commit).(*PolicyRefusedError); ok {
		refused = converted
		return refused, nil
	}
	return nil, err
}

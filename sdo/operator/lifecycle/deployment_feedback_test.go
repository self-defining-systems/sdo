package lifecycle

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

type synthTimeout struct{}

func (synthTimeout) Error() string               { return "build timed out" }
func (synthTimeout) InfrastructureTimeout() bool { return true }

func TestDeploymentTimeoutFeedbackIsNotAPlanRejection(t *testing.T) {
	timeout := deploymentFailureFeedback(fmt.Errorf("deployment strategy: %w", synthTimeout{}))
	if !strings.Contains(timeout, "not a plan rejection") {
		t.Fatalf("timeout feedback = %q", timeout)
	}
	plan := deploymentFailureFeedback(errors.New("lstat: no such file"))
	if strings.Contains(plan, "not a plan rejection") || !strings.HasPrefix(plan, "independent source deployment failed:") {
		t.Fatalf("plain failure feedback = %q", plan)
	}
}

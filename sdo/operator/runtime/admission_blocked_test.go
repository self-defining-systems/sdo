package runtime

import (
	"context"
	"sdo.dev/operator/sdk"
	"testing"
)

func TestAdmissionRejectionClassifiesPrerequisitesOnly(t *testing.T) {
	for _, message := range []string{
		`failed calling webhook "validate.example.test": x509: certificate signed by unknown authority`,
		`admission webhook "validate.example.test" denied the request: policy`,
		`pods denied by policy require-labels`,
	} {
		if !admissionRejection(message) {
			t.Errorf("missed admission prerequisite: %s", message)
		}
	}
	for _, message := range []string{"FailedScheduling: insufficient cpu", "image pull access denied", "webhook backend container crashed", "quota exceeded"} {
		if admissionRejection(message) {
			t.Errorf("false admission prerequisite: %s", message)
		}
	}
}

type admissionClearDetector struct{ pipelineDetector }

func (admissionClearDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

// pendingDeploymentRollout settles every non-Deployment write and hands each
// Deployment rollout back with err.
type pendingDeploymentRollout struct{ err error }

func (r pendingDeploymentRollout) Wait(_ context.Context, refs []sdk.ObjectRef) error {
	for _, ref := range refs {
		if ref.Kind == "Deployment" {
			return r.err
		}
	}
	return nil
}

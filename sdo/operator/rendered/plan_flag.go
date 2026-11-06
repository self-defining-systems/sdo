package rendered

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// ParseDeploymentPlan decodes the --deployment-plan flag value: one strict
// ProtoJSON DeploymentPlan the human or harness provides for an application
// that ships its own deployment. Empty is no plan (nil). Admission of what the
// plan says (a source, no retired fields) is the lifecycle's, not the parser's.
func ParseDeploymentPlan(encoded string) (*contracts.DeploymentPlan, error) {
	if encoded == "" {
		return nil, nil
	}
	plan := &contracts.DeploymentPlan{}
	if err := protojson.Unmarshal([]byte(encoded), plan); err != nil {
		return nil, fmt.Errorf("invalid deployment plan: %w", err)
	}
	if plan.GetSource() == nil {
		return nil, errors.New("invalid deployment plan: it names no deployment source")
	}
	return plan, nil
}

// FormatDeploymentPlan encodes a DeploymentPlan as the compact ProtoJSON that
// --deployment-plan accepts. nil encodes as the empty string.
func FormatDeploymentPlan(plan *contracts.DeploymentPlan) (string, error) {
	if plan == nil {
		return "", nil
	}
	encoded, err := protojson.MarshalOptions{}.Marshal(plan)
	if err != nil {
		return "", fmt.Errorf("encode deployment plan: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return "", fmt.Errorf("encode deployment plan: %w", err)
	}
	return compact.String(), nil
}

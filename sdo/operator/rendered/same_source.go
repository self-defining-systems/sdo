package rendered

import (
	"bytes"
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// SameDeploymentSource reports whether two sources are the same deployment
// input. It compares the canonical encoding, not Any bytes: a source decoded
// from durable evidence re-encodes its config, and wire bytes of the same
// message may differ (map order, another language's encoder). Two nil sources
// are the same; a source that cannot be encoded equals nothing.
func SameDeploymentSource(left, right *contracts.DeploymentSource) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	a, errA := canonicalSource(left)
	b, errB := canonicalSource(right)
	return errA == nil && errB == nil && a == b
}

func canonicalSource(source *contracts.DeploymentSource) (string, error) {
	encoded, err := protojson.MarshalOptions{}.Marshal(source)
	if err != nil {
		return "", err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return "", err
	}
	return compact.String(), nil
}

package rendered

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// ParseDeploymentSource decodes the --deployment-source flag value: one strict
// ProtoJSON DeploymentSource. Empty is no source (nil).
func ParseDeploymentSource(encoded string) (*contracts.DeploymentSource, error) {
	if encoded == "" {
		return nil, nil
	}
	source := &contracts.DeploymentSource{}
	if err := protojson.Unmarshal([]byte(encoded), source); err != nil {
		return nil, fmt.Errorf("invalid deployment source: %w", err)
	}
	return source, nil
}

// FormatDeploymentSource encodes a DeploymentSource as the compact ProtoJSON
// that --deployment-source accepts. nil encodes as the empty string.
func FormatDeploymentSource(source *contracts.DeploymentSource) (string, error) {
	if source == nil {
		return "", nil
	}
	encoded, err := protojson.MarshalOptions{}.Marshal(source)
	if err != nil {
		return "", fmt.Errorf("encode deployment source: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return "", fmt.Errorf("encode deployment source: %w", err)
	}
	return compact.String(), nil
}

// DeploymentSourceFromFlags resolves the two controller source flags. Naming
// both is an error. Transitional: the path form is removed with
// --deployed-manifest-path (S3-11c), leaving ParseDeploymentSource.
func DeploymentSourceFromFlags(encoded string, paths []string) (*contracts.DeploymentSource, error) {
	if encoded != "" && len(paths) > 0 {
		return nil, errors.New("--deployment-source and --deployed-manifest-path are mutually exclusive")
	}
	if encoded != "" {
		return ParseDeploymentSource(encoded)
	}
	return LegacyRawSource(paths)
}

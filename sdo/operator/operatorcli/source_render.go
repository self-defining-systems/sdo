//archcheck:ratchet R5 count=3 until=S3-11c
package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

// RunSourceRender is an offline read-only transport for the production source
// renderer. Benchmark restore callers consume it instead of reimplementing
// renderer decisions. Secret data is excluded at the owning source edge.
//
// The request names its DeploymentSource as ProtoJSON in deployment_source.
// The legacy path list is the transitional raw form; a request carries one
// or the other.
func RunSourceRender(ctx context.Context, input io.Reader, output io.Writer) error {
	var request struct {
		Repository       string          `json:"repository"`
		Revision         string          `json:"revision"`
		Namespace        string          `json:"namespace"`
		DeploymentSource json.RawMessage `json:"deployment_source"`
		ManifestPaths    []string        `json:"manifest_paths"`
		// RenderEnvironment is the lane's recorded install-time environment
		// (ProtoJSON); absent is unknown, the renderer defaults.
		RenderEnvironment json.RawMessage `json:"render_environment"`
	}
	decoder := json.NewDecoder(io.LimitReader(input, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("invalid source render request")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("source render request must contain exactly one object")
	}
	deployment, err := renderRequestSource(request.DeploymentSource, request.ManifestPaths)
	if err != nil {
		return err
	}
	var environment *contracts.RenderEnvironment
	if len(request.RenderEnvironment) > 0 && string(request.RenderEnvironment) != "null" {
		if environment, err = renderenv.Parse(string(request.RenderEnvironment)); err != nil {
			return err
		}
	}
	source := rendered.Source{Repository: request.Repository, Deployment: deployment, Catalog: renderers.Catalog(), Environment: environment}
	inventory, err := source.AcceptedInventory(ctx, request.Revision, request.Namespace)
	if err != nil {
		return err
	}
	if len(inventory.Objects) == 0 {
		return json.NewEncoder(output).Encode([]any{})
	}
	return json.NewEncoder(output).Encode(inventory.Objects)
}

func renderRequestSource(encoded json.RawMessage, paths []string) (*contracts.DeploymentSource, error) {
	if len(encoded) == 0 || string(encoded) == "null" {
		return rendered.LegacyRawSource(paths)
	}
	if len(paths) > 0 {
		return nil, errors.New("source render request names both deployment_source and a legacy path list")
	}
	deployment := &contracts.DeploymentSource{}
	if err := protojson.Unmarshal(encoded, deployment); err != nil {
		return nil, fmt.Errorf("invalid deployment_source: %w", err)
	}
	return deployment, nil
}

package operatorcli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"sdo.dev/operator/rendered"
)

// RunSourceRender is an offline read-only transport for the production source
// renderer. Benchmark restore callers consume it instead of reimplementing
// generator decisions. Secret data is excluded at the owning source edge.
func RunSourceRender(ctx context.Context, input io.Reader, output io.Writer) error {
	var request struct {
		Repository    string   `json:"repository"`
		Revision      string   `json:"revision"`
		Namespace     string   `json:"namespace"`
		ManifestPaths []string `json:"manifest_paths"`
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
	source := rendered.Source{Repository: request.Repository, ManifestPaths: request.ManifestPaths}
	inventory, err := source.AcceptedInventory(ctx, request.Revision, request.Namespace)
	if err != nil {
		return err
	}
	if len(inventory.Objects) == 0 {
		return json.NewEncoder(output).Encode([]any{})
	}
	return json.NewEncoder(output).Encode(inventory.Objects)
}

// Package planfile is the stored form of a DeploymentPlan: the YAML (or JSON)
// document at .sdo/deployment/plan.yaml. It is the DeploymentPlan message in
// ProtoJSON field names, decoded strictly: duplicate keys, unknown fields,
// the retired strategy fields, and renderer or image-source configs the
// registered catalogs do not accept are errors. The package names no catalog
// type; callers pass whatever can Decode a config for an ID.
package planfile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/yaml.v3"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// ConfigDecoder checks that an ID is registered and that its config unpacks to
// the registered type. *render.Catalog and *imagesrc.Catalog both satisfy it.
type ConfigDecoder interface {
	Decode(id string, config *anypb.Any) (proto.Message, error)
}

// Catalogs are the registries a plan is checked against. A nil field skips that
// check; the structural decode (known fields, known config message types) always runs.
type Catalogs struct {
	Renderers ConfigDecoder
	Images    ConfigDecoder
}

// Parse decodes and checks a plan document.
func Parse(data []byte, catalogs Catalogs) (*contracts.DeploymentPlan, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("deployment plan is empty")
	}
	if err := layout.RejectDuplicateKeys(data); err != nil {
		return nil, fmt.Errorf("deployment plan: %w", err)
	}
	var generic any
	if err := yaml.Unmarshal(data, &generic); err != nil {
		return nil, fmt.Errorf("deployment plan: %w", err)
	}
	if _, ok := generic.(map[string]any); !ok {
		return nil, errors.New("deployment plan must be a mapping of DeploymentPlan fields")
	}
	asJSON, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("deployment plan is not JSON-compatible: %w", err)
	}
	plan := &contracts.DeploymentPlan{}
	if err := protojson.Unmarshal(asJSON, plan); err != nil {
		return nil, fmt.Errorf("deployment plan: %w", err)
	}
	if plan.GetStrategyId() != "" || plan.GetStrategyConfig() != nil {
		return nil, errors.New("deployment plan sets the retired strategy fields; describe the deployment only with source, images and expectedWorkloadIds")
	}
	source := plan.GetSource()
	if source == nil {
		return nil, errors.New("deployment plan names no deployment source; set source to a renderer ID and its config")
	}
	if catalogs.Renderers != nil {
		if _, err := catalogs.Renderers.Decode(source.GetRendererId(), source.GetConfig()); err != nil {
			return nil, fmt.Errorf("deployment plan source: %w", err)
		}
	}
	for i, image := range plan.GetImages() {
		if image.GetSourceId() == "" {
			if image.GetConfig() != nil {
				return nil, fmt.Errorf("deployment plan images[%d] (%s) has a config but no sourceId", i, image.GetImage())
			}
			continue
		}
		if catalogs.Images != nil {
			if _, err := catalogs.Images.Decode(image.GetSourceId(), image.GetConfig()); err != nil {
				return nil, fmt.Errorf("deployment plan images[%d] (%s): %w", i, image.GetImage(), err)
			}
		}
	}
	return plan, nil
}

// Format encodes a plan in its canonical stored form: compact ProtoJSON, which
// is also valid YAML, with a trailing newline.
func Format(plan *contracts.DeploymentPlan) ([]byte, error) {
	encoded, err := protojson.MarshalOptions{}.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("encode deployment plan: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return nil, fmt.Errorf("encode deployment plan: %w", err)
	}
	compact.WriteByte('\n')
	return compact.Bytes(), nil
}

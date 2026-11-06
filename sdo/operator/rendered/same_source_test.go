package rendered_test

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
)

func kustomizeFlagSource(t *testing.T) *contracts.DeploymentSource {
	t.Helper()
	config, err := anypb.New(&contracts.KustomizeRenderConfig{Path: "overlays/prod"})
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.DeploymentSource{RendererId: "kustomize", Config: config}
}

// Sources are compared by content, not Any bytes: a config re-encoded from
// durable evidence or written by another encoder is the same source.
func TestSameDeploymentSourceComparesContent(t *testing.T) {
	canonical := kustomizeFlagSource(t)
	var noncanonical []byte // path set twice; the last value wins
	noncanonical = protowire.AppendTag(noncanonical, 1, protowire.BytesType)
	noncanonical = protowire.AppendString(noncanonical, "base")
	noncanonical = protowire.AppendTag(noncanonical, 1, protowire.BytesType)
	noncanonical = protowire.AppendString(noncanonical, "overlays/prod")
	reencoded := &contracts.DeploymentSource{RendererId: "kustomize", Config: &anypb.Any{TypeUrl: canonical.GetConfig().GetTypeUrl(), Value: noncanonical}}
	if proto.Equal(canonical, reencoded) || !rendered.SameDeploymentSource(canonical, reencoded) {
		t.Fatal("equal content with different bytes must be the same source")
	}
	other := proto.Clone(canonical).(*contracts.DeploymentSource)
	other.RendererId = "raw"
	if rendered.SameDeploymentSource(canonical, other) || rendered.SameDeploymentSource(canonical, nil) || !rendered.SameDeploymentSource(nil, nil) {
		t.Fatal("distinct or missing sources compared equal")
	}
}

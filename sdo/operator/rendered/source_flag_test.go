package rendered_test

import (
	"strings"
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

func TestDeploymentSourceFlagRoundTrips(t *testing.T) {
	want := kustomizeFlagSource(t)
	encoded, err := rendered.FormatDeploymentSource(want)
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(encoded, "\n\t") {
		t.Fatalf("flag value must be one compact line: %q", encoded)
	}
	got, err := rendered.ParseDeploymentSource(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatalf("round trip = %v, want %v", got, want)
	}
}

func TestDeploymentSourceFlagEmptyIsNoSource(t *testing.T) {
	if encoded, err := rendered.FormatDeploymentSource(nil); err != nil || encoded != "" {
		t.Fatalf("Format(nil) = %q, %v", encoded, err)
	}
	if got, err := rendered.ParseDeploymentSource(""); err != nil || got != nil {
		t.Fatalf("Parse(\"\") = %v, %v", got, err)
	}
	if got, err := rendered.DeploymentSourceFromFlags("", nil); err != nil || got != nil {
		t.Fatalf("FromFlags(none) = %v, %v", got, err)
	}
}

func TestDeploymentSourceFlagIsStrict(t *testing.T) {
	for name, encoded := range map[string]string{
		"not json":      "kubernetes",
		"unknown field": `{"rendererId":"raw","bogus":1}`,
	} {
		if _, err := rendered.ParseDeploymentSource(encoded); err == nil {
			t.Errorf("%s: accepted %q", name, encoded)
		}
	}
}

func TestDeploymentSourceFlagsAreMutuallyExclusive(t *testing.T) {
	encoded, err := rendered.FormatDeploymentSource(kustomizeFlagSource(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rendered.DeploymentSourceFromFlags(encoded, []string{"kubernetes"}); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("both flags: err = %v", err)
	}
	got, err := rendered.DeploymentSourceFromFlags("", []string{"kubernetes"})
	if err != nil || got.GetRendererId() != "raw" {
		t.Fatalf("path form = %v, %v", got, err)
	}
	got, err = rendered.DeploymentSourceFromFlags(encoded, nil)
	if err != nil || got.GetRendererId() != "kustomize" {
		t.Fatalf("source form = %v, %v", got, err)
	}
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

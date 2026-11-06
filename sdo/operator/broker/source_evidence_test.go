package broker_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func syntheticEvidence(t *testing.T) broker.SourceEvidence {
	t.Helper()
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	source := &contracts.DeploymentSource{RendererId: "raw", Config: config}
	tree := strings.Repeat("a", 40)
	return broker.SourceEvidence{
		DeploymentSource: source,
		Plan:             &contracts.DeploymentPlan{Source: proto.Clone(source).(*contracts.DeploymentSource), ExpectedWorkloadIds: []string{"Deployment/ns/app"}},
		BaseTree:         tree, ObservedRevision: tree,
		RenderDigest: strings.Repeat("b", 64), EnvDigest: strings.Repeat("c", 64),
		Images: []string{"app@sha256:" + strings.Repeat("d", 64)}, ResourceIDs: []string{"Deployment/ns/app"}, Ready: true,
	}
}

func TestSourceEvidenceRecordRoundTripsTheSourceAndPlan(t *testing.T) {
	want := syntheticEvidence(t)
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"deployment_source"`, `"plan"`, `"base_tree"`, `"render_digest"`, `"env_digest"`} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("record %s lacks %s", encoded, field)
		}
	}
	var got broker.SourceEvidence
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.DeploymentSource, want.DeploymentSource) || !proto.Equal(got.Plan, want.Plan) || got.BaseTree != want.BaseTree || got.RenderDigest != want.RenderDigest || got.EnvDigest != want.EnvDigest {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// D4: a record written before deployment sources has no reader; it fails with
// an actionable reinstall message.
func TestSourceEvidenceFromAnOlderOperatorIsRejected(t *testing.T) {
	tree := strings.Repeat("a", 40)
	for name, record := range map[string]string{
		"strategy": `{"proposal_tree_digest":"` + tree + `","observed_revision":"` + tree + `","strategy_id":"raw-manifests-v1","images":["i"],"resource_ids":["r"],"ready":true}`,
		"no source":          `{"base_tree":"` + tree + `","observed_revision":"` + tree + `","render_digest":"x","env_digest":"y","images":["i"],"resource_ids":["r"],"ready":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			var got broker.SourceEvidence
			err := json.Unmarshal([]byte(record), &got)
			if !errors.Is(err, broker.ErrSourceEvidencePredatesDeploymentSource) || !strings.Contains(err.Error(), "reinstall") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestPublishVerifiedSourceRequiresSourceAndDigests(t *testing.T) {
	// Each mutation is rejected for its own reason, so the unmutated synthetic
	// evidence is otherwise valid.
	for name, c := range map[string]struct {
		mutate func(*broker.SourceEvidence)
		reason string
	}{
		"no source":        {func(e *broker.SourceEvidence) { e.DeploymentSource = nil }, "names no deployment source"},
		"no render digest": {func(e *broker.SourceEvidence) { e.RenderDigest = "" }, "digests"},
		"no env digest":    {func(e *broker.SourceEvidence) { e.EnvDigest = "" }, "digests"},
		"plan of another source": {func(e *broker.SourceEvidence) {
			other, _ := anypb.New(&contracts.RawRenderConfig{Paths: []string{"other"}})
			e.Plan.Source = &contracts.DeploymentSource{RendererId: "raw", Config: other}
		}, "another source"},
		"revision differs from tree": {func(e *broker.SourceEvidence) { e.ObservedRevision = strings.Repeat("e", 40) }, "does not match the deployed tree"},
	} {
		t.Run(name, func(t *testing.T) {
			evidence := syntheticEvidence(t)
			c.mutate(&evidence)
			// Validation precedes any repository access.
			if _, err := (&broker.Broker{}).PublishVerifiedSource(context.Background(), "source", evidence); err == nil || !strings.Contains(err.Error(), c.reason) {
				t.Fatalf("err = %v, want %q", err, c.reason)
			}
		})
	}
}

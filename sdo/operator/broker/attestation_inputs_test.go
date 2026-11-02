//go:build integration

package broker

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

// Sweep the complete decision schema: a newly added field must acquire an
// explicit binding test (or an explicit informational-only exception).
func TestReflectionIdentityBindsEveryDecisionField(t *testing.T) {
	_, b, _ := sharedReflectionFixture(t)
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	p := reflectionProposal(t, b, "lane-a")
	writeLearning(t, p, "alpha", "Synthetic learning")
	publication := &validate.PublicationContext{BaseCommit: p.BaseCommit, IncidentID: "lane-a"}
	identity := func(d *contracts.ReflectionDecision) string {
		t.Helper()
		value, err := b.validationIdentity(context.Background(), p.Path, p.BaseCommit, Responder, ReflectionPhase, publication, d)
		if err != nil || value == "" {
			t.Fatalf("identity: %q %v", value, err)
		}
		return value
	}
	original := identity(&contracts.ReflectionDecision{})
	cases := map[string]string{
		"learning_decision": `{"learning_decision":"updated"}`,
		"no_change_reason":  `{"no_change_reason":"synthetic"}`,
		"playbook_ids":      `{"playbook_ids":["alpha"]}`,
		"error":             `{"error":"synthetic"}`,
		"sdk_gaps":          `{"sdk_gaps":[{"primitive":"synthetic"}]}`,
		"usage":             `{"usage":{"input_tokens":7}}`,
		"decline_reason":    `{"decline_reason":"synthetic"}`,
		"failure_reason":    `{"failure_reason":"timeout"}`,
	}
	fields := (&contracts.ReflectionDecision{}).ProtoReflect().Descriptor().Fields()
	if fields.Len() != len(cases) {
		t.Fatal("decision schema changed: extend the identity sweep")
	}
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		t.Run(name, func(t *testing.T) {
			encoded, ok := cases[name]
			if !ok {
				t.Fatal("missing field binding test")
			}
			d := &contracts.ReflectionDecision{}
			if err := protojson.Unmarshal([]byte(encoded), d); err != nil {
				t.Fatal(err)
			}
			same := identity(d) == original
			if same != (name == "usage") {
				t.Fatalf("field %s: identity match=%v", name, same)
			}
			// Repeating exactly the same inputs must always match.
			if identity(proto.Clone(d).(*contracts.ReflectionDecision)) != identity(d) {
				t.Fatal("exact decision did not match")
			}
		})
	}
}

func TestReflectionIdentityBindsEveryPublicationField(t *testing.T) {
	_, b, _ := sharedReflectionFixture(t)
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	p := reflectionProposal(t, b, "lane-a")
	writeLearning(t, p, "alpha", "Synthetic learning")
	proposalRoot := p.Path
	original := validate.PublicationContext{BaseCommit: p.BaseCommit, IncidentID: "lane-a"}
	mutations := map[string]func(*validate.PublicationContext){
		"ObservationCeiling":   func(p *validate.PublicationContext) { p.ObservationCeiling = sdk.ObservationScope{Version: 1} },
		"ApplicationNamespace": func(p *validate.PublicationContext) { p.ApplicationNamespace = "peer" },
		"BaseCommit":           func(p *validate.PublicationContext) { p.BaseCommit = "different-base" },
		"IncidentID":           func(p *validate.PublicationContext) { p.IncidentID = "peer" },
		"VerifiedSourceTree":   func(p *validate.PublicationContext) { p.VerifiedSourceTree = "different-source" },
	}
	if reflect.TypeOf(original).NumField() != len(mutations) {
		t.Fatal("publication schema changed: extend the identity sweep")
	}
	identity := func(p *validate.PublicationContext) string {
		t.Helper()
		id, err := b.validationIdentity(context.Background(), proposalRoot, original.BaseCommit, Responder, ReflectionPhase, p, nil)
		if err != nil || id == "" {
			t.Fatalf("identity: %q %v", id, err)
		}
		return id
	}
	before := identity(&original)
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := original
			mutate(&changed)
			if identity(&changed) == before {
				t.Fatal("changed publication input reused identity")
			}
			if identity(&changed) != identity(&changed) {
				t.Fatal("exact publication did not match")
			}
		})
	}
}

func TestReflectionIdentityBindsCaptureNamesModesAndIncident(t *testing.T) {
	_, b, _ := sharedReflectionFixture(t)
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	p := reflectionProposal(t, b, "lane-a")
	writeLearning(t, p, "alpha", "Synthetic learning")
	publication := &validate.PublicationContext{BaseCommit: p.BaseCommit, IncidentID: "lane-a"}
	capture := t.TempDir()
	path := filepath.Join(capture, "synthetic.json")
	if err := os.WriteFile(path, []byte("synthetic observation"), 0600); err != nil {
		t.Fatal(err)
	}
	identity := func(incident string) string {
		t.Helper()
		id, err := b.validationIdentity(validate.WithCaptureSource(context.Background(), capture, incident), p.Path, p.BaseCommit, Responder, ReflectionPhase, publication, nil)
		if err != nil || id == "" {
			t.Fatalf("identity: %q %v", id, err)
		}
		return id
	}
	original := identity("lane-a")
	if identity("lane-a") != original {
		t.Fatal("exact capture did not match")
	}
	if identity("peer") == original {
		t.Fatal("capture incident omitted")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if identity("lane-a") == original {
		t.Fatal("capture mode omitted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, filepath.Join(capture, "renamed.json")); err != nil {
		t.Fatal(err)
	}
	if identity("lane-a") == original {
		t.Fatal("capture name omitted")
	}
}

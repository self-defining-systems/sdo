package broker

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"strings"
	"testing"
)

func TestBrokerObservationCeilingIgnoresForgedProposalPolicy(t *testing.T) {
	ctx := context.Background()
	root := repository(t)
	memoryFixture(t, root)
	ceiling := sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{{APIVersion: "v1", Kind: "Service", Namespace: "approved"}}}
	b, err := New(root, t.TempDir(), func(context.Context, string, []string) error { return nil }, ObservationPolicy{Ceiling: ceiling, ApplicationNamespace: "app"})
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "scope", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(proposal.Path, ".sdo", "arch.md")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), "schema_version: 1", "schema_version: 1\nobservation_scope:\n  version: 1\n  namespaces:\n  - name: forbidden\n    kinds: [{api_version: v1, kind: Service}]", 1))
	if err = os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	forged := ceiling
	forged.Targets = []sdk.ObservationTarget{{APIVersion: "v1", Kind: "Service", Namespace: "forbidden"}}
	if err = validate.WritePublicationContext(ctx, proposal.Path, validate.PublicationContext{BaseCommit: proposal.BaseCommit, IncidentID: "scope", ObservationCeiling: forged}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SDO_OBSERVATION_CEILING", `{"version":1,"targets":[{"api_version":"v1","kind":"Service","namespace":"forbidden"}]}`)
	if _, err = b.Publish(ctx, "scope", Upkeep, InitialPhase); err == nil || !strings.Contains(err.Error(), "exceeds installer ceiling") {
		t.Fatalf("forged policy expanded broker scope: %v", err)
	}
}

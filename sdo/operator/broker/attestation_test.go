package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"errors"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestReflectionCheckAttestationReusesTrustedCheck(t *testing.T) {
	_, b, _ := sharedReflectionFixture(t)
	calls := 0
	b.validator = func(context.Context, string, []string) error { calls++; return nil }
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	p := reflectionProposal(t, b, "lane-a")
	writeLearning(t, p, "alpha", "New synthetic learning")
	d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"alpha"}}
	report, err := b.CheckReflection(context.Background(), "lane-a", d)
	if err != nil || len(report.GetDiagnostics()) != 0 {
		t.Fatalf("check: %v %v", report, err)
	}
	if calls != 1 {
		t.Fatalf("check ran validator %d times", calls)
	}
	if _, err := b.PublishReflection(context.Background(), "lane-a", d); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("publication repeated attested validation: %d", calls)
	}
}

func TestReflectionAttestationBindsEveryInput(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*testing.T, *Broker, Proposal, string)
	}{
		{"bytes", func(t *testing.T, b *Broker, p Proposal, capture string) {
			writeLearning(t, p, "alpha", "Changed after check")
		}},
		{"mode", func(t *testing.T, b *Broker, p Proposal, capture string) {
			if err := os.Chmod(filepath.Join(p.Path, ".sdo/playbooks/alpha/README.md"), 0755); err != nil {
				t.Fatal(err)
			}
		}},
		{"base", func(t *testing.T, b *Broker, p Proposal, capture string) {
			if _, err := b.RecordOutcome(context.Background(), "peer", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "peer", Classification: "success"}); err != nil {
				t.Fatal(err)
			}
		}},
		{"toolchain", func(t *testing.T, b *Broker, p Proposal, capture string) {
			b.ValidationToolchain = "different-toolchain"
		}},
		{"metrics policy", func(t *testing.T, b *Broker, p Proposal, capture string) { b.MetricsDetectors = true }},
		{"namespace policy", func(t *testing.T, b *Broker, p Proposal, capture string) { b.applicationNamespace = "peer" }},
		{"observation policy", func(t *testing.T, b *Broker, p Proposal, capture string) {
			b.observationCeiling = sdk.ObservationScope{Version: 1}
		}},
		{"replay policy", func(t *testing.T, b *Broker, p Proposal, capture string) { b.replay = "off" }},
		{"captures", func(t *testing.T, b *Broker, p Proposal, capture string) {
			if err := os.WriteFile(filepath.Join(capture, "fixed.json"), []byte("changed"), 0600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, b, _ := sharedReflectionFixture(t)
			calls := 0
			b.validator = func(context.Context, string, []string) error { calls++; return nil }
			b.ValidationToolchain = "synthetic-immutable-toolchain"
			p := reflectionProposal(t, b, "lane-a")
			writeLearning(t, p, "alpha", "New synthetic learning")
			capture := t.TempDir()
			// Keep this path outside the candidate and bind it through trusted context.
			ctx := validate.WithCaptureSource(context.Background(), capture, "lane-a")
			d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"alpha"}}
			if report, err := b.CheckReflection(ctx, "lane-a", d); err != nil || len(report.GetDiagnostics()) != 0 {
				t.Fatalf("check: %v %v", report, err)
			}
			test.change(t, b, p, capture)
			if _, err := b.PublishReflection(ctx, "lane-a", d); err != nil {
				t.Fatal(err)
			}
			if calls != 2 {
				t.Fatalf("%s mismatch reused validation: calls=%d", test.name, calls)
			}
		})
	}
}

func TestReflectionAttestationNeverAcceptsWorkerClaimsOrFailedChecks(t *testing.T) {
	_, b, _ := sharedReflectionFixture(t)
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	p := reflectionProposal(t, b, "lane-a")
	writeLearning(t, p, "alpha", "New synthetic learning")
	calls := 0
	b.validator = func(context.Context, string, []string) error {
		calls++
		return errors.New("synthetic validator failure")
	}
	d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"alpha"}}
	if _, err := b.CheckReflection(context.Background(), "lane-a", d); err == nil {
		t.Fatal("failed validator accepted")
	}
	// Neither a forged success file nor a failed trusted check authorizes reuse.
	if err := os.WriteFile(filepath.Join(p.Path, ".git", "attestation.json"), []byte(`{"accepted":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	b.validator = func(context.Context, string, []string) error { calls++; return nil }
	if _, err := b.PublishReflection(context.Background(), "lane-a", d); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("untrusted/failed proof reused: calls=%d", calls)
	}
}

func TestReflectionAttestationBindsDecisionAndPublication(t *testing.T) {
	_, b, _ := sharedReflectionFixture(t)
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	p := reflectionProposal(t, b, "lane-a")
	writeLearning(t, p, "alpha", "New synthetic learning")
	ctx := context.Background()
	d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"alpha"}}
	publication := &validate.PublicationContext{BaseCommit: p.BaseCommit, IncidentID: "lane-a"}
	original, err := b.validationIdentity(ctx, p.Path, p.BaseCommit, Responder, ReflectionPhase, publication, d)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name        string
		actor       Actor
		phase       Phase
		publication *validate.PublicationContext
		decision    *contracts.ReflectionDecision
	}{
		{"decision", Responder, ReflectionPhase, publication, &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "different"}},
		{"decision IDs", Responder, ReflectionPhase, publication, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"beta"}}},
		{"actor", HealthJudge, ReflectionPhase, publication, d},
		{"phase", Responder, InitialPhase, publication, d},
		{"publication incident", Responder, ReflectionPhase, &validate.PublicationContext{BaseCommit: p.BaseCommit, IncidentID: "peer"}, d},
		{"verified source", Responder, ReflectionPhase, &validate.PublicationContext{BaseCommit: p.BaseCommit, IncidentID: "lane-a", VerifiedSourceTree: "peer"}, d},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity, err := b.validationIdentity(ctx, p.Path, p.BaseCommit, test.actor, test.phase, test.publication, test.decision)
			if err != nil {
				t.Fatal(err)
			}
			if identity == original {
				t.Fatal("changed bound input retained identity")
			}
		})
	}
}

func TestReflectionAttestationInvalidatedByRestart(t *testing.T) {
	root, b, _ := sharedReflectionFixture(t)
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	p := reflectionProposal(t, b, "lane-a")
	writeLearning(t, p, "alpha", "New synthetic learning")
	d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"alpha"}}
	if _, err := b.CheckReflection(context.Background(), "lane-a", d); err != nil {
		t.Fatal(err)
	}
	calls := 0
	restarted, err := New(root, b.workspaceRoot, func(context.Context, string, []string) error { calls++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	restarted.ValidationToolchain = b.ValidationToolchain
	if _, err := restarted.PublishReflection(context.Background(), "lane-a", d); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("restart trusted stale attestation: calls=%d", calls)
	}
}

package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// An accepted app ConfigMap may embed a script with trailing whitespace. The
// repair, its outcome and the reflection that follows must all publish with
// those bytes untouched, while authored memory text keeps the whitespace rule.
func TestIncidentClosesAndReflectsOverAppDataWhitespace(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	memoryFixture(t, target)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	repair, err := b.CreateProposal(ctx, "inc-ws")
	if err != nil {
		t.Fatal(err)
	}
	script := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: mongo-geo-script\ndata:\n  init.sh: |\n    mongo --eval 'db.geo.insert({})'   \n    \n"
	manifest := filepath.Join("kubernetes", "geo", "mongo-geo-script-configmap.yaml")
	if err := os.MkdirAll(filepath.Join(repair.Path, filepath.Dir(manifest)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repair.Path, manifest), []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, "inc-ws", Responder, ProposalPhase); err != nil {
		t.Fatalf("source repair over app whitespace rejected: %v", err)
	}
	if _, err := b.RecordOutcome(ctx, "inc-ws", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "inc-ws", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	reflection, err := b.CreateProposalForPhase(ctx, "inc-ws", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := os.ReadFile(filepath.Join(reflection.Path, manifest))
	if err != nil || string(accepted) != script {
		t.Fatalf("accepted app bytes changed: %q %v", accepted, err)
	}
	book := filepath.Join(reflection.Path, ".sdo", "playbooks", "geo-script")
	if err := os.MkdirAll(book, 0o755); err != nil {
		t.Fatal(err)
	}
	readme := "---\nschema_version: 1\nowner: responder\nfault_class: geo-script\noriginating_incident: inc-ws\n---\nRepair <TARGET_RESOURCE>\n"
	if err := os.WriteFile(filepath.Join(reflection.Path, ".sdo", "playbooks", "README.md"), []byte("# Playbooks\n\n- [base](base/README.md)\n- [geo-script](geo-script/README.md)\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"geo-script"}}
	if err := os.WriteFile(filepath.Join(book, "README.md"), []byte(strings.Replace(readme, "Repair", "Repair \n", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishReflection(ctx, "inc-ws", decision); err == nil || !strings.Contains(err.Error(), "diff.whitespace") {
		t.Fatalf("authored memory whitespace was not rejected: %v", err)
	}
	if err := os.WriteFile(filepath.Join(book, "README.md"), []byte(readme), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishReflection(ctx, "inc-ws", decision); err != nil {
		t.Fatalf("reflection over app whitespace rejected: %v", err)
	}
}

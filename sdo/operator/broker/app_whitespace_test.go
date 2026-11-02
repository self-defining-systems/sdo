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
// those bytes untouched, while authored memory whitespace is normalized.
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
	if err := os.WriteFile(filepath.Join(book, "README.md"), []byte(strings.ReplaceAll(readme, "\n", " \t\n")+"\n\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	receipt, err := b.PublishReflection(ctx, "inc-ws", decision)
	if err != nil {
		t.Fatalf("reflection over authored whitespace rejected: %v", err)
	}
	published, err := runGit(ctx, target, "show", receipt.Commit+":.sdo/playbooks/geo-script/README.md")
	if err != nil || string(published) != readme {
		t.Fatalf("authored whitespace was not normalized: %q %v", published, err)
	}
	app, err := runGit(ctx, target, "show", receipt.Commit+":"+filepath.ToSlash(manifest))
	if err != nil || string(app) != script {
		t.Fatalf("app bytes changed: %q %v", app, err)
	}
}

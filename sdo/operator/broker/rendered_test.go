package broker

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/rendered"
)

func TestRenderedLockRejectsAgentEditsAndStaleRender(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}} {
		if _, err := runGit(context.Background(), root, args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("/.sdo/rendered/\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "app.yaml")
	data := []byte("apiVersion: v1\nkind: Service\nmetadata:\n  name: api\nspec:\n  ports:\n  - port: 80\n    targetPort: 8080\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(context.Background(), root, "add", "."); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(context.Background(), root, "commit", "-qm", "source"); err != nil {
		t.Fatal(err)
	}
	head, _ := gitText(context.Background(), root, "rev-parse", "HEAD")
	source := rendered.Source{Repository: root, ManifestPaths: []string{"app.yaml"}}
	snapshot, err := source.BuildLock(context.Background(), head, "app")
	if err != nil {
		t.Fatal(err)
	}
	authority, err := New(root, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	locked, err := authority.RecordRenderedLock(context.Background(), head, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if locked == head {
		t.Fatal("ignored rendered lock was not published")
	}
	proposal, err := authority.CreateProposal(context.Background(), "agent-lock")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, rendered.LockPath), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Publish(context.Background(), "agent-lock", Responder, ProposalPhase); err == nil {
		t.Fatal("agent edited controller lock")
	}
	memory, err := authority.CreateProposalForPhase(context.Background(), "human-memory", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	goal := []byte("---\nschema_version: 1\nowner: human\napplication: test\n---\nGoal\n")
	if err := os.WriteFile(filepath.Join(memory.Path, ".sdo/goal.md"), goal, 0600); err != nil {
		t.Fatal(err)
	}
	receipt, err := authority.Publish(context.Background(), "human-memory", Human, InitialPhase)
	if err != nil {
		t.Fatalf("memory publication rejected inherited lock: %v", err)
	}
	before, _ := gitText(context.Background(), root, "rev-parse", locked+":.sdo/rendered")
	after, _ := gitText(context.Background(), root, "rev-parse", receipt.Commit+":.sdo/rendered")
	if before != after {
		t.Fatal("memory publication changed rendered lock")
	}
	locked = receipt.Commit
	snapshot.Lock.SourceCommit = locked
	for name, body := range snapshot.Files {
		snapshot.Files[name] = append(body, []byte("unexpected: true\n")...)
		for i := range snapshot.Lock.Objects {
			if snapshot.Lock.Objects[i].File == name {
				sum := sha256.Sum256(snapshot.Files[name])
				snapshot.Lock.Objects[i].SHA256 = fmt.Sprintf("%x", sum)
			}
		}
	}
	if _, err := authority.RecordRenderedLock(context.Background(), locked, snapshot); err == nil {
		t.Fatal("broker accepted stale or altered lock")
	}
}

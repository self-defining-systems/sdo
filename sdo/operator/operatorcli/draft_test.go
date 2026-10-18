package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestParseDraftTestRequiresTypedProposal(t *testing.T) {
	if _, err := ParseDraftTestLaunch([]string{"draft-test", "--app", "."}); err == nil {
		t.Fatal("draft-test accepted no typed artifact proposal")
	}
	config, err := ParseDraftTestLaunch([]string{"draft-test", "--app", ".", "--proposal-json", "draft.json", "--operator-root", ".."})
	if err != nil || config.ProposalJSON != "draft.json" {
		t.Fatalf("parse draft-test: %+v, %v", config, err)
	}
}

func TestDraftTestCompilesTypedArtifactsWithoutEditingCheckout(t *testing.T) {
	app := t.TempDir()
	fixture := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app")
	copy := exec.Command("cp", "-a", fixture+"/.", app)
	if output, err := copy.CombinedOutput(); err != nil {
		t.Fatalf("copy fixture: %v: %s", err, output)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "."}, {"-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"}} {
		command := exec.Command("git", args...)
		command.Dir = app
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	detach := exec.Command("git", "-C", app, "checkout", "--detach", "--quiet", "HEAD")
	if output, err := detach.CombinedOutput(); err != nil {
		t.Fatalf("detach proposal checkout: %v: %s", err, output)
	}
	proposal := &contracts.OperationalArtifactProposal{Detectors: []*contracts.ProposedDetector{{
		Id: "draft-incident", DetectorClass: "incident", Owner: "responder", Constructor: "New", OriginatingIncident: "draft-incident", OriginatingCommit: "accepted",
		Watches: []*contracts.MemoryWatch{{ApiVersion: "v1", Kind: "Pod"}}, Interval: "30s", Firing: 1, Clearing: 1, Severity: "critical", Debounce: "500ms",
		Sources: []*contracts.ProposedGoSource{{Name: "detector.go", Content: "package draft\nfunc New() undeclared.Type { return nil }\n"}},
	}}}
	data, err := protojson.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	proposalPath := filepath.Join(t.TempDir(), "draft.json")
	if err := os.WriteFile(proposalPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	err = RunDraftTest(context.Background(), DraftTestConfig{ApplicationRoot: app, OperatorRoot: "..", ProposalJSON: proposalPath})
	if err == nil || !strings.Contains(err.Error(), "go test") {
		t.Fatalf("draft compile error was not reported: %v", err)
	}
	status := exec.Command("git", "-C", app, "status", "--porcelain")
	output, err := status.CombinedOutput()
	if err != nil || len(output) != 0 {
		t.Fatalf("draft-test changed accepted checkout: %v: %s", err, output)
	}
}

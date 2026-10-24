package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/memory/validate"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"sdo.dev/operator/memory"
)

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func repository(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "accepted")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, root, "init", "-q")
	git(t, root, "config", "user.name", "Test")
	git(t, root, "config", "user.email", "test@example.com")
	if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "base")
	return root
}

func memoryFixture(t *testing.T, root string) {
	t.Helper()
	files := map[string]string{
		".sdo/schema-version":           "3\n",
		".sdo/goal.md":                  "---\nschema_version: 1\nowner: human\napplication: test\n---\nGoal\n",
		".sdo/arch.md":                  "---\nschema_version: 1\ngenerated_at_commit: base\ngenerated_at: 2026-01-01T00:00:00Z\napplication: test\ntopology_fingerprint: abc\n---\nArchitecture\n",
		".sdo/playbooks/README.md":      "# Playbooks\n\n- [base](base/README.md)\n",
		".sdo/playbooks/base/README.md": "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>\n",
		".sdo/diagnostics/detectors/health/health/manifest.yaml": detectorYAML("health", "health", 1, "base"),
		".sdo/diagnostics/detectors/health/health/detector.go":   "package objective\n",
	}
	for path, contents := range files {
		absolute := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git(t, root, "add", ".")
	git(t, root, "commit", "-qm", "initial memory")
}

func TestValidateMemoryTreeRejectsUnformattedGo(t *testing.T) {
	root := repository(t)
	memoryFixture(t, root)
	path := filepath.Join(root, ".sdo", "diagnostics", "detectors", "health", "health", "detector.go")
	if err := os.WriteFile(path, []byte("package objective\nfunc f(){println(1)}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	diagnostics, err := validate.Validate(context.Background(), validate.Request{Root: root, Base: "HEAD", Actor: validate.Responder, Phase: validate.ReflectionPhase, Layers: validate.Structural | validate.Semantic})
	if err == nil {
		err = validate.Err(diagnostics)
	}
	if err == nil || !strings.Contains(err.Error(), "not gofmt formatted") {
		t.Fatalf("unformatted direct edit accepted: %v", err)
	}
}

func TestResponderProgramProposalIgnoresPreexistingUnformattedHealthGo(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	memoryFixture(t, target)
	health := filepath.Join(target, ".sdo", "diagnostics", "detectors", "health", "health", "detector.go")
	if err := os.WriteFile(health, []byte("package objective\nfunc f(){println(1)}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(target, ".sdo", "lib", "base", "base.go")
	if err := os.MkdirAll(filepath.Dir(program), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(program, []byte("package base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, target, "add", ".")
	git(t, target, "commit", "-qm", "accepted memory")
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	repair, err := b.CreateProposal(ctx, "incident")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repair.Path, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, "incident", Responder, ProposalPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecordOutcome(ctx, "incident", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "incident", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, ".sdo", "lib", "base", "base.go"), []byte("package base\n\nfunc Diagnose() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var warnings bytes.Buffer
	oldOutput := log.Writer()
	log.SetOutput(&warnings)
	defer log.SetOutput(oldOutput)
	if _, err := b.PublishReflection(ctx, "incident", &contracts.ReflectionDecision{LearningDecision: "updated"}); err != nil {
		t.Fatalf("program proposal rejected: %v", err)
	}
	if !strings.Contains(warnings.String(), "owner: health_judge") || !strings.Contains(warnings.String(), "detector.go") {
		t.Fatalf("missing owner warning: %q", warnings.String())
	}
	if data, err := os.ReadFile(health); err != nil || string(data) != "package objective\nfunc f(){println(1)}\n" {
		t.Fatalf("accepted health source changed: %q, %v", data, err)
	}
}

func TestResponderDirectProgramEditRejectsUnformattedGo(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	memoryFixture(t, target)
	program := filepath.Join(target, ".sdo", "playbooks", "base", "diagnose.go")
	if err := os.WriteFile(program, []byte("package base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, target, "add", ".")
	git(t, target, "commit", "-qm", "accepted program")
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	repair, err := b.CreateProposal(ctx, "incident")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repair.Path, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, "incident", Responder, ProposalPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecordOutcome(ctx, "incident", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "incident", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, ".sdo", "playbooks", "base", "diagnose.go"), []byte("package base\nfunc Diagnose(){}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishReflection(ctx, "incident", &contracts.ReflectionDecision{LearningDecision: "updated"}); err == nil || !strings.Contains(err.Error(), "not gofmt formatted") {
		t.Fatalf("unformatted direct program edit accepted: %v", err)
	}
}

func TestNewProgramRequiresExecutableCodeRatherThanCaptureProvenance(t *testing.T) {
	root := repository(t)
	memoryFixture(t, root)
	dir := filepath.Join(root, ".sdo", "playbooks", "fault")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "diagnose.go"), []byte("package fault\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	caseDir := filepath.Join(root, ".sdo", "corpus", "fault")
	if err := os.MkdirAll(caseDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"open.json": "{}", "fixed.json": "{}", "healthy.json": "{}", "recordings.json": "[]", "labels.json": `{"root_cause_program_ids":["fault"]}`} {
		if err := os.WriteFile(filepath.Join(caseDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	diagnostics, err := validate.Validate(context.Background(), validate.Request{Root: root, Base: "HEAD", Actor: validate.Responder, Phase: validate.ReflectionPhase, Layers: validate.Structural | validate.Semantic})
	if err == nil {
		err = validate.Err(diagnostics)
	}
	if err == nil || !strings.Contains(err.Error(), "program.invalid") {
		t.Fatalf("non-executable program accepted: %v", err)
	}
}

func TestResponderProposalCommitsCanonicalGo(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	memoryFixture(t, target)
	health := filepath.Join(target, ".sdo", "diagnostics", "detectors", "health", "health", "detector.go")
	if err := os.WriteFile(health, []byte("package objective\nfunc f(){println(1)}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, target, "add", ".")
	git(t, target, "commit", "-qm", "accepted unformatted health source")
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	repair, err := b.CreateProposal(ctx, "incident")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repair.Path, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(ctx, "incident", Responder, ProposalPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecordOutcome(ctx, "incident", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "incident", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	manifest := "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: fault\nconstructor: New\nclass: incident\nowner: responder\nwatches:\n- apiVersion: v1\n  kind: Pod\ninterval: 30s\npersistence: {firing: 1, clearing: 1}\nbatching: {severity: critical, debounce: 500ms}\noriginatingIncident: incident\noriginatingCommit: accepted\npossiblePlaybooks: [base]\n"
	dir := filepath.Join(proposal.Path, ".sdo/diagnostics/detectors/incidents/fault")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "detector.go"), []byte("package fault\n\nfunc New() { println(1) }\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := b.PublishReflection(ctx, "incident", &contracts.ReflectionDecision{LearningDecision: "updated"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(target, ".sdo", "diagnostics", "detectors", "incidents", "fault", "detector.go"))
	if err != nil || string(data) != "package fault\n\nfunc New() { println(1) }\n" {
		t.Fatalf("accepted Go is not canonical: %q, %v", data, err)
	}
}

func TestInitialBootstrapUsesSequentialOwnedProposals(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		id    string
		actor Actor
		files map[string]string
	}{
		{"bootstrap-goal", Human, map[string]string{
			".sdo/goal.md": "---\nschema_version: 1\nowner: human\napplication: test\n---\nGoal\n",
		}},
		{"bootstrap-metadata", Upkeep, map[string]string{
			".sdo/schema-version": "3\n", ".sdo/diagnostics/go.mod": "module app-diagnostics\n\ngo 1.24\n",
		}},
		{"bootstrap-source", Deployer, map[string]string{
			"app.txt": "deployed source\n",
		}},
		{"bootstrap-architecture", Deployer, map[string]string{
			".sdo/arch.md": "---\nschema_version: 1\ngenerated_at_commit: SOURCE_COMMIT\napplication: test\n---\nArchitecture\n",
		}},
		{"bootstrap-health", HealthJudge, map[string]string{
			".sdo/playbooks/README.md":                                         "# Playbooks\n\n- [Health objective](health-objective/README.md)\n",
			".sdo/playbooks/health-objective/README.md":                        "---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\noriginating_incident: lifecycle-bootstrap\n---\nRepair <AFFECTED_RESOURCE>\n",
			".sdo/diagnostics/detectors/health/health-objective/manifest.yaml": detectorYAML("health", "health-objective", 2, "health-objective"),
			".sdo/diagnostics/detectors/health/health-objective/detector.go":   "package objective\n",
		}},
	}
	for _, step := range steps {
		proposal, err := b.CreateProposalForPhase(ctx, step.id, InitialPhase)
		if err != nil {
			t.Fatal(err)
		}
		for path, content := range step.files {
			content = strings.ReplaceAll(content, "SOURCE_COMMIT", git(t, target, "rev-parse", "HEAD"))
			absolute := filepath.Join(proposal.Path, path)
			if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(absolute, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		var publishErr error
		if step.actor == Deployer && step.id == "bootstrap-source" {
			var expected string
			expected, publishErr = ProposalTreeDigest(ctx, proposal.Path)
			if publishErr == nil {
				_, publishErr = b.PublishVerifiedSource(ctx, step.id, sourceEvidence(expected))
			}
		} else {
			_, publishErr = b.Publish(ctx, step.id, step.actor, InitialPhase)
		}
		if publishErr != nil {
			t.Fatalf("%s: %v", step.id, publishErr)
		}
	}
	if got := git(t, target, "log", "-n", "5", "--format=%s"); len(strings.Split(got, "\n")) != len(steps) {
		t.Fatalf("expected one accepted commit per actor, got %q", got)
	}
}

func TestVerifiedSourceRejectsMutationAfterDeployment(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "source", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("deployed source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	expected, err := ProposalTreeDigest(ctx, proposal.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("changed after deployment\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishVerifiedSource(ctx, "source", sourceEvidence(expected)); err == nil {
		t.Fatal("mutated source proposal was accepted")
	}
	if got := git(t, target, "show", "HEAD:app.txt"); got != "base" {
		t.Fatalf("accepted source changed: %q", got)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("deployed source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	receipt, err := b.PublishVerifiedSource(ctx, "source", sourceEvidence(expected))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit != git(t, target, "rev-parse", "HEAD") {
		t.Fatalf("unexpected commit: %+v", receipt)
	}
	acceptedTree := git(t, target, "rev-parse", "HEAD^{tree}")
	if acceptedTree != expected {
		t.Fatalf("accepted tree %s differs from deployed tree %s", acceptedTree, expected)
	}
	recovered, ok, err := b.SourceEvidence(ctx, "source")
	if err != nil || !ok || recovered.AcceptedCommit != receipt.Commit || recovered.ProposalTreeDigest != expected || len(recovered.ManifestPaths) != 1 || recovered.ManifestPaths[0] != "deploy/app.yaml" {
		t.Fatalf("source evidence not durable: %+v, %v, %v", recovered, ok, err)
	}
}

func sourceEvidence(tree string) SourceEvidence {
	return SourceEvidence{ProposalTreeDigest: tree, ObservedRevision: tree, StrategyID: "raw-manifests", Images: []string{"registry.example/app@sha256:" + strings.Repeat("a", 64)}, ResourceIDs: []string{"Deployment/default/app"}, ManifestPaths: []string{"deploy/app.yaml"}, Ready: true}
}

func TestSourceEvidenceRecoversCommitAfterInterruptedLedgerFinalization(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "source", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("deployed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tree, err := ProposalTreeDigest(ctx, proposal.Path)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := b.PublishVerifiedSource(ctx, "source", sourceEvidence(tree))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.writeSourceEvidence("source", sourceEvidence(tree)); err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.SourceEvidence(ctx, "source")
	if err != nil || !ok || got.AcceptedCommit != receipt.Commit {
		t.Fatalf("reconciliation failed: %+v %v %v", got, ok, err)
	}
}

func TestSourceEvidencePendingWithoutCommitNeverReportsSuccess(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return errors.New("validation interrupted") })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(ctx, "source", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("deployed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	tree, err := ProposalTreeDigest(ctx, proposal.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishVerifiedSource(ctx, "source", sourceEvidence(tree)); err == nil {
		t.Fatal("interrupted validation accepted source")
	}
	if _, err := os.Stat(b.sourceEvidencePath("source")); err != nil {
		t.Fatalf("trusted evidence was not staged first: %v", err)
	}
	if _, found, err := b.SourceEvidence(ctx, "source"); err != nil || found {
		t.Fatalf("pending evidence presented as accepted: %v %v", found, err)
	}
	b.validator = func(context.Context, string, []string) error { return nil }
	if _, err := b.PublishVerifiedSource(ctx, "source", sourceEvidence(tree)); err != nil {
		t.Fatalf("retry after interrupted validation: %v", err)
	}
}

func TestInitialArchitectureCannotCiteProposalLocalSourceCommit(t *testing.T) {
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(context.Background(), "mixed-deployer", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(proposal.Path, ".sdo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("new source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, ".sdo", "arch.md"), []byte("---\nschema_version: 1\ngenerated_at_commit: "+proposal.BaseCommit+"\n---\nArchitecture\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "mixed-deployer", Deployer, InitialPhase); err == nil {
		t.Fatal("source and architecture accepted in the same commit")
	}
	if git(t, target, "rev-parse", "HEAD") != proposal.BaseCommit {
		t.Fatal("rejected proposal changed accepted state")
	}
}

func TestInitialPhaseCannotReopenCompletedMemory(t *testing.T) {
	target := repository(t)
	memoryFixture(t, target)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := b.CreateProposalForPhase(context.Background(), "late-goal", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proposal.Path, ".sdo", "goal.md"), []byte("---\nschema_version: 1\nowner: human\n---\nChanged goal\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "late-goal", Human, InitialPhase); err == nil {
		t.Fatal("completed memory reopened through initial phase")
	}
}

func TestProposalHasIndependentGitMetadataAndCannotPublishDirectly(t *testing.T) {
	target := repository(t)
	broker, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := broker.CreateProposal(context.Background(), "inc-1")
	if err != nil {
		t.Fatal(err)
	}
	if git(t, proposal.Path, "rev-parse", "--git-common-dir") != ".git" {
		t.Fatal("proposal uses linked Git metadata")
	}
	if _, err := os.Stat(filepath.Join(proposal.Path, ".git", "objects", "info", "alternates")); !os.IsNotExist(err) {
		t.Fatalf("clone uses object alternates: %v", err)
	}
	if git(t, proposal.Path, "remote") != "" {
		t.Fatal("proposal retains a remote capable of publishing")
	}
	base := git(t, target, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(proposal.Path, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, proposal.Path, "config", "user.name", "Agent")
	git(t, proposal.Path, "config", "user.email", "agent@example.com")
	git(t, proposal.Path, "add", ".")
	git(t, proposal.Path, "commit", "-qm", "agent repair")
	if git(t, target, "rev-parse", "HEAD") != base {
		t.Fatal("agent commit changed accepted head")
	}
	receipt, err := broker.Publish(context.Background(), "inc-1", Responder, ProposalPhase)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == base || receipt.Commit != git(t, target, "rev-parse", "HEAD") {
		t.Fatalf("invalid receipt %+v", receipt)
	}
	if got := git(t, target, "log", "-1", "--format=%an%n%B"); !strings.Contains(got, "SDO Commit Broker") || !strings.Contains(got, "SDO-Validation: passed") {
		t.Fatalf("unattributed accepted commit: %s", got)
	}
}

func TestBrokerRejectsOutcomeEditsAndUnvalidatedRepair(t *testing.T) {
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := b.CreateProposal(context.Background(), "inc-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Path, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "inc-2", Responder, ProposalPhase); err == nil {
		t.Fatal("unvalidated repair accepted")
	}
	if git(t, target, "show", "HEAD:app.txt") != "base" {
		t.Fatal("accepted tree changed")
	}
	if err := os.MkdirAll(filepath.Join(p.Path, ".sdo", "outcomes"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Path, ".sdo", "outcomes", "x.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	b.validator = func(_ context.Context, _ string, _ []string) error { return nil }
	if _, err := b.Publish(context.Background(), "inc-2", Responder, ReflectionPhase); err == nil {
		t.Fatal("agent outcome edit accepted")
	}
}

func TestBrokerRecordsOutcomeOnlyOnce(t *testing.T) {
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), nil)
	if err != nil {
		t.Fatal(err)
	}
	doc := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "inc-3", Classification: "success"}
	first, err := b.RecordOutcome(context.Background(), "inc-3", doc)
	if err != nil {
		t.Fatal(err)
	}
	second, err := b.RecordOutcome(context.Background(), "inc-3", doc)
	if err != nil {
		t.Fatal(err)
	}
	if first.Commit != second.Commit {
		t.Fatalf("duplicate commit: %+v %+v", first, second)
	}
	if _, err := b.RecordOutcome(context.Background(), "inc-3", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "inc-3", Classification: "failed"}); err == nil {
		t.Fatal("conflicting outcome accepted")
	}
}

func TestLegacyOutcomeMigrationPublishesAtomicallyAfterInterruptedAttempt(t *testing.T) {
	ctx := context.Background()
	target := repository(t)
	legacy := filepath.Join(target, ".sdo", "outcomes.jsonl")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o755); err != nil {
		t.Fatal(err)
	}
	first := `{"schema_version":1,"incident_id":"old","classification":"success"}`
	conflict := `{"schema_version":1,"incident_id":"old","classification":"failed"}`
	if err := os.WriteFile(legacy, []byte(first+"\n"+conflict+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, target, "add", ".sdo/outcomes.jsonl")
	git(t, target, "commit", "-qm", "legacy outcomes")
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), nil)
	if err != nil {
		t.Fatal(err)
	}
	before := git(t, target, "rev-parse", "HEAD")
	document := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "new", Classification: "success"}
	if _, err := b.RecordOutcome(ctx, "new", document); err == nil {
		t.Fatal("conflicting migration accepted")
	}
	if got := git(t, target, "rev-parse", "HEAD"); got != before {
		t.Fatalf("interrupted migration changed accepted HEAD: %s", got)
	}
	if _, err := os.Stat(legacy); err != nil {
		t.Fatalf("legacy stream disappeared before acceptance: %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, ".sdo", "outcomes")); !os.IsNotExist(err) {
		t.Fatalf("partial migration leaked into accepted checkout: %v", err)
	}
	second := `{"schema_version":1,"incident_id":"other","classification":"failed"}`
	if err := os.WriteFile(legacy, []byte(first+"\n"+second+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, target, "add", ".sdo/outcomes.jsonl")
	git(t, target, "commit", "-qm", "repair legacy stream")
	receipt, err := b.RecordOutcome(ctx, "new", document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("legacy stream survived accepted migration: %v", err)
	}
	store := memory.NewJSONStore(target)
	all, err := store.List(ctx)
	if err != nil || len(all) != 3 {
		t.Fatalf("accepted migration lost outcomes: %+v, %v", all, err)
	}
	changes := git(t, target, "show", "--format=", "--name-status", receipt.Commit)
	if !strings.Contains(changes, "D\t.sdo/outcomes.jsonl") || strings.Count(changes, ".sdo/outcomes/") != 3 {
		t.Fatalf("migration was not one accepted commit: %s", changes)
	}
	again, err := b.RecordOutcome(ctx, "new", document)
	if err != nil || again.Commit != receipt.Commit {
		t.Fatalf("migration retry was not idempotent: %+v, %v", again, err)
	}
}

func TestReflectionUsesFreshProposalAfterOutcome(t *testing.T) {
	target := repository(t)
	memoryFixture(t, target)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	first, err := b.CreateProposal(context.Background(), "inc-4")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(first.Path, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "inc-4", Responder, ProposalPhase); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecordOutcome(context.Background(), "inc-4", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "inc-4", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	reflection, err := b.CreateProposalForPhase(context.Background(), "inc-4", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	if reflection.Path == first.Path || reflection.BaseCommit == first.BaseCommit {
		t.Fatal("reflection did not start from accepted outcome")
	}
	if err := os.MkdirAll(filepath.Join(reflection.Path, ".sdo", "playbooks", "foo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(reflection.Path, ".sdo", "playbooks", "foo", "README.md"), []byte("---\nschema_version: 1\nowner: responder\nfault_class: foo\noriginating_incident: inc-4\n---\nRepair <TARGET_RESOURCE>\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(reflection.Path, ".sdo", "playbooks", "README.md"), []byte("# Playbooks\n\n- [base](base/README.md)\n- [foo](foo/README.md)\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PublishReflection(context.Background(), "inc-4", &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"foo"}}); err != nil {
		t.Fatal(err)
	}
}

func TestActorCannotPublishUnownedSource(t *testing.T) {
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, err := b.CreateProposalForPhase(context.Background(), "initial-1", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Path, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "initial-1", HealthJudge, InitialPhase); err == nil {
		t.Fatal("health judge source change accepted")
	}
}

func TestBrokerRejectsInvalidYAMLDespitePermissiveExternalValidator(t *testing.T) {
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, err := b.CreateProposal(context.Background(), "inc-yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Path, "service.yaml"), []byte("kind: Service\nmetadata:\n  name: a\n  name: b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "inc-yaml", Responder, ProposalPhase); err == nil {
		t.Fatal("duplicate YAML key accepted")
	}
	if git(t, target, "rev-parse", "HEAD") != p.BaseCommit {
		t.Fatal("invalid proposal changed accepted state")
	}
}

func TestBrokerRejectsInvalidGoSyntaxBeforeExternalValidator(t *testing.T) {
	target := repository(t)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, err := b.CreateProposal(context.Background(), "inc-go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.Path, "repair.go"), []byte("package repair\nfunc Broken( {\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish(context.Background(), "inc-go", Responder, ProposalPhase); err == nil {
		t.Fatal("invalid Go syntax accepted")
	}
}

func TestResponderCannotChangeJudgeManifestRegistration(t *testing.T) {
	target := repository(t)
	memoryFixture(t, target)
	b, err := New(target, filepath.Join(t.TempDir(), "workspaces"), func(_ context.Context, _ string, _ []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.RecordOutcome(context.Background(), "inc-protected", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "inc-protected", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	p, err := b.CreateProposalForPhase(context.Background(), "inc-protected", ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(p.Path, ".sdo", "diagnostics", "detectors", "health", "health", "manifest.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), "interval: 30s", "interval: 20s", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = b.Publish(context.Background(), "inc-protected", Responder, ReflectionPhase)
	if err == nil {
		t.Fatal("responder changed judge registration")
	}
	for _, want := range []string{"ownership.denied", "ownership.protected-detector", "health/health/manifest.yaml"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("rejection does not carry the validator diagnostics (missing %q): %v", want, err)
		}
	}
}

// detectorYAML is one detector's manifest. possiblePlaybook names a logical
// playbook that must exist in the tree the manifest is validated against.
func detectorYAML(class, id string, firing int, possiblePlaybook string) string {
	owner, origin := "health_judge", ""
	if class == "incident" {
		owner, origin = "responder", "originatingIncident: inc\n"
	}
	return fmt.Sprintf("apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: %s\nconstructor: New\nclass: %s\nowner: %s\nwatches:\n  - apiVersion: v1\n    kind: Pod\ninterval: 30s\npersistence:\n  firing: %d\n  clearing: 1\nbatching:\n  severity: critical\n  debounce: 500ms\npossiblePlaybooks:\n  - %s\n%soriginatingCommit: initial\n", id, class, owner, firing, possiblePlaybook, origin)
}

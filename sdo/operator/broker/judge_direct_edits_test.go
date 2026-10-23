package broker

import (
	"bytes"
	"context"
	"errors"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

func writeJudgeFile(t *testing.T, root, path, content string) {
	t.Helper()
	if strings.HasSuffix(path, ".go") {
		if formatted, err := format.Source([]byte(content)); err == nil {
			content = string(formatted)
		}
	}
	target := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// This fake authors final files just as an external judge does.
func fakeJudgeEdits(t *testing.T, root string) {
	t.Helper()
	writeJudgeFile(t, root, ".sdo/diagnostics/detectors/health/health-objective/manifest.yaml", detectorYAML("health", "health-objective", 2, "health-objective"))
	writeJudgeFile(t, root, ".sdo/diagnostics/detectors/health/health-objective/detector.go", "package objective\n// judge authored\n")
	writeJudgeFile(t, root, ".sdo/playbooks/health-objective/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: health-objective-failure\noriginating_incident: lifecycle-bootstrap\n---\nRepair <AFFECTED_RESOURCE>.\n")
	writeJudgeFile(t, root, ".sdo/playbooks/README.md", "# Playbooks\n\n- [base](base/README.md)\n- [health-objective](health-objective/README.md)\n")
	writeJudgeFile(t, root, ".sdo/diagnostics/traffic/workloads/links.yaml", "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: links\npurpose: link-probe\nlinks:\n  - from: api\n    to: db\n    port: 80\n")
}

func judgeBroker(t *testing.T) (*Broker, string) {
	t.Helper()
	root := repository(t)
	memoryFixture(t, root)
	if err := os.RemoveAll(filepath.Join(root, ".sdo/diagnostics/detectors")); err != nil {
		t.Fatal(err)
	}
	git(t, root, "add", "-A")
	git(t, root, "commit", "-qm", "before diagnostics")
	b, err := New(root, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return b, root
}

func TestFakeJudgeDirectEditsBootstrapAndRefinementOwnership(t *testing.T) {
	ctx := context.Background()
	b, root := judgeBroker(t)
	proposal, err := b.CreateProposalForPhase(ctx, "judge-bootstrap", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	fakeJudgeEdits(t, proposal.Path)
	receipt, err := b.Publish(ctx, "judge-bootstrap", HealthJudge, InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"service-endpoints", "workload-health", "traffic-links"} {
		path := ".sdo/diagnostics/detectors/health/" + id + "/manifest.yaml"
		if !slices.Contains(receipt.ChangedPaths, path) {
			t.Fatalf("Go bootstrap file missing from receipt: %s", path)
		}
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatal(err)
		}
	}
	source := ".sdo/diagnostics/detectors/health/health-objective/detector.go"
	proposal, err = b.CreateProposalForPhase(ctx, "judge-refine", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	writeJudgeFile(t, proposal.Path, source, "package objective\n// refined without changing registration\n")
	if _, err := b.Publish(ctx, "judge-refine", HealthJudge, InitialPhase); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, source))
	if err != nil || !strings.Contains(string(data), "refined") {
		t.Fatalf("refinement missing: %s %v", data, err)
	}
	for _, path := range []string{"app.txt", ".sdo/goal.md", ".sdo/playbooks/base/README.md", ".sdo/outcomes/forbidden.json", ".sdo/diagnostics/detectors/incidents/fault/detector.go"} {
		t.Run(path, func(t *testing.T) {
			id := "forbidden-" + strings.ReplaceAll(path, "/", "-")
			p, err := b.CreateProposalForPhase(ctx, id, InitialPhase)
			if err != nil {
				t.Fatal(err)
			}
			writeJudgeFile(t, p.Path, path, "forbidden change\n")
			_, err = b.Publish(ctx, id, HealthJudge, InitialPhase)
			var failure *validate.Failure
			if !errors.As(err, &failure) {
				t.Fatalf("expected typed ownership rejection: %v", err)
			}
			found := false
			for _, d := range failure.Diagnostics {
				found = found || d.Rule == validate.RuleOwnershipDenied
			}
			if !found {
				t.Fatalf("missing ownership diagnostic: %v", failure)
			}
			if err := b.ReleaseProposal(id, InitialPhase); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMalformedJudgeDetectorCLIAndBrokerDiagnosticParity(t *testing.T) {
	ctx := context.Background()
	b, _ := judgeBroker(t)
	proposal, err := b.CreateProposalForPhase(ctx, "malformed-judge", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	fakeJudgeEdits(t, proposal.Path)
	path := ".sdo/diagnostics/detectors/health/health-objective/detector.go"
	writeJudgeFile(t, proposal.Path, path, "package objective\nfunc Broken( {\n")
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.CommandContext(ctx, "go", "build", "-p", "8", "-o", binary, "../cmd/sdo-operator")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v: %s", err, output)
	}
	command := exec.CommandContext(ctx, binary, "memory", "check", "--json")
	command.Dir = proposal.Path
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	command.Env = append(os.Environ(), "SDO_ACTOR=health_judge", "SDO_PHASE=initial", "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot)
	var output, stderr bytes.Buffer
	command.Stdout, command.Stderr = &output, &stderr
	var exit *exec.ExitError
	if err := command.Run(); !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("CLI must reject malformed detector: %v %s", err, stderr.String())
	}
	report := &contracts.MemoryCheckReport{}
	if err := protojson.Unmarshal(output.Bytes(), report); err != nil {
		t.Fatal(err)
	}
	_, err = b.Publish(ctx, "malformed-judge", HealthJudge, InitialPhase)
	var failure *validate.Failure
	if !errors.As(err, &failure) {
		t.Fatalf("broker must reject malformed detector: %v", err)
	}
	cli, broker := []*contracts.MemoryDiagnostic{}, []*contracts.MemoryDiagnostic{}
	for _, d := range report.GetDiagnostics() {
		if d.GetRule() == validate.RuleGoSyntax {
			cli = append(cli, d)
		}
	}
	for _, d := range validate.ProtoDiagnostics(failure.Diagnostics) {
		if d.GetRule() == validate.RuleGoSyntax {
			broker = append(broker, d)
		}
	}
	if len(cli) != 1 || len(broker) != 1 || cli[0].GetPath() != path || cli[0].GetLine() != broker[0].GetLine() || cli[0].GetMessage() != broker[0].GetMessage() {
		t.Fatalf("named diagnostics differ: CLI %v, broker %v", cli, broker)
	}
}

func TestJudgeCheckDoesNotOverwriteAuthoredGenericPolicy(t *testing.T) {
	ctx := context.Background()
	b, _ := judgeBroker(t)
	proposal, err := b.CreateProposalForPhase(ctx, "generic-conflict", InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	fakeJudgeEdits(t, proposal.Path)
	if err := memory.PrepareHealth(proposal.Path); err != nil {
		t.Fatal(err)
	}
	path := ".sdo/diagnostics/detectors/health/service-endpoints/detector.go"
	before, err := os.ReadFile(filepath.Join(proposal.Path, path))
	if err != nil {
		t.Fatal(err)
	}
	authored := string(before) + "\n// authored policy override\n"
	writeJudgeFile(t, proposal.Path, path, authored)
	_, err = b.Publish(ctx, "generic-conflict", HealthJudge, InitialPhase)
	var failure *validate.Failure
	if !errors.As(err, &failure) || len(failure.Diagnostics) != 1 || failure.Diagnostics[0].Rule != validate.RuleGenericHealth {
		t.Fatalf("expected named generic-policy rejection: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(proposal.Path, path))
	if err != nil || string(after) != authored {
		t.Fatalf("authored file overwritten: %s %v", after, err)
	}
}

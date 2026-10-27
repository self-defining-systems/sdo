package broker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

const parityFixture = "../memory/validate/testdata/good"

func copyParityTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	err := filepath.WalkDir(parityFixture, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(parityFixture, p)
		target := filepath.Join(root, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
	module, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	put(t, root, ".sdo/diagnostics/go.mod", string(module), 0644)
	return root
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	testutil.Git(t, root, args...)
}

func put(t *testing.T, root, rel, content string, mode os.FileMode) {
	t.Helper()
	if strings.HasSuffix(rel, ".go") {
		if formatted, err := format.Source([]byte(content)); err == nil {
			content = string(formatted)
		}
	}
	target := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(target); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

const (
	incidentManifest = "apiVersion: sdo.dev/v1alpha1\nkind: DetectorManifest\nsdkVersion: v0.1\nid: dns\nconstructor: New\nclass: incident\nowner: responder\nwatches:\n  - apiVersion: v1\n    kind: Pod\ninterval: 30s\npersistence:\n  firing: 1\n  clearing: 1\nbatching:\n  severity: warn\n  debounce: \"0\"\npossiblePlaybooks:\n  - %s\noriginatingIncident: inc-2\noriginatingCommit: abc\n"
)

// TestCLIAndBrokerAgreeOnAcceptance exercises every layer through the real CLI
// and the broker pipeline, including the validation Job's actual runner body.
// Any caller-only diagnostic or omitted layer changes these verdicts/rules.
func TestCLIAndBrokerAgreeOnAcceptance(t *testing.T) {
	testCLIAndBrokerAgreeOnAcceptance(t, 0)
}

func TestCLIAndBrokerAgreeOnAcceptanceSecondShard(t *testing.T) {
	testCLIAndBrokerAgreeOnAcceptance(t, 1)
}

func testCLIAndBrokerAgreeOnAcceptance(t *testing.T, shard int) {
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.Command("go", "build", "-o", binary, "../cmd/sdo-operator")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, out)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}

	baseline := t.TempDir()
	put(t, baseline, "healthy.json", string(capturefixture.FromView(t, `{"namespace":"demo","services":[],"pods":[]}`)), 0644)
	schema, err := builder.SchemaIdentity(operatorRoot)
	if err != nil {
		t.Fatal(err)
	}

	type parityCase struct {
		name            string
		actor           validate.Actor
		phase           validate.Phase
		edit            func(t *testing.T, root string)
		want            []string
		allowAdditional bool
	}
	cases := []parityCase{
		{"healthy refused service ports", validate.Responder, validate.ReflectionPhase, parityRefusedPortsDetector, []string{validate.RuleDetectorHealthyBaseline}, false},
		{"healthy refused ports without recordings", validate.Responder, validate.ReflectionPhase, parityRefusedPortsDetector, []string{validate.RuleDetectorHealthyBaseline}, false},
		{"missing controller healthy capture", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", parityDetector(t, "cache")+"\n// requires trusted healthy reference\n", 0644)
		}, []string{validate.RuleDetectorHealthyBaseline}, false},
		// A closure verified healthy is the only precondition: the cause is bound
		// by the successful repair write on its own resource, so an opening
		// finding on another resource neither blocks nor is needed.
		{"successful cause without matching opening finding", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", parityDetector(t, "cache")+"\n// cause bound by its recorded repair write\n", 0644)
		}, nil, false},
		{"unverified peer gaps", validate.Responder, validate.ReflectionPhase, func(*testing.T, string) {}, nil, false},
		{"unverified peer no verified", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", parityDetector(t, "cache")+"\n// No verified cause.\n", 0644)
		}, []string{"reflection.executable-cause"}, false},
		{"verified cause executable with unverified peer", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) { parityProgram(t, r) }, nil, false},
		{"unverified peer program", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) { parityProgram(t, r) }, []string{"reflection.executable-cause"}, false},
		{"unverified peer script", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/base/scripts/fix.sh", "#!/bin/bash\nset -eu\nkubectl rollout restart \"$1\"\n# unverified peer\n", 0755)
		}, []string{"reflection.executable-cause"}, false},
		{"unverified peer library", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) { put(t, r, ".sdo/lib/unbound/helper.go", "package unbound\n", 0644) }, []string{"reflection.executable-cause"}, false},
		{"unverified peer executable", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", parityDetector(t, "cache")+"\n// Unverified peer signature.\n", 0644)
		}, []string{"reflection.executable-cause"}, false},
		{"unchanged good tree", validate.Responder, validate.ReflectionPhase, func(*testing.T, string) {}, nil, false},
		{"responder adds a detector", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", strings.Replace(incidentManifest, "%s", "base", 1), 0o644)
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", parityDetector(t, "dns"), 0o644)
		}, nil, false},
		{"detector watches a resource the controller cannot load", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			watchDetector(t, r, "policy", "v1", "PodDisruptionBudget")
		}, []string{validate.RuleDetectorControllerStart}, false},
		{"detector watches PersistentVolumeClaims", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			watchDetector(t, r, "", "v1", "PersistentVolumeClaim")
		}, nil, false},
		{"detector names a missing playbook", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", strings.Replace(incidentManifest, "%s", "ghost", 1), 0o644)
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", parityDetector(t, "dns"), 0o644)
		}, []string{"ref.unknown-playbook"}, false},
		{"unclassified file", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/notes.txt", "scratch", 0o644)
		}, []string{"layout.unknown-path", "ownership.denied"}, false},
		{"responder edits the goal", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\napplication: demo\n---\nChanged.\n", 0o644)
		}, []string{"ownership.denied"}, false},
		{"script not executable", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/base/scripts/fix.sh", "#!/bin/bash\ntrue\n", 0o644)
		}, []string{"script.not-executable"}, false},
		{"physical path in playbook text", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/base/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nSee .sdo/playbooks/other/README.md. Repair <RESOURCE_NAME>. Run fix.sh.\n", 0o644)
		}, []string{"text.physical-path"}, false},
		{"malformed playbook ID", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/Bad_ID/README.md", "x", 0o644)
		}, []string{"id.pattern", "ownership.denied"}, false},
		{"compile failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", strings.Replace(parityDetector(t, "cache"), "return Detector{}", "return undefinedValue", 1), 0o644)
		}, []string{validate.RuleDetectorCompile}, false},
		{"vet failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			source := strings.Replace(parityDetector(t, "cache"), `"context"`, `"context"; "fmt"`, 1)
			source = strings.Replace(source, "return Detector{}", `_ = fmt.Sprintf("%d", "bad"); return Detector{}`, 1)
			formatted, err := format.Source([]byte(source))
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", string(formatted), 0o644)
		}, []string{validate.RuleDetectorVet}, false},
		{"detector test failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector_test.go", `package detector
import "testing"
func TestReject(t *testing.T){t.Fatal("sentinel detector failure")}
`, 0o644)
		}, []string{validate.RuleDetectorTest}, false},

		{"unknown actor", "stranger", validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/base/scripts/fix.sh", "#!/bin/bash\ntrue\n", 0o755)
		}, []string{"ownership.unknown-actor"}, false},
	}
	cases = append(cases, []parityCase{
		{"snapshot projection", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			old := ".sdo/diagnostics/detectors/incidents/cache/detector.go"
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/implementation.go", parityDetector(t, "cache"), 0644)
			if err := os.Remove(filepath.Join(r, old)); err != nil {
				t.Fatal(err)
			}
		}, []string{validate.RuleSnapshotProjection}, false},

		{"trailing whitespace normalized", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/playbooks/base/README.md", "Repair", "Repair \n")
		}, nil, false},
		{"space before tab indent", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/playbooks/base/README.md", "Repair", " \tRepair")
		}, []string{validate.RuleWhitespace}, false},
		{"aggregate snapshot budget", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			for _, id := range []string{"large-a", "large-b", "large-c"} {
				put(t, r, ".sdo/playbooks/"+id+"/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: "+id+"\noriginating_incident: incident\n---\nRepair <RESOURCE_NAME>.\n"+strings.Repeat("Verified guidance paragraph.\n", 7000), 0644)
			}
			replaceParityText(t, r, ".sdo/playbooks/README.md", "# Playbooks", "# Playbooks\n- [large-a](large-a/README.md)\n- [large-b](large-b/README.md)\n- [large-c](large-c/README.md)")
		}, []string{validate.RuleSnapshotBudget}, false},
		{"always firing on healthy baseline", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", strings.Replace(parityDetector(t, "cache"), "return nil, nil", `return []sdk.Finding{{DetectorID:"cache",RuleID:"always",Status:sdk.FindingActive}}, nil`, 1), 0644)
		}, []string{validate.RuleDetectorHealthyBaseline}, false},
		{"missing accepted outcome", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/playbooks/base/README.md", "Repair", "Carefully repair")
		}, []string{validate.RuleAcceptedOutcome}, false},
		{"decision IDs absent from diff", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/playbooks/base/README.md", "Repair", "Carefully repair")
		}, []string{validate.RuleReflectionDecision}, false},
		{"no_change with edits", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/playbooks/base/README.md", "Repair", "Carefully repair")
		}, []string{validate.RuleReflectionDecision}, false},
		{"initial unverified source", validate.Deployer, validate.InitialPhase, func(t *testing.T, r string) { put(t, r, "app.txt", "changed source\n", 0644) }, []string{validate.RuleBootstrapClosed, validate.RuleVerifiedSource}, false},
	}...)

	// Reuse the validator’s bad trees through both public entry points.
	// Ownership may add diagnostics; every fixture must still name its rule.
	for name, rule := range map[string]string{
		"unknown-path":                  validate.RuleUnknownPath,
		"invalid-id":                    validate.RuleInvalidID,
		"secret-path":                   validate.RuleSecretPath,
		"legacy-manifest":               validate.RuleLegacyManifest,
		"schema-version":                validate.RuleSchemaVersion,
		"yaml-parse":                    validate.RuleYAMLParse,
		"go-syntax":                     validate.RuleGoSyntax,
		"document-parse":                validate.RuleDocumentParse,
		"manifest-parse":                validate.RuleManifestParse,
		"manifest-invalid":              validate.RuleManifestInvalid,
		"manifest-location":             validate.RuleManifestLocation,
		"goal-missing":                  validate.RuleGoalMissing,
		"architecture-missing":          validate.RuleArchitectureMissing,
		"goal-owner":                    validate.RuleGoalOwner,
		"manifest-missing":              validate.RuleManifestMissing,
		"detector-none":                 validate.RuleDetectorNone,
		"detector-no-source":            validate.RuleDetectorNoSource,
		"duplicate-id":                  validate.RuleDuplicateID,
		"unknown-playbook":              validate.RuleUnknownPlaybook,
		"playbook-none":                 validate.RulePlaybookNone,
		"playbook-missing-readme":       validate.RulePlaybookNoReadme,
		"playbook-owner":                validate.RulePlaybookOwner,
		"playbook-fault-class":          validate.RulePlaybookFaultClass,
		"playbook-originating-incident": validate.RulePlaybookIncident,
		"playbook-placeholder":          validate.RulePlaybookPlaceholder,
		"index-missing":                 validate.RuleIndexMissing,
		"index-link":                    validate.RuleIndexLink,
		"playbook-not-indexed":          validate.RulePlaybookNotIndexed,
		"script-unreferenced":           validate.RuleScriptUnreferenced,
		"script-syntax":                 validate.RuleScriptSyntax,
		"physical-path":                 validate.RulePhysicalPath,
		"detector-physical-path":        validate.RulePhysicalPath,
		"manifest-playbook-path":        validate.RuleManifestInvalid,
	} {
		cases = append(cases, parityCase{name: "fixture/" + name, actor: validate.Responder, phase: validate.ReflectionPhase,
			edit: func(t *testing.T, root string) { overlayParityCase(t, root, name) }, want: []string{rule}, allowAdditional: true})
	}
	cases = append(cases, []parityCase{
		{name: "unsafe path", actor: validate.Responder, phase: validate.ProposalPhase, edit: func(t *testing.T, r string) {
			put(t, r, `unsafe\path.txt`, "change\n", 0o644)
		}, want: []string{validate.RuleUnsafePath}},
		{name: "source YAML syntax", actor: validate.Responder, phase: validate.ProposalPhase, edit: func(t *testing.T, r string) {
			put(t, r, "service.yaml", "kind: Service\nmetadata:\n  name: a\n  name: b\n", 0o644)
		}, want: []string{validate.RuleYAMLParse}},
		{name: "source Go syntax", actor: validate.Responder, phase: validate.ProposalPhase, edit: func(t *testing.T, r string) {
			put(t, r, "repair.go", "package repair\nfunc Broken( {\n", 0o644)
		}, want: []string{validate.RuleGoSyntax}},
		{name: "responder firing threshold", actor: validate.Responder, phase: validate.ReflectionPhase, edit: func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", "firing: 1", "firing: 2")
		}, want: []string{validate.RuleResponderFiring}},
		{name: "preserve detector provenance", actor: validate.Responder, phase: validate.ReflectionPhase, edit: func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", "originatingIncident: inc-1", "originatingIncident: other")
		}, want: []string{validate.RuleProvenanceRewrite}},
		{name: "protected health registration", actor: validate.Responder, phase: validate.ReflectionPhase, edit: func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/diagnostics/detectors/health/health/manifest.yaml", "interval: 30s", "interval: 20s")
		}, want: []string{validate.RuleOwnershipDenied, validate.RuleProtectedDetector}},
		{name: "upkeep requires migration", actor: validate.Upkeep, phase: validate.InitialPhase, edit: func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package detector", "package detector\n// changed")
		}, want: []string{validate.RuleUpkeepMigration}},
		{name: "unknown phase", actor: validate.Responder, phase: "unknown", edit: func(t *testing.T, r string) {
			put(t, r, "app.txt", "change\n", 0o644)
		}, want: []string{validate.RuleUnknownPhase}},
		{name: "symlink", actor: validate.Responder, phase: validate.ReflectionPhase, edit: func(t *testing.T, r string) {
			if err := os.Symlink("goal.md", filepath.Join(r, ".sdo/link.md")); err != nil {
				t.Fatal(err)
			}
		}, want: []string{validate.RuleSymlink}, allowAdditional: true},
		{name: "nonregular file", actor: validate.Responder, phase: validate.ReflectionPhase, edit: func(t *testing.T, r string) {
			if err := syscall.Mkfifo(filepath.Join(r, ".sdo/goal.pipe"), 0o644); err != nil {
				t.Fatal(err)
			}
			replaceParityText(t, r, ".sdo/playbooks/base/README.md", "Repair", "Carefully repair")
		}, want: []string{validate.RuleNotRegular}, allowAdditional: true},
		{name: "oversized playbook", actor: validate.Responder, phase: validate.ReflectionPhase, edit: func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/playbooks/base/README.md", "Repair", strings.Repeat("x", 300*1024)+"Repair")
		}, want: []string{validate.RuleTooLarge}, allowAdditional: true},
		{name: "initial judge needs manifest", actor: validate.HealthJudge, phase: validate.InitialPhase, edit: func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/playbooks/README.md", "# Playbooks", "# Playbooks\nMore")
		}, want: []string{validate.RuleInitialNeedsDetectors}},
		{name: "initial judge health only", actor: validate.HealthJudge, phase: validate.InitialPhase, edit: func(t *testing.T, r string) {
			if err := os.RemoveAll(filepath.Join(r, ".sdo/diagnostics/detectors")); err != nil {
				t.Fatal(err)
			}
			runGit(t, r, "add", "-A")
			runGit(t, r, "commit", "-qm", "before detectors")
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", strings.Replace(incidentManifest, "%s", "base", 1), 0o644)
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", parityDetector(t, "dns"), 0o644)
		}, want: []string{validate.RuleInitialHealthOnly}, allowAdditional: true},
		{name: "architecture accepted source", actor: validate.Deployer, phase: validate.InitialPhase, edit: func(t *testing.T, r string) {
			replaceParityText(t, r, ".sdo/arch.md", "generated_at_commit:", "generated_at_commit: deadbeef #")
		}, want: []string{validate.RuleArchitectureBaseline}},
		{name: "architecture source binding", actor: validate.Deployer, phase: validate.InitialPhase, edit: func(t *testing.T, r string) {
			put(t, r, "app.txt", "deployed\n", 0o644)
			runGit(t, r, "add", "-A")
			runGit(t, r, "commit", "-qm", "source\n\nSDO-Actor: deployer\nSDO-Phase: initial")
			replaceParityText(t, r, ".sdo/arch.md", "generated_at_commit:", "generated_at_commit: deadbeef #")
		}, want: []string{validate.RuleArchitectureCommit}},
		{name: "architecture alone", actor: validate.Deployer, phase: validate.InitialPhase, edit: func(t *testing.T, r string) {
			put(t, r, "app.txt", "changed\n", 0o644)
			runGit(t, r, "add", "-A")
			runGit(t, r, "commit", "-qm", "source\n\nSDO-Actor: deployer\nSDO-Phase: initial")
			head, err := exec.Command("git", "-C", r, "rev-parse", "HEAD").Output()
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, ".sdo/arch.md", "---\nschema_version: 1\ngenerated_at_commit: "+strings.TrimSpace(string(head))+"\n---\nArchitecture\n", 0o644)
			put(t, r, "app.txt", "changed again\n", 0o644)
		}, want: []string{validate.RuleArchitectureAlone}},
	}...)
	cases = append(cases, []parityCase{
		{"direct program", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) { parityProgram(t, r) }, nil, false},
		{"program repeat has no detector", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			if err := os.RemoveAll(filepath.Join(r, ".sdo/diagnostics/detectors/incidents/routing-relations")); err != nil {
				t.Fatal(err)
			}
		}, []string{validate.RuleProgramRepeatReachable}, false},
		{"program repeat missing seven deployment roles", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			path := filepath.Join(r, ".sdo/playbooks/service-targetport-mismatch/diagnose.go")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var roles strings.Builder
			for _, name := range []string{"deployment", "search", "rate", "reservation", "profile", "recommendation", "user"} {
				fmt.Fprintf(&roles, ", {Name:%q,Kind:\"Deployment\",Required:true}", name)
			}
			source := strings.Replace(string(data), `Required: true}}}`, `Required: true}`+roles.String()+`}}`, 1)
			if source == string(data) {
				t.Fatal("fixture spec was not changed")
			}
			put(t, r, ".sdo/playbooks/service-targetport-mismatch/diagnose.go", source, 0644)
		}, []string{validate.RuleProgramRepeatReachable}, false},

		{"program mitigation leaves target port faulty", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			put(t, r, ".sdo/playbooks/service-targetport-mismatch/mitigate.go", `package targetport
import ("context"; "sdo.dev/operator/sdk/program")
func (p Program) Mitigate(ctx context.Context,v program.ReadView,b program.Bindings)(program.Plan,error){
 verdict,err:=p.Diagnose(ctx,v,b);if err!=nil || verdict.Result!=program.Match{return program.Plan{},err}
 return program.Plan{Actions:[]program.Action{program.SetField{Ref:b.Objects["service"],Path:"spec.ports[0].targetPort",Value:program.FromObserved(b.Objects["service"],"spec.ports[0].targetPort")}}},nil
}
`, 0644)
		}, []string{"program.mitigation-not-repaired"}, false},
		{"program mitigation skips unreplayable effect", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			put(t, r, ".sdo/playbooks/service-targetport-mismatch/mitigate.go", `package targetport
import ("context"; "sdo.dev/operator/sdk/program")
func (p Program) Mitigate(ctx context.Context,v program.ReadView,b program.Bindings)(program.Plan,error){
 verdict,err:=p.Diagnose(ctx,v,b);if err!=nil || verdict.Result!=program.Match{return program.Plan{},err}
 return program.Plan{Actions:[]program.Action{program.ReconcileFromSource{Ref:b.Objects["service"]}}},nil
}
`, 0644)
		}, []string{"program.mitigation-not-repaired"}, false},

		{"program README physical path", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			path := filepath.Join(r, ".sdo/playbooks/service-targetport-mismatch/README.md")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, ".sdo/playbooks/service-targetport-mismatch/README.md", string(data)+"\nSee .sdo/goal.md.\n", 0644)
		}, []string{"text.physical-path"}, false},
		{"program import policy", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			path := ".sdo/playbooks/service-targetport-mismatch/diagnose.go"
			data, err := os.ReadFile(filepath.Join(r, path))
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, path, strings.Replace(string(data), "import (", "import (\n _ \"os/exec\"", 1), 0644)
		}, []string{"program.invalid"}, false},
		{"program compile failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			path := ".sdo/playbooks/service-targetport-mismatch/diagnose.go"
			data, err := os.ReadFile(filepath.Join(r, path))
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, path, strings.Replace(string(data), "type Program struct{}", "type Program struct{ missing UndefinedType }", 1), 0644)
		}, []string{validate.RuleDetectorCompile}, false},
		{"program replay failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			path := ".sdo/diagnostics/detectors/incidents/routing-relations/detector.go"
			data, err := os.ReadFile(filepath.Join(r, path))
			if err != nil {
				t.Fatal(err)
			}
			source := strings.Replace(string(data), `ref := sdk.ObjectRefFrom("Service", "v1", &service)`, `ref := sdk.ObjectRefFrom("Service", "v1", &service); ref.Name = "not-observed"`, 1)
			put(t, r, path, source, 0644)
		}, []string{validate.RuleProgramRepeatReachable}, false},

		{"program corpus provenance", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			path := ".sdo/corpus/targetport-mismatch/labels.json"
			data, err := os.ReadFile(filepath.Join(r, path))
			if err != nil {
				t.Fatal(err)
			}
			var label map[string]any
			if err := json.Unmarshal(data, &label); err != nil {
				t.Fatal(err)
			}
			delete(label, "provenance")
			data, err = json.Marshal(label)
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, path, string(data), 0644)
		}, nil, false},

		{"program README shell policy", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			path := ".sdo/playbooks/service-targetport-mismatch/README.md"
			data, err := os.ReadFile(filepath.Join(r, path))
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, path, string(data)+"\n```sh\nkubectl get pods\n```\n", 0644)
		}, []string{"program.invalid"}, false},
		{"library import policy", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			put(t, r, ".sdo/lib/ports/ports.go", "package ports\nimport _ \"os/exec\"\n", 0644)
		}, []string{"program.invalid"}, false},
		{"trusted captured program", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			parityCaptures(t, r)
		}, nil, false},
		{"captured bytes differ", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			parityCaptures(t, r)
			path := ".sdo/corpus/targetport-mismatch/recordings.json"
			data, err := os.ReadFile(filepath.Join(r, path))
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, path, string(data)+" ", 0644)
		}, []string{"diff.whitespace"}, false},

		{"positive corpus without capture environment", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			if err := os.RemoveAll(filepath.Join(filepath.Dir(r), "captures")); err != nil {
				t.Fatal(err)
			}
		}, nil, false},
		{"positive corpus with empty incident ID", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			parityLabel(t, r, func(label map[string]any) { label["provenance"].(map[string]any)["incident_id"] = "" })
		}, nil, false},
		{"positive corpus references another incident", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityProgram(t, r)
			parityLabel(t, r, func(label map[string]any) { label["provenance"].(map[string]any)["incident_id"] = "other" })
		}, nil, false},
		{"existing program adds unproven positive case", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			if err := os.CopyFS(filepath.Join(r, ".sdo/corpus/another-case"), os.DirFS(filepath.Join(r, ".sdo/corpus/targetport-mismatch"))); err != nil {
				t.Fatal(err)
			}
			parityCaseLabel(t, r, "another-case", func(label map[string]any) { delete(label, "provenance") })
		}, nil, false},
		{"existing program adds captured positive case", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			if err := os.CopyFS(filepath.Join(r, ".sdo/corpus/another-case"), os.DirFS(filepath.Join(r, ".sdo/corpus/targetport-mismatch"))); err != nil {
				t.Fatal(err)
			}
		}, nil, false},
		{"existing program adds synthetic negative case without captures", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			dir := filepath.Join(r, ".sdo/corpus/another-case")
			if err := os.CopyFS(dir, os.DirFS(filepath.Join(r, ".sdo/corpus/targetport-mismatch"))); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
			if err != nil {
				t.Fatal(err)
			}
			put(t, r, ".sdo/corpus/another-case/open.json", string(data), 0644)
			parityCaseLabel(t, r, "another-case", func(label map[string]any) {
				delete(label, "provenance")
				delete(label, "root_cause_program_ids")
				label["no_match_program_ids"] = []string{"service-targetport-mismatch"}
				label["synthetic_no_match_reason"] = "healthy same-kind peer has the correct target port"
			})
			if err := os.RemoveAll(filepath.Join(filepath.Dir(r), "captures")); err != nil {
				t.Fatal(err)
			}
		}, nil, false},
		{"existing program erases accepted provenance", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			parityLabel(t, r, func(label map[string]any) { delete(label, "provenance") })
		}, []string{"program.corpus-immutable"}, false},
		{"existing program rewrites accepted labels", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			parityLabel(t, r, func(label map[string]any) { label["bindings"] = label["peer_bindings"] })
		}, []string{"program.corpus-immutable"}, false},
		{"existing program rewrites accepted incident", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			parityLabel(t, r, func(label map[string]any) { label["provenance"].(map[string]any)["incident_id"] = "other" })
		}, []string{"program.corpus-immutable"}, false},
		{"existing program deletes accepted case", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			if err := os.CopyFS(filepath.Join(r, ".sdo/corpus/retained-case"), os.DirFS(filepath.Join(r, ".sdo/corpus/targetport-mismatch"))); err != nil {
				t.Fatal(err)
			}
			runGit(t, r, "add", "-f", ".sdo/corpus")
			runGit(t, r, "commit", "-qm", "accept another case before proposing deletion")
			if err := os.RemoveAll(filepath.Join(r, ".sdo/corpus/targetport-mismatch")); err != nil {
				t.Fatal(err)
			}
		}, []string{"program.corpus-immutable", "program.invalid"}, false},
		{"existing program erases accepted recordings", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			put(t, r, ".sdo/corpus/targetport-mismatch/recordings.json", "[]", 0644)
		}, []string{"program.corpus-immutable"}, false},
		{"existing program changes accepted snapshot", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			put(t, r, ".sdo/corpus/targetport-mismatch/open.json", string(capturefixture.Raw(t, "forged capture", []byte(`{ "namespace": "forged" }`))), 0644)
		}, []string{"program.corpus-immutable"}, false},
		{"accepted case needs no current capture", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			parityAcceptedProgram(t, r)
			if err := os.RemoveAll(filepath.Join(filepath.Dir(r), "captures")); err != nil {
				t.Fatal(err)
			}
			put(t, r, ".sdo/playbooks/base/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nRepair <RESOURCE_NAME>. Run fix.sh.\nAdditional guidance.\n", 0644)
		}, nil, false},
	}...)
	for index, test := range cases {
		if index%2 != shard {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			// Each verdict owns its repositories and captures; only the compiled
			// CLI and read-only healthy baseline are shared. CI caps -parallel=2.
			t.Parallel()
			root := copyParityTree(t)
			put(t, root, "app.txt", "base\n", 0o644)
			testutil.InitGit(t, root)
			runGit(t, root, "config", "user.name", "T")
			runGit(t, root, "config", "user.email", "t@example.com")
			runGit(t, root, "add", "-A")
			runGit(t, root, "commit", "-q", "-m", "base")
			test.edit(t, root)
			// Git omits FIFOs; the proposal sandbox still contains them, and the
			// structural validator must reject them before publication copies files.
			var nonregular []string
			if err := filepath.WalkDir(root, func(p string, e fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				rel, _ := filepath.Rel(root, p)
				if rel == ".git" {
					return filepath.SkipDir
				}
				if !e.IsDir() && !e.Type().IsRegular() {
					nonregular = append(nonregular, rel)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			paths, err := validate.ChangedPaths(context.Background(), root, "HEAD")
			if err != nil {
				t.Fatal(err)
			}
			accepted := filepath.Join(t.TempDir(), "accepted")
			testutil.CloneGit(t, root, accepted, "--no-local")
			repository, err := broker.New(accepted, t.TempDir(), broker.JobValidator(subprocessParityJob{binary: binary, operatorRoot: operatorRoot, schema: schema, baseline: baseline}))
			if err != nil {
				t.Fatal(err)
			}
			if test.phase == validate.ReflectionPhase && test.name != "missing accepted outcome" {
				outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "incident", Classification: "success"}
				if len(paths) > 0 {
					refs := []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}
					outcome.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Summary: "independently verified fixture repair", Resources: refs}}
					outcome.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: refs, Items: []*contracts.ReflectionWorkItem{{ProgramId: "base", Disposition: "covered", Reason: "verified_equivalent_repair", Resources: refs}}}}}
				}
				if strings.HasPrefix(test.name, "unverified peer ") || test.name == "verified cause executable with unverified peer" {
					a := []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "verified"}}
					if test.name == "verified cause executable with unverified peer" {
						a[0].Name = "front"
					}
					b := []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "unverified"}}
					outcome.Classification = "failed"
					outcome.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Summary: "verified A", Resources: a}, {Summary: "unverified B", Resources: b}}
					outcome.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Causes: []*contracts.ReflectionCauseWork{{CauseIndex: 0, Resources: a}}}
					if test.name == "unverified peer no verified" {
						outcome.ReflectionEligibility.Causes = nil
					}
				}
				parityFindingRepairProof(outcome)
				if test.name == "successful cause without matching opening finding" {
					outcome.Findings[0].PrimaryResource = &contracts.ObjectRef{Kind: "Service", Namespace: "demo", Name: "actual-fault"}
				}
				if _, err := repository.RecordOutcome(context.Background(), "incident", outcome); err != nil {
					t.Fatal(err)
				}
			}
			proposalPhase := test.phase
			if proposalPhase == "unknown" {
				proposalPhase = validate.ProposalPhase
			}
			proposal, err := repository.CreateProposalForPhase(context.Background(), "incident", proposalPhase)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range append(paths, nonregular...) {
				copyParityChange(t, root, proposal.Path, name)
			}
			capture := filepath.Join(filepath.Dir(root), "captures")
			if _, err := os.Stat(capture); os.IsNotExist(err) {
				capture = ""
			}
			if capture == "" {
				capture = t.TempDir()
			}
			if test.name != "missing controller healthy capture" {
				put(t, capture, "healthy/healthy.json", string(capturefixture.FromView(t, `{"namespace":"demo","services":[],"pods":[]}`)), 0644)
			}
			if strings.HasPrefix(test.name, "healthy refused") {
				// The legacy/caller-authored healthy.json has no Consul ports.
				// Only the controller capture retains the original healthy app.
				snapshot := map[string]any{"namespace": "demo", "services": []any{map[string]any{"metadata": map[string]any{"name": "consul", "namespace": "demo"}, "spec": map[string]any{"ports": []any{map[string]any{"port": 8300}, map[string]any{"port": 8400}}}}}}
				if test.name == "healthy refused service ports" {
					snapshot["readings"] = []any{
						map[string]any{"method": "ProbeTCP", "arguments": []any{map[string]any{"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "consul"}, 8300}, "result": map[string]any{"success": false, "detail": "refused"}},
						map[string]any{"method": "ProbeTCP", "arguments": []any{map[string]any{"api_version": "v1", "kind": "Service", "namespace": "demo", "name": "consul"}, 8400}, "result": map[string]any{"success": false, "detail": "refused"}},
					}
				}
				put(t, capture, "healthy/healthy.json", string(capturefixture.FromView(t, snapshot)), 0644)
			}
			root = proposal.Path
			decision := &contracts.ReflectionDecision{LearningDecision: "updated"}
			if len(paths) > 0 {
				// Coverage of this synthetic verified cause is intentionally
				// declined; executable authorization is checked independently.
				resource := "front"
				decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "fixture", Primitive: "fixture", Reason: "fixture coverage gap", ScratchScript: "scratch/fixture", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: resource}}}}
			}
			if len(paths) == 0 {
				decision.LearningDecision = "no_change"
				decision.NoChangeReason = "already covered"
			}
			if strings.HasPrefix(test.name, "unverified peer ") || test.name == "verified cause executable with unverified peer" {
				decision.SdkGaps = []*contracts.SdkGap{{FaultClass: "verified", Primitive: "missing", Reason: "SDK gap for verified A", ScratchScript: "scratch/a", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "verified"}}}}
				if test.name == "verified cause executable with unverified peer" {
					decision.SdkGaps[0].Resources[0].Name = "front"
				}
				if test.name == "unverified peer gaps" {
					decision.LearningDecision = "sdk_gap"
					decision.NoChangeReason = "typed gaps for both verified and unverified causes"
					decision.SdkGaps = append(decision.SdkGaps, &contracts.SdkGap{FaultClass: "unverified", Primitive: "independent-evidence", Reason: "evidence_gap", ScratchScript: "scratch/b", Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "unverified"}}})
				}
			}
			ids := map[string]bool{}
			for _, p := range paths {
				kind, id, ok := layout.Classify(p)
				id, _, _ = strings.Cut(id, "/")
				if ok && (kind == layout.KindPlaybook || kind == layout.KindPlaybookScript || kind == layout.KindPlaybookProgram) {
					ids[id] = true
				}
			}
			for id := range ids {
				decision.PlaybookIds = append(decision.PlaybookIds, id)
			}
			sort.Strings(decision.PlaybookIds)
			if test.name == "decision IDs absent from diff" {
				decision.PlaybookIds = []string{"unrelated"}
			}
			if test.name == "no_change with edits" {
				decision.LearningDecision = "no_change"
				decision.NoChangeReason = "covered"
			}
			if test.phase == validate.ReflectionPhase {
				// The decision file is the ReflectionResult the worker returns.
				data, err := protojson.Marshal(&contracts.ReflectionResult{
					LearningDecision: decision.LearningDecision, NoChangeReason: decision.NoChangeReason,
					ProposedArtifactIds: decision.PlaybookIds, SdkGaps: decision.SdkGaps,
				})
				if err != nil {
					t.Fatal(err)
				}
				put(t, root, ".git/"+validate.DecisionFile, string(data), 0600)
			}

			var stdout, stderr bytes.Buffer
			command := exec.Command(binary, "memory", "check", "--json")
			command.Dir = root
			command.Env = append(os.Environ(), "SDO_ACTOR="+string(test.actor), "SDO_PHASE="+string(test.phase), "SDO_BASE_COMMIT="+proposal.BaseCommit, "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE="+baseline, "SDO_CAPTURED_CORPUS_DIR="+capture, "SDO_INCIDENT_ID=incident")
			command.Stdout, command.Stderr = &stdout, &stderr
			code := 0
			if err := command.Run(); err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				code = exit.ExitCode()
			}
			if code > 1 {
				t.Fatalf("CLI infrastructure error %d: %s", code, stderr.String())
			}
			report := &contracts.MemoryCheckReport{}
			if err := protojson.Unmarshal(stdout.Bytes(), report); err != nil {
				t.Fatalf("CLI output is not a MemoryCheckReport: %v\n%s", err, stdout.String())
			}

			var publishErr error
			var receipt broker.Receipt
			if test.phase == validate.ReflectionPhase && test.actor == validate.Responder {
				receipt, publishErr = repository.PublishReflection(validate.WithCaptureSource(context.Background(), capture, "incident"), "incident", decision)
			} else {
				receipt, publishErr = repository.Publish(context.Background(), "incident", test.actor, test.phase)
			}
			err = publishErr
			head, headErr := exec.Command("git", "-C", accepted, "rev-parse", "HEAD").Output()
			if headErr != nil {
				t.Fatal(headErr)
			}
			if err != nil || len(paths) == 0 {
				if strings.TrimSpace(string(head)) != proposal.BaseCommit {
					t.Fatal("rejected or no-op candidate changed accepted HEAD")
				}
			} else {
				if receipt.Commit == "" || receipt.Commit != strings.TrimSpace(string(head)) {
					t.Fatalf("publication omitted accepted commit: %+v", receipt)
				}
				candidateTree, treeErr := broker.ProposalTreeDigest(context.Background(), root)
				if treeErr != nil {
					t.Fatal(treeErr)
				}
				acceptedTree, treeErr := exec.Command("git", "-C", accepted, "rev-parse", "HEAD^{tree}").Output()
				if treeErr != nil {
					t.Fatal(treeErr)
				}
				if candidateTree != strings.TrimSpace(string(acceptedTree)) {
					t.Fatal("publication changed authored bytes")
				}
			}
			var brokerDiagnostics []validate.Diagnostic

			var failure *validate.Failure
			switch {
			case err == nil:
			case errors.As(err, &failure):
				brokerDiagnostics = failure.Diagnostics
			default:
				t.Fatalf("broker infrastructure error: %v", err)
			}

			if verdictCLI, verdictBroker := code == 0, err == nil; verdictCLI != verdictBroker {
				t.Fatalf("verdicts differ: CLI clean=%v broker clean=%v\nCLI diagnostics: %v\nbroker diagnostics: %v\nCLI stderr: %s", verdictCLI, verdictBroker, report.Diagnostics, brokerDiagnostics, stderr.String())
			}
			if got := rules(report.Diagnostics, brokerDiagnostics); !reflect.DeepEqual(got.cli, got.broker) {
				t.Fatalf("rule sets differ: CLI %v broker %v", got.cli, got.broker)
			}
			if len(report.Diagnostics) != len(brokerDiagnostics) {
				t.Fatalf("diagnostic counts differ: CLI %v broker %v", report.Diagnostics, brokerDiagnostics)
			}
			for i, want := range validate.ProtoDiagnostics(brokerDiagnostics) {
				if got := report.Diagnostics[i]; got.Rule != want.Rule || got.Path != want.Path || got.Line != want.Line || got.Message != want.Message || got.Hint != want.Hint {
					t.Errorf("diagnostic %d differs: CLI %v broker %v", i, got, want)
				}
			}
			got := rules(report.Diagnostics, nil).cli
			want := append([]string(nil), test.want...)
			if test.phase == validate.InitialPhase && test.actor != validate.Upkeep && test.actor != validate.HealthJudge && test.name != "initial unverified source" {
				want = append(want, validate.RuleBootstrapClosed)
			}
			if test.name == "architecture alone" {
				want = append(want, validate.RuleVerifiedSource)
			}
			sort.Strings(want)

			if test.allowAdditional {
				for _, rule := range want {
					if !slices.Contains(got, rule) {
						t.Errorf("missing rule %s: %v", rule, report.Diagnostics)
					}
				}
			} else if !reflect.DeepEqual(got, want) {
				t.Fatalf("rules = %v, want %v\n%v", got, want, report.Diagnostics)
			}
		})
	}
}

type ruleSets struct{ cli, broker []string }

func rules(cli []*contracts.MemoryDiagnostic, broker []validate.Diagnostic) ruleSets {
	collect := func(add func(func(string))) []string {
		seen := map[string]bool{}
		add(func(rule string) { seen[rule] = true })
		var out []string
		for rule := range seen {
			out = append(out, rule)
		}
		sort.Strings(out)
		return out
	}
	return ruleSets{
		cli: collect(func(f func(string)) {
			for _, d := range cli {
				f(d.Rule)
			}
		}),
		broker: collect(func(f func(string)) {
			for _, d := range broker {
				f(d.Rule)
			}
		}),
	}
}

func parityDetector(t *testing.T, id string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(parityFixture, ".sdo/diagnostics/detectors/incidents/cache/detector.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := strings.ReplaceAll(string(data), `"cache"`, strconv.Quote(id))
	if id == "dns" {
		source = strings.ReplaceAll(source, `"inc-1"`, `"inc-2"`)
	}
	return source
}

// watchDetector adds an incident detector whose manifest and spec both declare
// one watch of the given resource.
func watchDetector(t *testing.T, root, group, version, kind string) {
	t.Helper()
	apiVersion := version
	if group != "" {
		apiVersion = group + "/" + version
	}
	manifest := strings.Replace(strings.Replace(incidentManifest, "%s", "base", 1), "  - apiVersion: v1\n    kind: Pod\n", "  - apiVersion: "+apiVersion+"\n    kind: "+kind+"\n", 1)
	put(t, root, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", manifest, 0o644)
	source := strings.Replace(parityDetector(t, "dns"), `{APIVersion: "v1", Kind: "Pod"}`, fmt.Sprintf("{APIVersion: %q, Kind: %q}", apiVersion, kind), 1)
	put(t, root, ".sdo/diagnostics/detectors/incidents/dns/detector.go", source, 0o644)
}

// overlayParityCase preserves executable bits and applies the fixture’s deletions.
func overlayParityCase(t *testing.T, root, name string) {
	t.Helper()
	dir := filepath.Join("../memory/validate/testdata/bad", name)
	if deleted, err := os.ReadFile(filepath.Join(dir, "DELETE")); err == nil {
		for _, rel := range strings.Fields(string(deleted)) {
			if err := os.RemoveAll(filepath.Join(root, rel)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() == "DELETE" {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		put(t, root, rel, string(data), info.Mode().Perm())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func replaceParityText(t *testing.T, root, rel, before, after string) {
	t.Helper()
	path := filepath.Join(root, rel)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), before) {
		t.Fatalf("%s does not contain %q", rel, before)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	put(t, root, rel, strings.Replace(string(data), before, after, 1), info.Mode().Perm())
}

// subprocessParityJob replaces only cluster scheduling/isolation, which are
// outside acceptance rules. Publication stages the real tree and invokes the
// shipped validator binary, including healthy replay and its report transport.
type subprocessParityJob struct{ binary, operatorRoot, schema, baseline string }

func (j subprocessParityJob) Validate(ctx context.Context, r broker.ValidationJobRequest) (broker.ValidationJobReceipt, error) {
	args := []string{"validate", "--app", r.CandidateRoot, "--operator-root", j.operatorRoot, "--expect-schema", j.schema}
	needed, err := validate.BaselineGateRequired(r.CandidateRoot, r.ChangedPaths)
	if err != nil {
		return broker.ValidationJobReceipt{}, err
	}
	if needed {
		baseline := j.baseline
		if r.HealthyBaseline != "" {
			baseline = r.HealthyBaseline
		}
		args = append(args, "--healthy-baseline", baseline)
	}
	cmd := exec.CommandContext(ctx, j.binary, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	diagnostics, err := validate.ParseReport(stdout.String())
	if err != nil {
		return broker.ValidationJobReceipt{}, fmt.Errorf("validator subprocess: %v: %v: %s", runErr, err, stderr.String())
	}
	if err := validate.Err(diagnostics); err != nil {
		return broker.ValidationJobReceipt{}, err
	}
	if runErr != nil {
		return broker.ValidationJobReceipt{}, runErr
	}
	return broker.ValidationJobReceipt{CandidateTree: r.CandidateTree, Accepted: true, DetectorCompiled: true, SourceValidated: true, ReadOnlyMountObserved: true, NoPublishingCredentialsObserved: true, EgressIsolationCanaryPassed: true}, nil
}

func copyParityChange(t *testing.T, source, target, name string) {
	t.Helper()
	from, to := filepath.Join(source, name), filepath.Join(target, name)
	if err := os.RemoveAll(to); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(from); errors.Is(err, os.ErrNotExist) {
		return
	} else if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("cp", "-a", "--", from, to).CombinedOutput(); err != nil {
		t.Fatalf("copy candidate %s: %v %s", name, err, out)
	}
}

func parityProgram(t *testing.T, root string) {
	t.Helper()
	fixture := "../../../tests/fixtures/sdo/playbook_program_app/.sdo"
	testutil.CopyPortProgram(t, root, fixture)
	if err := os.CopyFS(filepath.Join(root, ".sdo/corpus/targetport-mismatch"), os.DirFS(filepath.Join(fixture, "corpus/targetport-mismatch"))); err != nil {
		t.Fatal(err)
	}
	labels := filepath.Join(root, ".sdo/corpus/targetport-mismatch/labels.json")
	data, err := os.ReadFile(labels)
	if err != nil {
		t.Fatal(err)
	}
	var label map[string]any
	if err := json.Unmarshal(data, &label); err != nil {
		t.Fatal(err)
	}
	opened, err := os.ReadFile(filepath.Join(root, ".sdo/corpus/targetport-mismatch/open.json"))
	if err != nil {
		t.Fatal(err)
	}
	fixed, err := os.ReadFile(filepath.Join(root, ".sdo/corpus/targetport-mismatch/fixed.json"))
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := os.ReadFile(filepath.Join(root, ".sdo/corpus/targetport-mismatch/healthy.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixedView, healthyView map[string]any
	if err := json.Unmarshal(fixed, &fixedView); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(healthy, &healthyView); err != nil {
		t.Fatal(err)
	}
	for key, value := range healthyView {
		if items, ok := value.([]any); ok {
			prior, _ := fixedView[key].([]any)
			fixedView[key] = append(prior, items...)
		}
	}
	fixed, err = json.Marshal(fixedView)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"fixed.json", "healthy.json"} {
		if err := os.WriteFile(filepath.Join(root, ".sdo/corpus/targetport-mismatch", name), fixed, 0644); err != nil {
			t.Fatal(err)
		}
	}
	label["provenance"] = map[string]string{"incident_id": "incident", "open_sha256": fmt.Sprintf("%x", sha256.Sum256(opened)), "fixed_sha256": fmt.Sprintf("%x", sha256.Sum256(fixed))}
	data, err = json.Marshal(label)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(labels, data, 0644); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(root, ".sdo/playbooks/README.md")
	data, err = os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(index, append(data, []byte("\n- [service-targetport-mismatch](service-targetport-mismatch/README.md)\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	parityCaptures(t, root)
}

func parityCaptures(t *testing.T, root string) {
	t.Helper()
	capture := filepath.Join(filepath.Dir(root), "captures")
	if err := os.MkdirAll(capture, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"open.json", "fixed.json", "recordings.json"} {
		data, err := os.ReadFile(filepath.Join(root, ".sdo/corpus/targetport-mismatch", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(capture, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func parityAcceptedProgram(t *testing.T, root string) {
	t.Helper()
	parityProgram(t, root)
	runGit(t, root, "add", "-f", ".sdo")
	runGit(t, root, "commit", "-qm", "previously accepted program")
}

func parityLabel(t *testing.T, root string, edit func(map[string]any)) {
	t.Helper()
	parityCaseLabel(t, root, "targetport-mismatch", edit)
}

func parityCaseLabel(t *testing.T, root, caseID string, edit func(map[string]any)) {
	t.Helper()
	path := ".sdo/corpus/" + caseID + "/labels.json"
	data, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	var label map[string]any
	if err := json.Unmarshal(data, &label); err != nil {
		t.Fatal(err)
	}
	edit(label)
	data, err = json.Marshal(label)
	if err != nil {
		t.Fatal(err)
	}
	put(t, root, path, string(data), 0644)
}

func parityFindingRepairProof(outcome *contracts.OutcomeRecord) {
	at := time.Unix(100, 0)
	for index, cause := range outcome.GetConfirmedRootCauses() {
		verified := outcome.GetClassification() == "success"
		for _, work := range outcome.GetReflectionEligibility().GetCauses() {
			verified = verified || work.GetCauseIndex() == uint32(index)
		}
		if !verified {
			continue
		}
		for _, ref := range cause.GetResources() {
			outcome.Findings = append(outcome.Findings, &contracts.Finding{DetectorId: "fixture", RuleId: "repair", Severity: "critical", Summary: "synthetic opening fault", Evidence: "synthetic opening observation", Status: "active", Fingerprint: ref.GetName(), PrimaryResource: ref})
			outcome.RepairActions = append(outcome.RepairActions, &contracts.RepairActionReceipt{ActionId: "fixture-" + ref.GetName(), Kind: "kubectl-patch", Target: ref.GetKind() + "/" + ref.GetName(), Summary: "synthetic recorded mutation", Details: "synthetic controller receipt", Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at.Add(time.Second))})
		}
	}
	outcome.FinalHealthDetectorState = []*contracts.DetectorEvaluation{{DetectorId: "fixture", Status: "clear", EvaluatedAt: timestamppb.New(at.Add(2 * time.Second))}}
}

func parityRefusedPortsDetector(t *testing.T, root string) {
	source := parityDetector(t, "cache")
	source = strings.Replace(source, `"sdo.dev/operator/sdk"`, `"sdo.dev/operator/sdk"`+"\n"+`"sdo.dev/operator/sdk/program"`, 1)
	start := strings.Index(source, "func (Detector) Detect(")
	if start < 0 {
		t.Fatal("detector fixture missing Detect")
	}
	source = source[:start] + `func (Detector) Detect(_ context.Context,ctx sdk.DetectionContext)([]sdk.Finding,error){
  var findings []sdk.Finding
  for _,service:=range ctx.Services(){
   ref:=sdk.ObjectRef{APIVersion:"v1",Kind:"Service",Namespace:service.Namespace,Name:service.Name}
   for _,port:=range service.Spec.Ports{
    result,err:=program.ReadFrom(ctx).ProbeTCP(ref,port.Port)
    if err!=nil{return nil,err}
    if !result.Success{findings=append(findings,sdk.Finding{DetectorID:"cache",RuleID:"refused",Status:sdk.FindingActive,Severity:sdk.SeverityWarning,PrimaryResource:ref,Summary:"declared Service port refuses connections",Evidence:"recorded TCP refusal"})}
   }
  }
  return findings,nil
 }
 `
	put(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", source, 0644)
}

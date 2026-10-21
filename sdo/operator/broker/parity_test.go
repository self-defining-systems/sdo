package broker_test

import (
	"bytes"
	"context"
	"errors"
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

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operatorcli"
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
	return root
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	if out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func put(t *testing.T, root, rel, content string, mode os.FileMode) {
	t.Helper()
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
	binary := filepath.Join(t.TempDir(), "sdo-operator")
	build := exec.Command("go", "build", "-p", "8", "-o", binary, "../cmd/sdo-operator")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, out)
	}
	operatorRoot, err := filepath.Abs("..")
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
		{"unchanged good tree", validate.Responder, validate.ReflectionPhase, func(*testing.T, string) {}, nil, false},
		{"responder adds a detector", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", strings.Replace(incidentManifest, "%s", "base", 1), 0o644)
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", parityDetector(t, "dns"), 0o644)
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
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", source, 0o644)
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
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			root := copyParityTree(t)
			put(t, root, "app.txt", "base\n", 0o644)
			runGit(t, root, "init", "-q")
			runGit(t, root, "config", "user.name", "T")
			runGit(t, root, "config", "user.email", "t@example.com")
			runGit(t, root, "add", "-A")
			runGit(t, root, "commit", "-q", "-m", "base")
			test.edit(t, root)

			var stdout, stderr bytes.Buffer
			command := exec.Command(binary, "memory", "check", "--json")
			command.Dir = root
			command.Env = append(os.Environ(), "SDO_ACTOR="+string(test.actor), "SDO_PHASE="+string(test.phase), "SDO_BASE_COMMIT=HEAD", "SDO_OPERATOR_ROOT="+operatorRoot, "SDO_HEALTHY_BASELINE=")
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

			brokerDiagnostics, err := broker.CheckCandidate(context.Background(), root, "HEAD", test.actor, test.phase, nil, func(ctx context.Context, root string, _ []string) error {
				return operatorcli.RunValidate(ctx, builder.Config{ApplicationRoot: root, OperatorRoot: operatorRoot})
			})
			if err == nil {
				err = validate.Err(brokerDiagnostics)
			}
			var failure *validate.Failure
			switch {
			case err == nil:
			case errors.As(err, &failure):
				brokerDiagnostics = failure.Diagnostics
			default:
				t.Fatalf("broker infrastructure error: %v", err)
			}

			if verdictCLI, verdictBroker := code == 0, err == nil; verdictCLI != verdictBroker {
				t.Fatalf("verdicts differ: CLI clean=%v broker clean=%v", verdictCLI, verdictBroker)
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
			if test.allowAdditional {
				for _, rule := range test.want {
					if !slices.Contains(got, rule) {
						t.Errorf("missing rule %s: %v", rule, report.Diagnostics)
					}
				}
			} else if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("rules = %v, want %v\n%v", got, test.want, report.Diagnostics)
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

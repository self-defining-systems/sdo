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
	"sort"
	"strconv"
	"strings"
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

	for _, test := range []struct {
		name  string
		actor validate.Actor
		phase validate.Phase
		edit  func(t *testing.T, root string)
		want  []string
	}{
		{"unchanged good tree", validate.Responder, validate.ReflectionPhase, func(*testing.T, string) {}, nil},
		{"responder adds a detector", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", strings.Replace(incidentManifest, "%s", "base", 1), 0o644)
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", parityDetector(t, "dns"), 0o644)
		}, nil},
		{"detector names a missing playbook", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/manifest.yaml", strings.Replace(incidentManifest, "%s", "ghost", 1), 0o644)
			put(t, r, ".sdo/diagnostics/detectors/incidents/dns/detector.go", parityDetector(t, "dns"), 0o644)
		}, []string{"ref.unknown-playbook"}},
		{"unclassified file", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/notes.txt", "scratch", 0o644)
		}, []string{"layout.unknown-path", "ownership.denied"}},
		{"responder edits the goal", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\napplication: demo\n---\nChanged.\n", 0o644)
		}, []string{"ownership.denied"}},
		{"script not executable", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/base/scripts/fix.sh", "#!/bin/bash\ntrue\n", 0o644)
		}, []string{"script.not-executable"}},
		{"physical path in playbook text", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/base/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nSee .sdo/playbooks/other/README.md. Repair <RESOURCE_NAME>. Run fix.sh.\n", 0o644)
		}, []string{"text.physical-path"}},
		{"malformed playbook ID", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/Bad_ID/README.md", "x", 0o644)
		}, []string{"id.pattern", "ownership.denied"}},
		{"compile failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", strings.Replace(parityDetector(t, "cache"), "return Detector{}", "return undefinedValue", 1), 0o644)
		}, []string{validate.RuleDetectorCompile}},
		{"vet failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			source := strings.Replace(parityDetector(t, "cache"), `"context"`, `"context"; "fmt"`, 1)
			source = strings.Replace(source, "return Detector{}", `_ = fmt.Sprintf("%d", "bad"); return Detector{}`, 1)
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector.go", source, 0o644)
		}, []string{validate.RuleDetectorVet}},
		{"detector test failure", validate.Responder, validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/diagnostics/detectors/incidents/cache/detector_test.go", `package detector
import "testing"
func TestReject(t *testing.T){t.Fatal("sentinel detector failure")}
`, 0o644)
		}, []string{validate.RuleDetectorTest}},

		{"unknown actor", "stranger", validate.ReflectionPhase, func(t *testing.T, r string) {
			put(t, r, ".sdo/playbooks/base/scripts/fix.sh", "#!/bin/bash\ntrue\n", 0o755)
		}, []string{"ownership.unknown-actor"}},
	} {
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
			if got := rules(report.Diagnostics, nil).cli; !reflect.DeepEqual(got, test.want) {
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

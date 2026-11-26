package runtime

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/program"
)

func writeSynthetic(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

const syntheticRoleLess = "package fresh\n\ntype P struct{}\n\nfunc (P) Spec() program.ProgramSpec { return program.ProgramSpec{ID: \"fresh\"} }\n"
const syntheticRoled = "package roled\n\ntype P struct{}\n\nfunc (P) Spec() program.ProgramSpec {\n\treturn program.ProgramSpec{ID: \"roled\", Params: []program.ParamSpec{{Name: \"component\", Kind: \"Deployment\", Required: true}}}\n}\n"

func syntheticCapture(t *testing.T, withRecordings bool) string {
	t.Helper()
	capture := t.TempDir()
	for _, name := range []string{"fixed.json", "open@detect.json"} {
		writeSynthetic(t, capture, name, "{}")
	}
	if withRecordings {
		writeSynthetic(t, capture, "open@detect-recordings.json", "[]")
	}
	return capture
}

// The closing turn's new playbook gets the first case of its own, written from
// the incident it was written for; a playbook that declares object roles has
// bindings Go does not know and gets none.
func TestANewPlaybookGetsItsFirstCaseFromItsOwnIncident(t *testing.T) {
	worktree := t.TempDir()
	writeSynthetic(t, worktree, ".sdo/playbooks/fresh/diagnose.go", syntheticRoleLess)
	writeSynthetic(t, worktree, ".sdo/playbooks/roled/diagnose.go", syntheticRoled)
	if err := os.MkdirAll(filepath.Join(worktree, ".sdo", "corpus"), 0o755); err != nil {
		t.Fatal(err)
	}

	recordFirstCases(worktree, []string{"fresh", "roled"}, syntheticCapture(t, true), "incident-1", memory.CaseContext{}, nil)

	labels, err := filepath.Glob(filepath.Join(worktree, ".sdo", "corpus", "*", "labels.json"))
	if err != nil || len(labels) != 1 {
		t.Fatalf("cases = %v, %v; want exactly the role-less playbook's", labels, err)
	}
	data, err := os.ReadFile(labels[0])
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Programs   []string `json:"root_cause_program_ids"`
		Provenance struct {
			IncidentID string `json:"incident_id"`
		} `json:"provenance"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Programs) != 1 || got.Programs[0] != "fresh" || got.Provenance.IncidentID != "incident-1" {
		t.Fatalf("labels = %s", data)
	}
}

// A case that cannot be written says why, in the controller log: the writer
// never drops its error.
func TestACaseThatCannotBeRecordedIsLoggedWithItsReason(t *testing.T) {
	worktree := t.TempDir()
	writeSynthetic(t, worktree, ".sdo/playbooks/fresh/diagnose.go", syntheticRoleLess)
	writeSynthetic(t, worktree, ".sdo/playbooks/roled/diagnose.go", syntheticRoled)
	if err := os.MkdirAll(filepath.Join(worktree, ".sdo", "corpus"), 0o755); err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	defer slog.SetDefault(previous)

	recordFirstCases(worktree, []string{"fresh", "roled"}, syntheticCapture(t, false), "incident-1", memory.CaseContext{}, nil)

	text := logged.String()
	for _, want := range []string{"corpus_case_not_recorded", "playbook=fresh", "open@detect-recordings.json", "playbook=roled", "object roles"} {
		if !strings.Contains(text, want) {
			t.Fatalf("log lacks %q:\n%s", want, text)
		}
	}
}

// A new playbook that matched only a later capture gets its first case from that
// capture: the opening state may not hold its cause.
func TestANewPlaybookCaseUsesTheCaptureItMatched(t *testing.T) {
	worktree := t.TempDir()
	writeSynthetic(t, worktree, ".sdo/playbooks/fresh/diagnose.go", syntheticRoleLess)
	capture := syntheticCapture(t, true)
	writeSynthetic(t, capture, "open@late-1.json", `{"namespace":"late"}`)
	writeSynthetic(t, capture, "open@late-1-recordings.json", "[]")
	origins := map[string]string{memory.CapturedInstanceKey("fresh", program.Bindings{}): "late-1"}

	recordFirstCases(worktree, []string{"fresh"}, capture, "incident-1", memory.CaseContext{}, origins)

	labels, err := filepath.Glob(filepath.Join(worktree, ".sdo", "corpus", "*", "labels.json"))
	if err != nil || len(labels) != 1 {
		t.Fatalf("cases = %v, %v", labels, err)
	}
	data, _ := os.ReadFile(labels[0])
	if !strings.Contains(string(data), `"open_at": "late-1"`) {
		t.Fatalf("labels = %s, want the later capture", data)
	}
}

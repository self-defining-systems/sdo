package validate

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShellCheckHygieneAndOwnershipWithoutGo(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		actor            Actor
		phase            Phase
		want             string
	}{
		{"judge detector", ".sdo/diagnostics/detectors/health/ready/check.sh", "exit 0\n", HealthJudge, InitialPhase, ""},
		{"reflection detector", ".sdo/diagnostics/detectors/incidents/dns/check.sh", "exit 1\n", Responder, ReflectionPhase, ""},
		{"Go source refused", ".sdo/diagnostics/detectors/incidents/dns/check.go", "package dns\n", Responder, ReflectionPhase, "shell.format"},
		{"free notes", ".sdo/playbooks/dns/notes.md", "No required front matter or format.\n", Responder, ReflectionPhase, ""},
		{"free script", ".sdo/playbooks/dns/fix.sh", "echo fix\n", Responder, ReflectionPhase, ""},
		{"protected health", ".sdo/diagnostics/detectors/health/ready/check.sh", "exit 0\n", Responder, ReflectionPhase, RuleOwnershipDenied},
		{"protected outcomes", ".sdo/outcomes/" + strings.Repeat("a", 64) + ".json", "{}", Responder, ReflectionPhase, RuleOwnershipDenied},
		{"binary", ".sdo/playbooks/dns/notes.md", "nul\x00byte", Responder, ReflectionPhase, "shell.text"},
		{"credential", ".sdo/playbooks/dns/notes.md", "api_key=synthetic_test_canary_12345", Responder, ReflectionPhase, "shell.credentials"},
		{"oversize", ".sdo/playbooks/dns/notes.md", strings.Repeat("x", 256*1024+1), Responder, ReflectionPhase, RuleTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			commit(t, root, "base")
			write(t, root, tc.path, tc.body)
			if strings.HasSuffix(tc.path, ".sh") {
				if err := os.Chmod(filepath.Join(root, tc.path), 0755); err != nil {
					t.Fatal(err)
				}
			}
			ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: tc.actor, Phase: tc.phase, CodePath: "shell"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == "" && len(ds) != 0 {
				t.Fatalf("unexpected: %#v", ds)
			}
			if tc.want != "" {
				found := false
				for _, d := range ds {
					found = found || d.Rule == tc.want
				}
				if !found {
					t.Fatalf("missing %s: %#v", tc.want, ds)
				}
			}
		})
	}
}

func TestShellCheckRejectsSymlinkAndUnknownMode(t *testing.T) {
	root := t.TempDir()
	commit(t, root, "base")
	if err := os.MkdirAll(filepath.Join(root, ".sdo/playbooks"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, ".sdo/playbooks/escape")); err != nil {
		t.Fatal(err)
	}
	ds, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CodePath: "shell"}, nil)
	if len(ds) == 0 {
		t.Fatalf("symlink accepted: %v %#v", err, ds)
	}
	_, err = Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CodePath: "unknown"}, nil)
	if err == nil {
		t.Fatal("unknown memory mode accepted")
	}
}

func TestShellHygieneRejectsUndispatchableSnapshot(t *testing.T) {
	root := t.TempDir()
	write(t, root, ".sdo/goal.md", "available")
	write(t, root, ".sdo/arch.md", "application")
	write(t, root, ".sdo/diagnostics/detectors/health/ready/check.sh", "exit 0")
	if err := os.Chmod(filepath.Join(root, ".sdo/diagnostics/detectors/health/ready/check.sh"), 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b", "c"} {
		write(t, root, ".sdo/playbooks/notes/"+name+".md", strings.Repeat("x", 200000))
	}
	diagnostics, err := ShellHygiene(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range diagnostics {
		if d.Rule == RuleSnapshotBudget {
			return
		}
	}
	t.Fatalf("undispatchable memory accepted: %v", diagnostics)
}

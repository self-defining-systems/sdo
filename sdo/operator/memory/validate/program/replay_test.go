package program

import (
	"context"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	sdkprogram "sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func TestReplayOffScriptOriginsRemainBoundToAcceptedReceipts(t *testing.T) {
	for _, mode := range []string{"digest", "missing receipt", "wrong digest", "failed", "untimed", "dynamic", "legacy linked", "legacy unlinked"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, ".sdo/playbooks/fix")
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			script := "echo synthetic repair"
			body := "package fix\nvar step=program.RunScript{Script:" + "`" + script + "`" + "}\n"
			if mode == "dynamic" {
				body = strings.Replace(body, "`"+script+"`", "makeScript()", 1)
			}
			if err := os.WriteFile(filepath.Join(dir, "mitigate.go"), []byte(body), 0644); err != nil {
				t.Fatal(err)
			}
			readme := "---\noriginating_incident: synthetic\n---\n# Fix\n"
			receipt := &contracts.RepairActionReceipt{ActionId: "repair-1", Success: true, ScriptDigest: sdkprogram.ScriptDigest(script), StartedAt: timestamppb.New(time.Unix(10, 0)), CompletedAt: timestamppb.New(time.Unix(11, 0))}
			switch mode {
			case "wrong digest":
				receipt.ScriptDigest = sdkprogram.ScriptDigest("other")
			case "failed":
				receipt.Success = false
			case "untimed":
				receipt.StartedAt = nil
			case "legacy linked", "legacy unlinked":
				receipt.ScriptDigest = ""
			}
			if mode == "legacy linked" {
				readme = strings.Replace(readme, "---\n#", "script_origins:\n  "+sdkprogram.ScriptDigest(script)+": repair-1\n---\n#", 1)
			}
			if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte(readme), 0644); err != nil {
				t.Fatal(err)
			}
			outcome := &contracts.OutcomeRecord{IncidentId: "synthetic", RepairActions: []*contracts.RepairActionReceipt{receipt}}
			if mode == "missing receipt" {
				outcome.RepairActions = nil
			}
			err := ValidateScriptOriginsWithoutCorpus(context.Background(), root, "", outcome, []string{".sdo/playbooks/fix/mitigate.go"})
			want := mode != "digest" && mode != "legacy linked"
			if (err != nil) != want {
				t.Fatalf("mode %s: %v", mode, err)
			}
		})
	}
}

func TestReplayOffPreservesScriptOriginsAcrossPlaybookRevisions(t *testing.T) {
	for _, mode := range []string{"unchanged prior", "import prior", "new linked", "new unlinked", "wrong receipt"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			testutil.InitGit(t, root)
			git := func(args ...string) string {
				t.Helper()
				out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput()
				if err != nil {
					t.Fatalf("git: %v %s", err, out)
				}
				return strings.TrimSpace(string(out))
			}
			git("config", "user.name", "Test")
			git("config", "user.email", "test@example.invalid")
			receipt := func(id, script string) *contracts.RepairActionReceipt {
				return &contracts.RepairActionReceipt{ActionId: id, Kind: "run-script", Target: "Pod/demo/synthetic", Summary: "synthetic", Details: "synthetic", Success: true, ScriptDigest: sdkprogram.ScriptDigest(script), StartedAt: timestamppb.New(time.Unix(10, 0)), CompletedAt: timestamppb.New(time.Unix(11, 0))}
			}
			old := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "older", Classification: "success", RepairActions: []*contracts.RepairActionReceipt{receipt("old-receipt", "echo old")}}
			if _, err := memory.NewJSONStore(root).Put(context.Background(), old); err != nil {
				t.Fatal(err)
			}
			git("add", ".")
			git("commit", "-qm", "accepted original receipt")
			base := git("rev-parse", "HEAD")
			current := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success", RepairActions: []*contracts.RepairActionReceipt{receipt("new-receipt", "echo new")}}
			script := "echo new"
			if mode == "unchanged prior" || mode == "import prior" {
				script = "echo old"
			}
			body := "---\noriginating_incident: older\n"
			if mode == "new linked" || mode == "wrong receipt" {
				id := "new-receipt"
				if mode == "wrong receipt" {
					id = "invented"
				}
				body += "script_receipts:\n  " + sdkprogram.ScriptDigest(script) + ":\n    incident_id: current\n    receipt_id: " + id + "\n"
			}
			body += "---\n# Synthetic\n"
			dir := filepath.Join(root, ".sdo/playbooks/fix")
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			for name, text := range map[string]string{"README.md": body, "mitigate.go": "package fix\nvar step=program.RunScript{Script:`" + script + "`}\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "import prior" {
				base = "HEAD"
				current = nil
			}
			err := ValidateScriptOriginsWithoutCorpus(context.Background(), root, base, current, []string{".sdo/playbooks/fix/mitigate.go"})
			want := mode == "unchanged prior" || mode == "import prior" || mode == "new linked"
			if (err == nil) != want {
				t.Fatalf("%s: %v", mode, err)
			}
		})
	}
}

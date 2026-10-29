package program

import (
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
			err := ValidateScriptOriginsWithoutCorpus(root, outcome, []string{".sdo/playbooks/fix/mitigate.go"})
			want := mode != "digest" && mode != "legacy linked"
			if (err != nil) != want {
				t.Fatalf("mode %s: %v", mode, err)
			}
		})
	}
}

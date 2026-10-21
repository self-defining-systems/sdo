package runtime

import (
	"context"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestSharedLibraryChangeResetsProgramRevisionAndTrust(t *testing.T) {
	root := nativeRepository(t)
	write := func(path, body string) {
		t.Helper()
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write(".sdo/playbooks/targetport/diagnose.go", "package targetport\nimport _ \"app-diagnostics/lib/shared\"\n")
	write(".sdo/lib/shared/ports.go", "package shared\nimport _ \"app-diagnostics/lib/transitive\"\n")
	write(".sdo/lib/transitive/port.go", "package transitive\nconst Port=80\n")
	write(".sdo/lib/unrelated/other.go", "package unrelated\nconst Port=1\n")
	commit := func(message string) string {
		nativeGit(t, root, "add", "-A")
		nativeGit(t, root, "commit", "-qm", message)
		return nativeGit(t, root, "rev-parse", "HEAD")
	}
	runs := []ProgramRun{{ProgramID: "targetport"}}
	fillProgramRevisions(context.Background(), runs, root, commit("program and libraries"))
	before := runs[0].ProgramRevision
	write(".sdo/lib/unrelated/other.go", "package unrelated\nconst Port=2\n")
	fillProgramRevisions(context.Background(), runs, root, commit("unrelated helper"))
	if before != runs[0].ProgramRevision {
		t.Fatal("unrelated helper reset trust")
	}
	write(".sdo/lib/transitive/port.go", "package transitive\nconst Port=9999\n")
	fillProgramRevisions(context.Background(), runs, root, commit("changed transitive helper"))
	after := runs[0].ProgramRevision
	if before == after {
		t.Fatal("transitive library edit retained obsolete program revision")
	}
	outcomes := []*contracts.OutcomeRecord{}
	for i := 0; i < 3; i++ {
		outcomes = append(outcomes, verifiedTrustOutcome(t, "targetport", before))
	}
	if !ProgramTrustStats(outcomes)["targetport"].Trusted {
		t.Fatal("fixture did not earn trust")
	}
	outcomes = append(outcomes, &contracts.OutcomeRecord{ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: after}}})
	if got := ProgramTrustStats(outcomes)["targetport"]; got.Trusted || got.VerifiedSuccesses != 0 {
		t.Fatalf("helper edit kept trust: %+v", got)
	}
}

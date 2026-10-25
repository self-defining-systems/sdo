package runtime

import (
	"context"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

func TestAdoptedProgramsStartWithoutInheritedTrust(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	old := verifiedTrustOutcome(t, "targetport", "revision-a")
	old.SchemaVersion = 1
	old.IncidentId = "inherited"
	if _, err := memory.NewJSONStore(root).Put(ctx, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: old.IncidentId, Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "old operational history")
	d := KubernetesJobDispatcher{Outcomes: priorOutcomeReader{outcomes: []memory.Outcome{{IncidentID: old.IncidentId, Record: old}}}}
	request := IncidentRequest{RepositoryWorktree: root, SourceCommit: nativeGit(t, root, "rev-parse", "HEAD")}
	before, err := d.programTrust(ctx, request)
	if err != nil || !(before["targetport"].VerifiedSuccesses > 0 && before["targetport"].Failures == 0) {
		t.Fatalf("fixture must have prior trust: %+v %v", before, err)
	}
	adopted, err := memory.MarkAdoptedExisting(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	request.SourceCommit = adopted
	after, err := d.programTrust(ctx, request)
	if err != nil || (after["targetport"].VerifiedSuccesses > 0 && after["targetport"].Failures == 0) || after["targetport"].VerifiedSuccesses != 0 {
		t.Fatalf("inherited trust survived adoption: %+v %v", after, err)
	}
}

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestNativeRolloutHandoffRequiresObservedNewRevision(t *testing.T) {
	accepted := nativeRepository(t)
	nativeMemoryFixture(t, accepted)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	revision := nativeGit(t, accepted, "rev-parse", "HEAD")
	fingerprint, err := repository.DiagnosticsFingerprint(ctx, revision)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger")}
	if err := os.MkdirAll(native.LedgerRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	expectation := ControllerRolloutExpectation{IncidentID: "inc-handoff", ReflectionCommit: revision, BeforeDetectorFingerprint: "old-tree", AfterDetectorFingerprint: fingerprint}
	receipt := ClosureReceipt{IncidentID: "inc-handoff", ReflectionCommit: revision, OutcomeCommit: "outcome", AckToken: "token"}
	if err := native.saveLedger(nativeLedger{IncidentID: receipt.IncidentID, ReflectionCommit: revision, RolloutExpectation: &expectation, Receipt: &receipt}); err != nil {
		t.Fatal(err)
	}
	launch := &contractsv1alpha1.ControllerConfig{DiagnosticsFingerprint: fingerprint, ControllerArtifactDigest: "sha256:binary", ControllerRevision: revision}
	if err := completeNativeRolloutHandoffs(ctx, native, nil, accepted); err == nil || !strings.Contains(err.Error(), "provenance") {
		t.Fatalf("missing provenance accepted: %v", err)
	}
	wrong := proto.Clone(launch).(*contractsv1alpha1.ControllerConfig)
	wrong.DiagnosticsFingerprint = "other-tree"
	if err := completeNativeRolloutHandoffs(ctx, native, wrong, accepted); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("wrong fingerprint accepted: %v", err)
	}
	if err := completeNativeRolloutHandoffs(ctx, native, launch, accepted); err != nil {
		t.Fatal(err)
	}
	if handoffs, err := native.PendingControllerRollouts(); err != nil || len(handoffs) != 0 {
		t.Fatalf("verified rollout remains pending: %+v, %v", handoffs, err)
	}
	if err := native.AcknowledgeClosure(ctx, receipt); err != nil {
		t.Fatalf("durable closure could not resume after restart: %v", err)
	}
}

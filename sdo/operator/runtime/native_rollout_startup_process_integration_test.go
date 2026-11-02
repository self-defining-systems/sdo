//go:build integration

package runtime

import (
	"context"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
<<<<<<< HEAD:sdo/operator/runtime/native_rollout_startup_process_integration_test.go
=======
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
>>>>>>> origin/vic-lsh/dev/sdo-improve:sdo/operator/runtime/native_rollout_startup_test.go
	"sdo.dev/operator/broker"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"testing"
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
	if err := native.saveLedger(&contractsv1alpha1.BrokerLedger{IncidentId: receipt.IncidentID, ReflectionCommit: revision, RolloutExpectation: rolloutExpectationContract(&expectation), Receipt: closureReceiptContract(&receipt)}); err != nil {
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

// A later accepted commit (for example a health refresh that changes
// diagnostics) must not strand the rollout of an earlier reflection.
func TestNativeRolloutHandoffAcceptsLaterAcceptedRevision(t *testing.T) {
	accepted := nativeRepository(t)
	nativeMemoryFixture(t, accepted)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	reflection := nativeGit(t, accepted, "rev-parse", "HEAD")
	reflected, err := repository.DiagnosticsFingerprint(ctx, reflection)
	if err != nil {
		t.Fatal(err)
	}
	nativeGit(t, accepted, "checkout", "-q", "-b", "unrelated")
	if err := os.WriteFile(filepath.Join(accepted, "unrelated.txt"), []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, accepted, "add", ".")
	nativeGit(t, accepted, "commit", "-qm", "unrelated")
	unrelated := nativeGit(t, accepted, "rev-parse", "HEAD")
	nativeGit(t, accepted, "checkout", "-q", "-")
	nativeGit(t, accepted, "reset", "-q", "--hard", reflection)
	refresh := filepath.Join(accepted, ".sdo/diagnostics/detectors/health/health/detector.go")
	if err := os.WriteFile(refresh, []byte("package objective\n\n// refreshed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, accepted, "add", ".")
	nativeGit(t, accepted, "commit", "-qm", "health refresh")
	later := nativeGit(t, accepted, "rev-parse", "HEAD")
	refreshed, err := repository.DiagnosticsFingerprint(ctx, later)
	if err != nil || refreshed == reflected {
		t.Fatalf("health refresh must change the diagnostics tree: %v", err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger")}
	if err := os.MkdirAll(native.LedgerRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	expectation := ControllerRolloutExpectation{IncidentID: "inc-later", ReflectionCommit: reflection, BeforeDetectorFingerprint: "old-tree", AfterDetectorFingerprint: reflected}
	receipt := ClosureReceipt{IncidentID: "inc-later", ReflectionCommit: reflection, OutcomeCommit: "outcome", AckToken: "token"}
	if err := native.saveLedger(&contractsv1alpha1.BrokerLedger{IncidentId: receipt.IncidentID, ReflectionCommit: reflection, RolloutExpectation: rolloutExpectationContract(&expectation), Receipt: closureReceiptContract(&receipt)}); err != nil {
		t.Fatal(err)
	}
	stray := &contractsv1alpha1.ControllerConfig{DiagnosticsFingerprint: refreshed, ControllerArtifactDigest: "sha256:binary", ControllerRevision: unrelated}
	if err := completeNativeRolloutHandoffs(ctx, native, stray, accepted); err == nil {
		t.Fatal("a revision that is not accepted HEAD was accepted")
	}
	launch := &contractsv1alpha1.ControllerConfig{DiagnosticsFingerprint: refreshed, ControllerArtifactDigest: "sha256:binary", ControllerRevision: later}
	if err := completeNativeRolloutHandoffs(ctx, native, launch, accepted); err != nil {
		t.Fatalf("later accepted revision did not satisfy the rollout: %v", err)
	}
	if handoffs, err := native.PendingControllerRollouts(); err != nil || len(handoffs) != 0 {
		t.Fatalf("verified rollout remains pending: %+v, %v", handoffs, err)
	}
	assertRolloutStrictReceipt(t, ctx, native, receipt.IncidentID)
}

// Feed the real startup/broker ledger into the benchmark consumer so their
// accepted-revision semantics cannot drift independently.
func assertRolloutStrictReceipt(t *testing.T, ctx context.Context, native NativeIncidentBroker, id string) {
	t.Helper()
	ledger, err := native.readLedger(id)
	if err != nil {
		t.Fatal(err)
	}
	stamp := timestamppb.New(time.Now().UTC())
	outcome := strings.Repeat("b", 40)
	ledger.Acknowledged = true
	ledger.OutcomeCommit = outcome
	ledger.Receipt.OutcomeCommit = outcome
	closure := nativeClosure(IncidentWorkspace{IncidentID: id, Worktree: "synthetic", BaseCommit: ledger.ReflectionCommit})
	closure.Request.Namespace = "synthetic"
	closure.Request.RepairPolicy = "recorded-actions"
	ledger.Closure, err = incidentClosureContract(closure)
	if err != nil {
		t.Fatal(err)
	}
	ledger.Closure.Result.RepairActions = []*contractsv1alpha1.RepairActionReceipt{{ActionId: "synthetic", Kind: "synthetic", Target: "synthetic", Summary: "synthetic repair", Details: "synthetic details", StartedAt: stamp, CompletedAt: stamp, Success: true}}

	ledger.OutcomeDocument = &contractsv1alpha1.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: "success"}
	receipt := &contractsv1alpha1.NativeProductionReceipt{
		SchemaVersion: "sdo.production-receipt/v2", BrokerMode: "native", WorkerMountMode: "proposal-only", IncidentId: id, Namespace: "synthetic", ControlNamespace: "synthetic-sdo",
		ControllerImage: "synthetic:controller", ResponderImage: "synthetic:responder", ValidatorImage: "synthetic:validator", ControllerPodUid: "synthetic-pod", NativeLedger: ledger,
		ResponderJobEvidence: []*contractsv1alpha1.ResponderJobEvidence{{IncidentId: id, JobName: "synthetic-job", JobUid: "synthetic-uid", Succeeded: true}},
	}
	raw, err := protojson.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	restartClassStrictReceipt(t, ctx, raw)
}

package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk/program"
)

func TestManySweepRunsPersistUnderBudgetAndRetainClosureEvidence(t *testing.T) {
	ctx := context.Background()
	closure := nativeClosure(IncidentWorkspace{IncidentID: "many-sweeps", Worktree: "/proposal", BaseCommit: "base"})
	for i := 0; i < 1000; i++ {
		run := ProgramRun{ProgramID: "synthetic-sweep", BindingSource: "sweep", Verdict: program.Verdict{Result: program.NoMatch, Evidence: []program.Evidence{{Fact: "synthetic", Observed: strings.Repeat("synthetic evidence ", 60)}}}}
		closure.Request.InitialProgramRuns = append(closure.Request.InitialProgramRuns, run)
		closure.Result.ProgramRuns = append(closure.Result.ProgramRuns, run)
		closure.Result.ProgramToolCalls = append(closure.Result.ProgramToolCalls, ToolCall{Command: "status", ResponseJSON: `{"healthy":true}`, Summary: "sweep"})
	}
	state := &contracts.ControllerState{Version: RuntimeStateVersion, IncidentOpen: true, ResponderDone: true,
		IncidentRequest: mustProject(t, incidentRequestContract, closure.Request), IncidentResult: mustProject(t, incidentResultContract, *closure.Result),
		PendingClosure: mustProject(t, incidentClosureContract, closure), ClosureState: contracts.ClosureState_CLOSURE_STATE_PENDING}
	before := proto.Clone(state)
	client := fake.NewSimpleClientset()
	root := t.TempDir()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state", root)
	revision, err := store.Save(ctx, state, "")
	if err != nil {
		t.Fatalf("persist sweep history: %v", err)
	}
	cm, err := client.CoreV1().ConfigMaps("demo").Get(ctx, "state", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cm.Data[lifecycleio.StateDataKey]) > lifecycleio.MaxConfigMapBytes {
		t.Fatal("restart document exceeds budget")
	}
	t.Logf("restart document: %d bytes", len(cm.Data[lifecycleio.StateDataKey]))
	store = lifecycleio.NewConfigMapStateStore(client, "demo", "state", root)
	loaded, gotRevision, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gotRevision != revision || !proto.Equal(loaded, state) || !proto.Equal(state, before) {
		t.Fatal("state evidence changed or save mutated its caller")
	}
	// Outcome/receipt consumers must see the original frozen closure after restart.
	derived, err := (ProtoOutcomeDeriver{}).Derive(ctx, incidentClosureState(loaded.PendingClosure), "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(derived.Record.GetProgramRuns()) != 1000 || len(derived.Record.GetProgramToolCalls()) != 1000 {
		t.Fatal("outcome lost program evidence")
	}
	if _, err := wire.Canonical(loaded.PendingClosure); err != nil {
		t.Fatalf("receipt closure invalid: %v", err)
	}
}

func TestArchivedStateEvidenceCASAndLostReply(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	root := t.TempDir()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state", root)
	closure := nativeClosure(IncidentWorkspace{IncidentID: "evidence-cas", Worktree: "/proposal", BaseCommit: "base"})
	closure.Result.ProgramToolCalls = []ToolCall{{Command: "status", ResponseJSON: strings.Repeat("x", 40000)}}
	state := &contracts.ControllerState{Version: RuntimeStateVersion, IncidentResult: mustProject(t, incidentResultContract, *closure.Result)}
	revision, err := store.Save(ctx, state, "")
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := store.Save(ctx, state, revision)
	if err != nil || unchanged != revision {
		t.Fatalf("unchanged evidence rewrote state: %s %v", unchanged, err)
	}
	state.IncidentResult.ProgramToolCalls[0].ResponseJson = strings.Repeat("y", 40000)
	loseNextReply(client)
	if _, err := store.Save(ctx, state, revision); err == nil {
		t.Fatal("lost reply was not reported")
	}
	next, err := store.Save(ctx, state, revision)
	if err != nil || next == revision {
		t.Fatalf("evidence-only update not durable: %s %v", next, err)
	}
	if _, err := lifecycleio.NewConfigMapStateStore(client, "demo", "state", root).Save(ctx, state, revision); !errors.Is(err, lifecycleio.ErrStateConflict) {
		t.Fatalf("stale writer not fenced: %v", err)
	}
	loaded, _, err := lifecycleio.NewConfigMapStateStore(client, "demo", "state", root).Load(ctx)
	if err != nil || !proto.Equal(loaded, state) {
		t.Fatalf("cold restart did not retain evidence-only change: %v", err)
	}
	cm, err := client.CoreV1().ConfigMaps("demo").Get(ctx, "state", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, cm.Annotations[lifecycleio.StateEvidenceAnnotation]+".json")
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(ctx); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("corrupt evidence accepted: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Load(ctx); err == nil {
		t.Fatal("missing evidence silently truncated closure")
	}
}

func TestArchiveFailureDoesNotAdvanceState(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	root := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(root, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state", root)
	closure := nativeClosure(IncidentWorkspace{IncidentID: "archive-failure", Worktree: "/proposal", BaseCommit: "base"})
	closure.Result.ProgramToolCalls = []ToolCall{{Command: "status", ResponseJSON: strings.Repeat("x", 40000)}}
	state := &contracts.ControllerState{Version: RuntimeStateVersion, IncidentResult: mustProject(t, incidentResultContract, *closure.Result)}
	if _, err := store.Save(ctx, state, ""); err == nil {
		t.Fatal("archive failure accepted")
	}
	if len(client.Actions()) != 0 {
		t.Fatal("state API called before archive was durable")
	}
}

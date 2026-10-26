package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

// Hand-written schedule: freeze an outcome, start a real registered Job, lose
// controller leadership, adopt new health inputs, then complete the original
// Job. Both its typed result and the frozen controller closure must survive.
func TestReflectionAdoptionAfterHealthRefreshAndRestart(t *testing.T) {
	for _, advance := range []bool{false, true} {
		t.Run(map[bool]string{false: "restart", true: "restart-and-health-adoption"}[advance], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			root, _, repository, _, coordinator := tracingFixture(t)
			old := tracingRefresh(t, root, coordinator)
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
			workspace, err := native.PrepareIncident(ctx, "reflection-adoption")
			if err != nil {
				t.Fatal(err)
			}
			closure := socialRepeatClosure(workspace)
			client := fake.NewSimpleClientset()
			registry := testIncidentRegistry(t, "reflector:test")
			agent, err := registry.AgentFor(&contracts.ReflectionRequest{})
			if err != nil {
				t.Fatal(err)
			}
			created := make(chan struct{}, 1)
			executor := worker_registry.KubernetesExecutor{Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(filepath.Dir(workspace.Worktree)), PollInterval: time.Millisecond, OnPhase: func(_ context.Context, phase string) error {
				if phase == "reflection_job_created" {
					created <- struct{}{}
				}
				return nil
			}}
			worker := RegisteredReflectionWorker{AgentID: agent, TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: time.Minute, TaskStore: &lifecycleio.TaskStore{Root: t.TempDir()}, Executor: executor, SessionExecutor: func(string) (ReflectionTaskExecutor, error) { return executor, nil }}
			native.Reflector = worker
			firstCtx, stopFirst := context.WithCancel(ctx)
			defer stopFirst()
			done := make(chan error, 1)
			goUntilCleanup(t, stopFirst, func() { _, err := native.ProcessClosure(firstCtx, closure); done <- err })
			select {
			case <-created:
			case err := <-done:
				t.Fatalf("Job not created: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			now := time.Now().UTC()
			cfg := testControllerConfig()
			cfg.SourceCommit = old.Commit
			initial, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			initial.pendingClosure = cloneIncidentClosure(&closure)
			initial.closureState = contracts.ClosureState_CLOSURE_STATE_PROCESSING
			store := lifecycleio.NewConfigMapStateStore(client, "demo", "controller-state")
			if _, err := store.Save(ctx, exportState(t, initial), ""); err != nil {
				t.Fatal(err)
			}
			if err := initial.SetIncidentBroker(native); err != nil {
				t.Fatal(err)
			}
			rv6Guard(initial, root, old.Commit)
			// Health checks must not take the effect lock held by the live Job.
			guarded := make(chan error, 1)
			goUntilCleanup(t, stopFirst, func() { guarded <- initial.CheckHealth(ctx) })
			select {
			case err := <-guarded:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				stopFirst()
				t.Fatal("health ownership check blocked behind running reflection")
			}
			stopFirst()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("leadership cancellation: %v", err)
			}
			current := old
			if advance {
				tracingSwap(t, root)
				current = tracingRefresh(t, root, coordinator)
			}
			cfg.SourceCommit = current.Commit
			restored, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := restored.AttachStateStore(ctx, store); err != nil {
				t.Fatal(err)
			}
			if err := restored.SetIncidentBroker(native); err != nil {
				t.Fatal(err)
			}
			rv6Guard(restored, root, current.Commit)
			// Startup precedes result availability. It must retain an owner for the Job.
			if err := restored.prepareHealthEffects(ctx, now); err != nil {
				t.Fatal(err)
			}
			if restored.closureState == contracts.ClosureState_CLOSURE_STATE_FAILED {
				t.Fatal("health adoption orphaned reflection of an accepted outcome")
			}
			jobs, err := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 1 {
				t.Fatalf("Jobs: %v %v", jobs, err)
			}
			job := jobs.Items[0]
			request, err := client.CoreV1().ConfigMaps("demo").Get(ctx, job.Name+"-request", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			task := new(contracts.AgentTask)
			if err := wire.Unmarshal([]byte(request.Data[worker_registry.TaskDataKey]), task); err != nil {
				t.Fatal(err)
			}
			response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "synthetic incident lacks independently verified repair evidence"})
			if err != nil {
				t.Fatal(err)
			}
			payload, err := wire.Canonical(&contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response})
			if err != nil {
				t.Fatal(err)
			}
			completed := metav1.Now()
			if _, err := client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-result", CreationTimestamp: completed}, Data: map[string]string{worker_registry.ResultDataKey: string(payload)}}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &completed
			if _, err := client.BatchV1().Jobs("demo").UpdateStatus(ctx, &job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := executeStartupEffects(ctx, restored, true); err != nil {
				t.Fatal(err)
			}
			select {
			case completion := <-restored.closureResults:
				restored.handleClosureCompletion(completion)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := restored.PersistState(ctx); err != nil {
				t.Fatal(err)
			}
			if err := executePausedEffects(ctx, restored); err != nil {
				t.Fatal(err)
			}
			select {
			case completion := <-restored.acknowledgmentResults:
				restored.handleAcknowledgmentCompletion(completion)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			ledger, err := native.readLedger(workspace.IncidentID)
			if err != nil || !ledger.ReflectionProcessed || len(ledger.ReflectionResults) != 1 || ledger.AcknowledgedAt == nil {
				t.Fatalf("reflection not adopted/ACKed once: %v %v", ledger, err)
			}
			before := nativeGit(t, root, "rev-parse", "HEAD")
			if _, err := native.ProcessClosure(ctx, closure); err != nil {
				t.Fatal(err)
			}
			if after := nativeGit(t, root, "rev-parse", "HEAD"); before != after {
				t.Fatal("duplicate completion published twice")
			}
			creates := 0
			for _, action := range client.Actions() {
				if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
					creates++
				}
			}
			if creates != 1 {
				t.Fatalf("original Job redispatched %d times", creates)
			}
		})
	}
}

func TestReflectionAdoptionConflictIsRecordedInOutcome(t *testing.T) {
	h := newRejectionHarness(t, "adoption-conflict", func(call int, w IncidentWorkspace) (ReflectionDecision, error) {
		// A different accepted writer changes the same toy memory line after each
		// frozen reflection checkout. The real broker's three-way merge must reject.
		return writeGoodPlaybook(w)
	})
	h.setReflector(func(call int, w IncidentWorkspace) (ReflectionDecision, error) {
		tracingWrite(t, h.target, ".sdo/playbooks/base/README.md", fmt.Sprintf("---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: initial\n---\nAccepted peer instruction %d\n", call))
		nativeGit(t, h.target, "add", ".")
		nativeGit(t, h.target, "commit", "-qm", "synthetic concurrent accepted writer")
		return writeGoodPlaybook(w)
	})
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ReflectionCommit != "" || len(h.calls) != 3 {
		t.Fatalf("conflict not terminal after bounded corrections: %v calls=%d", receipt, len(h.calls))
	}
	got, err := memory.OpenReader(h.target).Get(context.Background(), h.workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	rejections := got.Record.GetReflectionRejections()
	if len(rejections) != 3 {
		t.Fatalf("outcome lost conflict history: %d", len(rejections))
	}
	for _, rejection := range rejections {
		if len(rejection.Diagnostics) != 1 || rejection.Diagnostics[0].Rule != "memory.conflict" || rejection.Diagnostics[0].GetMemoryConflict().GetAcceptedCommit() == "" {
			t.Fatalf("unnamed rejection: %v", rejection)
		}
	}
	head := nativeGit(t, h.target, "rev-parse", "HEAD")
	if _, err := h.native.ProcessClosure(context.Background(), h.closure); err != nil {
		t.Fatal(err)
	}
	if head != nativeGit(t, h.target, "rev-parse", "HEAD") {
		t.Fatal("duplicate rejection completion republished outcome")
	}
}

func TestReflectionAdoptionValidatesAndPublishesAtCurrentHeadOnce(t *testing.T) {
	var acceptedHead string
	validations := 0
	h := newRejectionHarnessValidator(t, "adoption-current-head", nil, func(_ context.Context, stage string, paths []string) error {
		for _, path := range paths {
			if strings.HasPrefix(path, ".sdo/playbooks/") {
				validations++
				if head := nativeGit(t, stage, "rev-parse", "HEAD"); head != acceptedHead {
					return fmt.Errorf("validated against %s, expected current head %s", head, acceptedHead)
				}
				data, err := os.ReadFile(filepath.Join(stage, "peer.txt"))
				if err != nil || string(data) != "accepted peer intent\n" {
					return fmt.Errorf("accepted peer intent missing from validation: %q %v", data, err)
				}
			}
		}
		return nil
	})
	h.setReflector(func(_ int, w IncidentWorkspace) (ReflectionDecision, error) {
		// The proposal is already frozen when another accepted writer advances.
		tracingWrite(t, h.target, "peer.txt", "accepted peer intent\n")
		nativeGit(t, h.target, "add", ".")
		nativeGit(t, h.target, "commit", "-qm", "synthetic accepted peer advance")
		acceptedHead = nativeGit(t, h.target, "rev-parse", "HEAD")
		return writeGoodPlaybook(w)
	})
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ReflectionCommit == "" || validations != 1 || len(h.calls) != 1 {
		t.Fatalf("reflection was not validated/published exactly once: %+v validations=%d calls=%d", receipt, validations, len(h.calls))
	}
	if parent := nativeGit(t, h.target, "rev-parse", receipt.ReflectionCommit+"^"); parent != acceptedHead {
		t.Fatalf("publication parent = %s, expected %s", parent, acceptedHead)
	}
	if again, err := h.native.ProcessClosure(context.Background(), h.closure); err != nil || again != receipt {
		t.Fatalf("duplicate result changed receipt: %+v %v", again, err)
	}
	if validations != 1 || len(h.calls) != 1 || nativeGit(t, h.target, "rev-parse", "HEAD") != receipt.ReflectionCommit {
		t.Fatal("duplicate completion revalidated or republished")
	}
}

func TestReflectionAdoptionRejectsChangedObjectiveOrPolicyOnce(t *testing.T) {
	for _, change := range []string{"objective", "policy"} {
		t.Run(change, func(t *testing.T) {
			h := newRejectionHarness(t, "adoption-"+change, nil)
			h.setReflector(func(_ int, w IncidentWorkspace) (ReflectionDecision, error) {
				if change == "objective" {
					tracingWrite(t, h.target, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\n---\n# Health objective\nDifferent objective.\n")
				} else {
					tracingWrite(t, h.target, ".sdo/lib/new-policy.txt", "new policy input\n")
				}
				nativeGit(t, h.target, "add", ".")
				nativeGit(t, h.target, "commit", "-qm", "synthetic accepted "+change+" change")
				return writeGoodPlaybook(w)
			})
			receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.ReflectionCommit != "" || len(h.calls) != 1 {
				t.Fatalf("changed %s published or retried historical reflection: %+v calls=%d", change, receipt, len(h.calls))
			}
			got, err := memory.OpenReader(h.target).Get(context.Background(), h.workspace.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			rejections := got.Record.GetReflectionRejections()
			if len(rejections) != 1 || len(rejections[0].Diagnostics) != 1 || rejections[0].Diagnostics[0].Rule != "reflection."+change+"-changed" || rejections[0].Diagnostics[0].Message != change+" changed" {
				t.Fatalf("missing named rejection: %v", rejections)
			}
			if accepted := nativeGit(t, h.target, "show", "HEAD:.sdo/playbooks/base/README.md"); strings.Contains(accepted, "Repair <RESOURCE_NAME> carefully") {
				t.Fatal("rejected reflection reached accepted memory")
			}
			head := nativeGit(t, h.target, "rev-parse", "HEAD")
			if _, err := h.native.ProcessClosure(context.Background(), h.closure); err != nil {
				t.Fatal(err)
			}
			if nativeGit(t, h.target, "rev-parse", "HEAD") != head || len(h.calls) != 1 {
				t.Fatal("duplicate result repeated historical rejection")
			}
		})
	}
}

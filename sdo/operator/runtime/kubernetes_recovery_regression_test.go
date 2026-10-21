package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// recoveryOutageClient models an API outage independently of the controller's
// journal. A lost receipt commits the write, then hides its first readback.
type recoveryOutageClient struct {
	*planFakeClient
	objectClient  PlanObjectClient
	attempts      int
	rejectAttempt int
	loseReceipt   bool
	rejectRead    bool
}

func (c *recoveryOutageClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.attempts++
	if c.attempts == c.rejectAttempt {
		return errors.New("synthetic write outage")
	}
	backend := c.objectClient
	if backend == nil {
		backend = c.planFakeClient
	}
	if err := backend.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	if c.loseReceipt {
		c.loseReceipt = false
		c.rejectRead = true
		return errors.New("synthetic receipt loss")
	}
	return nil
}
func (c *recoveryOutageClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.rejectRead {
		c.rejectRead = false
		return nil, errors.New("synthetic read outage")
	}
	if c.objectClient != nil {
		return c.objectClient.Get(ctx, ref)
	}
	return c.planFakeClient.Get(ctx, ref)
}

type recoveryRolloutFailure struct{}

func (recoveryRolloutFailure) Wait(context.Context, []sdk.ObjectRef) error {
	return errors.New("synthetic rollout failure")
}

func repeatedReplicaPlan() (sdk.ObjectRef, program.Plan, *recoveryOutageClient) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "workers"}
	client := &recoveryOutageClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}}
	plan := program.Plan{}
	for _, value := range []int{2, 3, 4} {
		plan.Actions = append(plan.Actions, program.SetField{Ref: ref, Path: "spec.replicas", Value: value})
	}
	return ref, plan, client
}
func requireRecoveredReplicas(t *testing.T, client *recoveryOutageClient, ref sdk.ObjectRef) {
	t.Helper()
	value, _ := testField(client.objects[ref], "spec.replicas")
	if value != float64(1) {
		t.Fatalf("compensation left replicas=%v; want 1", value)
	}
}
func TestApplyRetainsTwoCompletedRollbackWrites(t *testing.T) {
	ctx := context.Background()
	ref, plan, client := repeatedReplicaPlan()
	client.rejectAttempt = 6 // Three forward writes, two reversals, then an outage.
	var durable PlanApplicationResult
	executor := PlanExecutor{Client: client, Rollouts: recoveryRolloutFailure{}, Verifier: planFakeVerifier{passed: true}, Checkpoint: func(_ context.Context, result PlanApplicationResult) error {
		payload, err := json.Marshal(result)
		if err != nil {
			return err
		}
		return json.Unmarshal(payload, &durable)
	}}
	result, err := executor.Apply(ctx, IncidentRequest{RepairPolicy: "recorded-actions"}, plan, false)
	if err == nil {
		t.Fatal("rollout failure should fail the application")
	}
	t.Logf("returned rollback progress=%d, durable progress=%d, patch attempts=%d", result.RollbackCompleted, durable.RollbackCompleted, client.attempts)
	if result.RollbackCompleted != 2 || durable.RollbackCompleted != 2 {
		t.Errorf("lost two completed reverse writes")
	}
	client.rejectAttempt = 0
	if err := executor.Rollback(ctx, durable); err != nil {
		t.Fatalf("retry after API recovery: %v", err)
	}
	requireRecoveredReplicas(t, client, ref)
}
func TestRestartRetainsTwoCompletedRollbackWrites(t *testing.T) {
	for _, markerOnly := range []bool{false, true} {
		name := "tool_journal"
		if markerOnly {
			name = "failed_marker"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ref, plan, client := repeatedReplicaPlan()
			original := recoveryLoop(t, client)
			applied, err := original.Executor.Apply(ctx, original.Request, plan, false)
			if err != nil {
				t.Fatal(err)
			}
			dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "operator"}
			if markerOnly {
				payload, err := json.Marshal(applied)
				if err != nil {
					t.Fatal(err)
				}
				_, err = dispatcher.Client.CoreV1().ConfigMaps("operator").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: IncidentJobName(original.Request.IncidentID) + "-fast-path"}, Data: map[string]string{"state": "failed", "application": string(payload), "program_ids": `["targetport"]`}}, metav1.CreateOptions{})
				if err != nil {
					t.Fatal(err)
				}
			} else {
				attachRecoveryStore(t, dispatcher, original)
				original.applications["application-1"] = applied
				original.applicationsLog = []RecordedPlanApplication{{ID: "application-1", Result: applied, Pending: true}}
				if err := original.persistToolState(ctx); err != nil {
					t.Fatal(err)
				}
			}
			client.rejectAttempt = 6
			for restart := 0; restart < 2; restart++ {
				loop := recoveryLoop(t, client)
				if markerOnly {
					if _, _, _, err := dispatcher.tryFastPath(ctx, loop.Request, loop); err != nil {
						t.Fatal(err)
					}
				} else {
					attachRecoveryStore(t, dispatcher, loop)
				}
				entry := loop.PlanApplications()[0]
				t.Logf("restart=%d progress=%d pending=%v", restart, entry.Result.RollbackCompleted, entry.Pending)
				if restart == 0 && entry.Result.RollbackCompleted != 2 {
					t.Errorf("restart erased completed rollback writes")
				}
				client.rejectAttempt = 0
			}
			requireRecoveredReplicas(t, client, ref)
		})
	}
}
func TestRestartRecoversLostReceiptAfterReadOutage(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "pending"
		if legacy {
			name = "legacy_pending_flag_missing"
		}
		t.Run(name, func(t *testing.T) { testLostReceiptReadOutage(t, legacy) })
	}
}
func testLostReceiptReadOutage(t *testing.T, legacy bool) {
	ctx := context.Background()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	client := &recoveryOutageClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}}
	dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(), Namespace: "operator"}
	loop := recoveryLoop(t, client)
	attachRecoveryStore(t, dispatcher, loop)
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: "targetport"})
	if planned.Error != "" {
		t.Fatal(planned.Error)
	}
	client.loseReceipt = true
	response := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if !strings.Contains(response.Error, "receipt loss") || !strings.Contains(response.Error, "read outage") || response.ApplicationID == "" {
		t.Fatalf("expected uncertain write with handle: %+v", response)
	}
	entry := loop.PlanApplications()[0]
	t.Logf("uncertain write: pending=%v pendingAction=%v rollingBack=%v", entry.Pending, entry.Result.PendingAction, entry.Result.RollingBack)
	if !entry.Pending || entry.Result.PendingAction == nil {
		t.Errorf("uncertain write lost its pending state")
	}
	if legacy {
		loop.applicationsLog[0].Pending = false
		if err := loop.persistToolState(ctx); err != nil {
			t.Fatal(err)
		}
	}
	restarted := recoveryLoop(t, client)
	attachRecoveryStore(t, dispatcher, restarted)
	value, _ := testField(client.objects[ref], "spec.ports.0.targetPort")
	if value != float64(9999) {
		t.Fatalf("restart left uncertain write at targetPort=%v; signals=%v", value, restarted.FailureSignals())
	}
	if !restarted.PlanApplications()[0].Result.RolledBack || len(restarted.FailureSignals()) == 0 {
		t.Fatal("missing durable recovery evidence")
	}
	writes := client.attempts
	replay := restarted.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planned.PlanID}})
	if replay.Error == "" || client.attempts != writes {
		t.Fatal("restart replayed a consumed forward plan")
	}
}
func TestResolvedFastPathPublicationSurvivesRestart(t *testing.T) {
	for _, journal := range []string{"current", "older_pending", "already_rolled_back"} {
		t.Run(journal, func(t *testing.T) { testResolvedFastPathRestart(t, journal) })
	}
}
func testResolvedFastPathRestart(t *testing.T, journal string) {
	ctx := context.Background()
	loop, client := trustedPortLoop(t, false, true, true)
	request := goldenRequest(t)
	request.Namespace, request.RepairPolicy, request.SourceCommit = "app", "recorded-actions", "tree-a"
	request.Findings = loop.Request.Findings
	loop.Request = request
	dispatcher := KubernetesJobDispatcher{Client: client, Namespace: "app"}
	attachRecoveryStore(t, dispatcher, loop)
	if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, loop); err != nil || !resolved {
		t.Fatalf("publish resolution: resolved=%v err=%v", resolved, err)
	}
	if journal == "older_pending" {
		// A marker written by the old publication order may accompany the last
		// executor checkpoint, whose application was still marked in flight.
		loop.applicationsLog[0].Pending = true
		if err := loop.persistToolState(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if journal == "already_rolled_back" {
		executor := loop.applicationExecutor("fast-path-1")
		if err := executor.Rollback(ctx, loop.applications["fast-path-1"]); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate death before the dispatcher result reaches its caller. Only the
	// marker and journal survive; no incident closure has been recorded.
	restarted := NewResponderLoop(request, loop.Programs, loop.Provider, loop.Detectors, loop.Executor, loop.MaxBatches)
	restarted.ReadView = loop.ReadView
	if journal == "already_rolled_back" {
		err := dispatcher.restoreToolState(ctx, request, restarted)
		if err == nil {
			_, _, resolved, err := dispatcher.tryFastPath(ctx, request, restarted)
			if err == nil && resolved {
				t.Fatal("cached resolution accepted a rolled-back application")
			}
		}
		return
	}
	attachRecoveryStore(t, dispatcher, restarted)
	if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, restarted); err != nil || !resolved {
		t.Fatalf("recover published resolution: resolved=%v err=%v", resolved, err)
	}
	service, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("cached success returned after repair was undone: targetPort=%d signals=%v", service.Spec.Ports[0].TargetPort.IntVal, restarted.FailureSignals())
	}
	if entries := restarted.PlanApplications(); len(entries) != 1 || entries[0].Pending || entries[0].Result.RolledBack {
		t.Fatalf("resolved application was not finalized: %+v", entries)
	}
}

type recoveryThreePortProgram struct{ fastloopPortProgram }

func (recoveryThreePortProgram) Mitigate(_ context.Context, _ program.ReadView, _ program.Bindings) (program.Plan, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	plan := program.Plan{}
	for _, port := range []int{8081, 8082, 8080} {
		plan.Actions = append(plan.Actions, program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: port})
	}
	return plan, nil
}
func TestFailedFastPathPublicationRetainsRollbackCursor(t *testing.T) {
	ctx := context.Background()
	loop, kube := trustedPortLoop(t, false, true, false)
	request := goldenRequest(t)
	request.Namespace, request.RepairPolicy, request.SourceCommit = "app", "recorded-actions", "tree-a"
	request.Findings = loop.Request.Findings
	loop.Request = request
	p := recoveryThreePortProgram{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	clear(loop.matchedPlans)
	client := &recoveryOutageClient{objectClient: loop.Executor.Client, rejectAttempt: 6}
	loop.Executor.Client = client
	dispatcher := KubernetesJobDispatcher{Client: kube, Namespace: "app"}
	attachRecoveryStore(t, dispatcher, loop)
	if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, loop); err != nil || resolved {
		t.Fatalf("failed verification: resolved=%v err=%v", resolved, err)
	}
	entry := loop.PlanApplications()[0]
	if entry.Result.RollbackCompleted != 2 {
		t.Errorf("failed fast-path publication lost rollback cursor: %d", entry.Result.RollbackCompleted)
	}
	client.rejectAttempt = 0
	restarted := recoveryLoop(t, client)
	restarted.Request = request
	attachRecoveryStore(t, dispatcher, restarted)
	if _, _, resolved, err := dispatcher.tryFastPath(ctx, request, restarted); err != nil || resolved {
		t.Fatalf("retry compensation: resolved=%v err=%v", resolved, err)
	}
	service, err := kube.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if service.Spec.Ports[0].TargetPort.IntVal != 9999 {
		t.Fatalf("failed fast path left targetPort=%d", service.Spec.Ports[0].TargetPort.IntVal)
	}
}

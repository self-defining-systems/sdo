package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

type peerBlockedRollout struct {
	entered    chan struct{}
	peerBroken func() bool
}

func (r peerBlockedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	for _, ref := range refs {
		if ref.Kind == "Deployment" && (r.peerBroken == nil || r.peerBroken()) {
			select {
			case r.entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
	}
	return nil
}

func TestPendingRolloutReleasesMenuForPeerRepairBeforeIncidentDeadline(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	loop.Request.RepairPolicy = "recorded-actions"
	loop.Request.ResponseDeadline = time.Now().Add(2 * time.Second)
	loop.Executor.Verifier = planFakeVerifier{true}
	loop.Executor.RolloutTimeout = 20 * time.Millisecond
	loop.Executor.Rollouts = peerBlockedRollout{entered: make(chan struct{}, 1), peerBroken: func() bool {
		value, _ := testField(cluster.objects[menuService], "spec.ports.0.targetPort")
		return value != float64(8080)
	}}
	cluster.objects[menuDeployment].Object["spec"].(map[string]any)["replicas"] = float64(0)
	loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan ToolResponse, 1)
	go func() {
		first <- loop.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "ScaleWorkload", RoundLimit: 1}, Parameters: map[string]string{"ref": "Deployment/app/frontend", "replicas": "1"}})
	}()
	var response ToolResponse
	select {
	case response = <-first:
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-first
		t.Fatal("rollout held tool lock instead of returning pending within its smaller budget")
	}
	data, _ := json.Marshal(response)
	var wire map[string]any
	_ = json.Unmarshal(data, &wire)
	if wire["rollout_pending"] == nil || response.Healthy || response.Report == nil || response.Report.Status == contracts.MitigationReport_STATUS_SUCCESS {
		t.Fatalf("pending rollout lost its typed/unverified classification: %s", data)
	}
	if value, _ := testField(cluster.objects[menuDeployment], "spec.replicas"); value != float64(1) {
		t.Fatalf("pending application was undone: %v", value)
	}
	second := loop.Handle(context.Background(), ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "SetField", RoundLimit: 1}, Parameters: map[string]string{"ref": "Service/app/backend", "path": "spec.ports[0].targetPort", "value": "8080", "reason": "repair independent peer"}})
	if second.Error != "" || second.Report == nil || second.Report.Status != contracts.MitigationReport_STATUS_SUCCESS || !time.Now().Before(loop.Request.ResponseDeadline) {
		t.Fatalf("peer repair could not run within original deadline: %+v", second)
	}
	if strings.Contains(string(data), `"independently_healthy":true`) {
		t.Fatal("pending rollout received independent success credit")
	}
}

type rolloutFunc func(context.Context, []sdk.ObjectRef) error

func (f rolloutFunc) Wait(ctx context.Context, refs []sdk.ObjectRef) error { return f(ctx, refs) }

func TestRolloutBudgetDefaultAndIncidentDeadline(t *testing.T) {
	for _, configured := range []time.Duration{0, 3 * time.Minute} {
		e := PlanExecutor{RolloutTimeout: configured, Rollouts: rolloutFunc(func(ctx context.Context, _ []sdk.ObjectRef) error {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > 90*time.Second || time.Until(deadline) < 89*time.Second {
				t.Fatalf("default rollout budget not ninety seconds: %v %v", deadline, ok)
			}
			return nil
		})}
		if _, err := e.waitForRollouts(context.Background(), IncidentRequest{ResponseDeadline: time.Now().Add(3 * time.Minute)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	original := time.Now().Add(20 * time.Millisecond)
	e := PlanExecutor{Rollouts: rolloutFunc(func(ctx context.Context, _ []sdk.ObjectRef) error {
		deadline, _ := ctx.Deadline()
		if !deadline.Equal(original) {
			t.Fatalf("original deadline extended: %v != %v", deadline, original)
		}
		<-ctx.Done()
		return ctx.Err()
	})}
	pending, err := e.waitForRollouts(context.Background(), IncidentRequest{ResponseDeadline: original}, []sdk.ObjectRef{menuDeployment})
	var typed *RolloutPendingError
	if pending == nil || !errors.As(err, &typed) {
		t.Fatalf("deadline did not return typed pending: %v %v", pending, err)
	}
}

func TestPendingApplicationRetainsWriteAndUnknownVerificationAcrossContract(t *testing.T) {
	loop, cluster := menuB3Loop(t)
	e := loop.Executor
	e.Verifier = planFakeVerifier{true}
	e.RolloutTimeout = time.Millisecond
	e.Rollouts = rolloutFunc(func(ctx context.Context, _ []sdk.ObjectRef) error { <-ctx.Done(); return ctx.Err() })
	plan := program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: menuDeployment, Replicas: testPlanValue(1)}}}
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(time.Second)}, plan, false)
	var typed *RolloutPendingError
	if !errors.As(err, &typed) || !result.Applied || result.RolledBack || result.VerificationCompleted || result.VerificationPassed {
		t.Fatalf("pending write misclassified: %+v %v", result, err)
	}
	if value, _ := testField(cluster.objects[menuDeployment], "spec.replicas"); value != float64(1) {
		t.Fatal("pending write was rolled back")
	}
	encoded, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	restored := planApplicationState(encoded).Result
	if restored.RolloutPending == nil || restored.VerificationCompleted || restored.VerificationPassed || encoded.GetVerification().GetResult() != contracts.Verdict_RESULT_UNKNOWN {
		t.Fatalf("pending contract became verified: %+v", encoded)
	}
}

func TestKubernetesPendingRolloutCarriesOwnedPodCreationEvidence(t *testing.T) {
	count := int32(1)
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: "app", UID: "deployment-uid", Generation: 2}, Spec: appsv1.DeploymentSpec{Replicas: &count}, Status: appsv1.DeploymentStatus{ObservedGeneration: 2, Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: "admission webhook TLS unknown authority"}}}}
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "frontend-next", Namespace: "app", UID: "rs-owned", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", UID: d.UID}}}}
	owned := &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "blocked", Namespace: "app"}, Type: corev1.EventTypeWarning, InvolvedObject: corev1.ObjectReference{UID: rs.UID, Namespace: "app", Kind: "ReplicaSet", Name: rs.Name}, Reason: "FailedCreate", Message: "failed calling webhook: x509 unknown authority"}
	foreign := owned.DeepCopy()
	foreign.Name = "foreign"
	foreign.InvolvedObject.UID = "foreign-rs"
	foreign.Message = "unrelated peer event must not bind"
	client := fake.NewSimpleClientset(d, rs, owned, foreign)
	e := PlanExecutor{Rollouts: KubernetesPlanRollouts{Client: client}, RolloutTimeout: time.Millisecond}
	pending, err := e.waitForRollouts(context.Background(), IncidentRequest{ResponseDeadline: time.Now().Add(time.Second)}, []sdk.ObjectRef{menuDeployment})
	if err == nil || pending == nil || len(pending.Blockers) != 1 || !strings.Contains(pending.Reason, "TLS unknown authority") || len(pending.Blockers[0].WarningEvents) != 1 || !strings.Contains(pending.Blockers[0].WarningEvents[0], "x509") {
		t.Fatalf("missing/foreign blocker evidence: %v %v", pending, err)
	}
}

func TestPendingFastPathDoesNotUndoLandedRepairOrEarnTrust(t *testing.T) {
	loop, client := trustedPortLoop(t, false, true, true)
	loop.Executor.RolloutTimeout = time.Millisecond
	loop.Executor.Rollouts = rolloutFunc(func(ctx context.Context, _ []sdk.ObjectRef) error { <-ctx.Done(); return ctx.Err() })
	attempt := loop.TryTrustedFastPath(context.Background(), 1)
	if !attempt.Attempted || attempt.Resolved || attempt.RolledBack || !strings.Contains(attempt.Summary, "rollout_pending") {
		t.Fatalf("pending fast path misclassified: %+v", attempt)
	}
	service, err := client.CoreV1().Services("app").Get(context.Background(), "api", metav1.GetOptions{})
	if err != nil || service.Spec.Ports[0].TargetPort.IntVal != 8080 {
		t.Fatalf("pending landed repair undone: %v %v", service, err)
	}
	if len(loop.FailureSignals()) != 0 || loop.PlanApplications()[0].Result.VerificationPassed {
		t.Fatal("pending application got trust success/failure credit")
	}
}

func TestPendingRolloutToolRecordIsBoundedAndNotVerified(t *testing.T) {
	p := &contracts.RolloutPending{Reason: strings.Repeat("blocked ", 2000), Blockers: []*contracts.RolloutBlocker{{Ref: planRefProto(menuDeployment), Reason: strings.Repeat("TLS failed ", 1000), WarningEvents: []string{strings.Repeat("FailedCreate ", 1000)}}}}
	record := recordedToolResponseJSON(ToolResponse{Command: "mitigate", Summary: strings.Repeat("rollout_pending ", 1000), Error: strings.Repeat("pending ", 1000), RolloutPending: p}, nil)
	if len(record) > maxRecordedToolResponse {
		t.Fatalf("pending evidence overflowed tool bound: %d", len(record))
	}
	var decoded struct {
		Truncated bool                      `json:"truncated"`
		Healthy   bool                      `json:"healthy"`
		Pending   *contracts.RolloutPending `json:"rollout_pending"`
	}
	if err := json.Unmarshal(record, &decoded); err != nil || !decoded.Truncated || decoded.Healthy || decoded.Pending == nil {
		t.Fatalf("pending record lost classification: %s %v", record, err)
	}
	if len(p.Reason) < 1000 {
		t.Fatal("bounded record mutated full native evidence")
	}
}

// A pending rollout has no independent repair verdict. It must not erase a
// previously verified revision's trust, nor mask a separate false match.
func TestPendingRolloutDoesNotCountAsDurableTrustFailure(t *testing.T) {
	for _, falseMatch := range []bool{false, true} {
		t.Run(fmt.Sprint(falseMatch), func(t *testing.T) {
			prior := &contracts.OutcomeRecord{Classification: "success", SourceCommit: "revision-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "repair", ProgramRevision: "revision-a"}}, ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Unassigned: []*contracts.ReflectionWorkItem{{Disposition: "covered", ProgramId: "repair", ProgramRevision: "revision-a"}}}}
			pending := &contracts.OutcomeRecord{Classification: "failed", SourceCommit: "revision-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "repair", ProgramRevision: "revision-a"}}, PlanApplications: []*contracts.PlanApplication{{ApplicationId: "fast-path-pending", Applied: true, ProgramIds: []string{"repair"}, Error: "rollout_pending", RolloutPending: &contracts.RolloutPending{Reason: "peer admission blocks rollout"}}}}
			if falseMatch {
				pending.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Unassigned: []*contracts.ReflectionWorkItem{{Disposition: "false_match", ProgramId: "repair", ProgramRevision: "revision-a"}}}
			}
			state := ProgramTrustStats([]*contracts.OutcomeRecord{prior, pending}, 1)["repair"]
			if falseMatch {
				if state.Failures != 1 || state.Trusted {
					t.Fatalf("pending masked independent false match: %+v", state)
				}
			} else if state.Failures != 0 || state.VerifiedSuccesses != 1 || !state.Trusted {
				t.Fatalf("pending alone changed durable trust: %+v", state)
			}
		})
	}
}

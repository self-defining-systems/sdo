package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
)

// A hostPort repair rolls the Deployment and replaces its Pending Pod. The
// responder names both; only the Deployment has an opening finding. The
// closure's recorded outcome must keep the cause learnable instead of turning
// it into an no_recorded_repair question that blocks the playbook.
func TestClosureLearnsWorkloadRepairWhoseCauseNamesItsReplacedPod(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const id = "owned-pod-cause"
	target := nativeRepository(t)
	repository, err := broker.New(target, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	sessions := t.TempDir()
	capture, err := memory.IncidentCorpusDir(sessions, id)
	if err != nil {
		t.Fatal(err)
	}
	opened := time.Now().UTC().Add(-time.Minute)
	controller := true
	owner := func(kind, name string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: name, Controller: &controller}}
	}
	for phase, at := range map[string]time.Time{"open@detect": opened, "fixed": opened.Add(30 * time.Second)} {
		set, pod := "search-old", "search-old-abcde"
		if phase == "fixed" {
			set, pod = "search-new", "search-new-fghij"
		}
		snapshot := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &at,
			DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "search", Namespace: "app"}}},
			ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: set, Namespace: "app", OwnerReferences: owner("Deployment", "search")}}},
			PodList:        []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: "app", OwnerReferences: owner("ReplicaSet", set)}}}}
		capturefixture.WriteBoundary(t, filepath.Join(capture, phase+".json"), snapshot, "app")
	}
	deployment := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "search"}
	replaced := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "search-old-abcde"}
	var reflected *contracts.OutcomeRecord
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), SessionRoot: sessions,
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			return DerivedOutcome{RequiresReflection: true, Record: &contracts.OutcomeRecord{
				SchemaVersion: 1, IncidentId: closure.Request.IncidentID, Classification: "success",
				ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "hostPort conflict left the search Pod Pending", Resources: []*contracts.ObjectRef{deployment, replaced}, ExplainedDetectors: []string{"workload"}}},
				Findings:            []*contracts.Finding{{DetectorId: "workload", RuleId: "unavailable", Status: "active", Severity: "warn", Fingerprint: "search", Summary: "search has 0/1 available replicas", Evidence: "Pending Pod: hostPort conflict", PrimaryResource: deployment}},
				RepairActions:       []*contracts.RepairActionReceipt{{ActionId: "act-search", Kind: "patch", Target: "Deployment/search", Summary: "removed the conflicting hostPort", Details: "controller-recorded Deployment patch", Success: true, Resources: []*contracts.ObjectRef{deployment}, StartedAt: timestamppb.New(opened.Add(10 * time.Second)), CompletedAt: timestamppb.New(opened.Add(20 * time.Second))}},
				DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: "workload", Status: "clear", EvaluatedAt: timestamppb.New(opened.Add(30 * time.Second))}},
			}}, nil
		}),
		ResponderBackend: "test", ResponderModel: "test",
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, _ IncidentWorkspace, outcome *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			reflected = outcome
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "test"}, nil
		}),
	}
	workspace, err := native.PrepareIncident(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.Request.RepairPolicy = "recorded-actions"
	if _, err := native.ProcessClosure(ctx, closure); err != nil {
		t.Fatal(err)
	}
	if reflected == nil {
		t.Fatal("verified closure skipped reflection")
	}
	for _, item := range reflected.GetReflectionEligibility().GetUnassigned() {
		if item.GetReason() == "no_recorded_repair" {
			t.Fatalf("replaced owned Pod made the workload repair unverifiable: %v", item)
		}
	}
	causes := reflected.GetReflectionEligibility().GetCauses()
	if len(causes) != 1 || causes[0].GetCauseIndex() != 0 || len(causes[0].GetResources()) != 1 || causes[0].GetResources()[0].GetKind() != "Deployment" {
		t.Fatalf("workload cause not offered for learning: %v", reflected.GetReflectionEligibility())
	}
	recorded, err := memory.NewJSONStore(target).Get(ctx, id)
	if err != nil || len(recorded.Record.GetConfirmedRootCauses()[0].GetResources()) != 1 {
		t.Fatalf("accepted outcome kept the replaced Pod as a cause resource: %+v %v", recorded, err)
	}
}

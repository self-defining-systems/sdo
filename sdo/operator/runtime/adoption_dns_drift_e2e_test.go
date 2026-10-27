package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// TestAdoptedDeploymentDNSPolicyDriftIsNamedAndRestoredWithoutImageEvidence
// follows the lane-50 social-network fault: an application adopted with
// install-existing (tag images, no deployer receipt, no image ledger, no
// rendered lock yet) has its dnsPolicy changed. Source comparison works like
// kubectl diff: the drift names spec.template.spec.dnsPolicy and one
// ReconcileFromSource restores it, whatever the deployer path recorded.
func TestAdoptedDeploymentDNSPolicyDriftIsNamedAndRestoredWithoutImageEvidence(t *testing.T) {
	ctx := context.Background()
	repo := nativeRepository(t)
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: nginx-thrift}\nspec:\n  replicas: 1\n  selector: {matchLabels: {app: nginx-thrift}}\n  template:\n    metadata: {labels: {app: nginx-thrift}}\n    spec:\n      containers:\n      - name: nginx-thrift\n        image: example/openresty-thrift:xenial\n"
	if err := os.MkdirAll(filepath.Join(repo, "kubernetes"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "kubernetes/nginx.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, repo, "add", ".")
	nativeGit(t, repo, "commit", "-qm", "adopted social network")
	locked := nativeGit(t, repo, "rev-parse", "HEAD")
	paths := []string{"kubernetes/nginx.yaml"}

	one := int32(1)
	labelsOf := map[string]string{"app": "nginx-thrift"}
	live := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "nginx-thrift", Namespace: "app", Generation: 1, UID: "nginx-uid", ResourceVersion: "1"},
		Spec:       appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labelsOf}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labelsOf}, Spec: corev1.PodSpec{DNSPolicy: corev1.DNSClusterFirst, Containers: []corev1.Container{{Name: "nginx-thrift", Image: "example/openresty-thrift:xenial"}}}}},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "nginx-thrift-1", Namespace: "app", Labels: labelsOf}, Spec: live.Spec.Template.Spec, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "nginx-thrift", Image: "example/openresty-thrift:xenial", Ready: true}}}}
	client := fake.NewSimpleClientset(live, pod)
	sourceClient := bridgeFake(t, client)
	source := GitPlanSource{Repository: repo, ManifestPaths: paths}

	controllerConfig := testControllerConfig()
	controllerConfig.Namespace, controllerConfig.SourceCommit = "app", locked
	controller, err := NewController(controllerConfig, nil, fastloopObjects{client: client}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, time.Unix(10, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{Source: source, Client: sourceClient, View: func(snapshot sdk.DetectionContext) program.ReadView {
		return LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: locked, Client: sourceClient}
	}})
	if err := controller.adoptOwnership(ctx, time.Unix(11, 0).UTC(), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatalf("ownership adoption: %v", err)
	}

	// The fault: dnsPolicy leaves ClusterFirst.
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "nginx-thrift"}
	adopted, err := client.AppsV1().Deployments("app").Get(ctx, "nginx-thrift", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	faulted := adopted.DeepCopy()
	faulted.Spec.Template.Spec.DNSPolicy = corev1.DNSDefault
	if _, err := client.AppsV1().Deployments("app").Update(ctx, faulted, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	provider := fastloopObjects{client: client}
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := sdktest.CaptureSnapshot(snapshot, "app")
	if err != nil {
		t.Fatal(err)
	}
	diff, err := (LiveReadView{DetectionContext: captured, Source: source, SourceCommit: locked, Client: sourceClient}).DiffAgainstSource(ref)
	if err != nil {
		t.Fatalf("adopted Deployment cannot be compared with accepted source: %v", err)
	}
	if !diff.Changed || !strings.Contains(diff.Text, "spec.template.spec.dnsPolicy") {
		t.Fatalf("drift does not name spec.template.spec.dnsPolicy: %+v", diff)
	}

	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: source, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{unschedulable{}}, Provider: provider}}
	result, err := executor.Apply(ctx, IncidentRequest{IncidentID: "dns", Namespace: "app", SourceCommit: locked, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("ReconcileFromSource did not apply: %+v %v", result, err)
	}
	after, err := client.AppsV1().Deployments("app").Get(ctx, "nginx-thrift", metav1.GetOptions{})
	if err != nil || after.Spec.Template.Spec.DNSPolicy != corev1.DNSClusterFirst {
		t.Fatalf("one ReconcileFromSource did not restore dnsPolicy: %q %v", after.Spec.Template.Spec.DNSPolicy, err)
	}
}

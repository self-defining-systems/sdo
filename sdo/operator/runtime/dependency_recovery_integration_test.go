//go:build integration

package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestGeneratedConfigMapCreateRecyclesOnlyBlockedMountedPods(t *testing.T) {
	root := t.TempDir()
	testutil.InitGit(t, root)
	if err := os.MkdirAll(filepath.Join(root, "deploy"), 0755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"kustomization.yaml": "configMapGenerator:\n- name: startup\n  files: [init.sh]\ngeneratorOptions: {disableNameSuffixHash: true}\n", "init.sh": "echo synthetic\n"} {
		if err := os.WriteFile(filepath.Join(root, "deploy", name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, root, "add", ".")
	testutil.Git(t, root, "commit", "-qm", "synthetic generated input")
	revision := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
	blocked := recoveryPod("blocked")
	blocked.Spec.Volumes = []corev1.Volume{{Name: "startup", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}}}}
	blocked.Status.ContainerStatuses[0].State.Waiting.Reason = "ContainerCreating"
	healthy := blocked.DeepCopy()
	healthy.Name = "healthy"
	healthy.UID = "healthy"
	healthy.Status.Phase = corev1.PodRunning
	healthy.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	unrelated := recoveryPod("unrelated")
	client := fake.NewSimpleClientset(blocked, healthy, unrelated)
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		obj := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		obj.UID = "generated-config"
		obj.ResourceVersion = "2"
		return false, nil, nil
	})
	executor := PlanExecutor{Client: TypedPlanClient{Client: bridgeFake(t, client)}, Source: rendered.Source{Repository: root, ManifestPaths: []string{"deploy"}}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{true}}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "startup"}
	inventory, err := executor.Source.(rendered.Source).AcceptedInventory(context.Background(), revision, "app")
	if err != nil {
		t.Fatal(err)
	}
	index := NewSourceInventory("app", revision, inventory.Objects)
	missing := index.Findings(nil, time.Unix(100, 0))
	if len(missing) != 1 || missing[0].PrimaryResource != ref {
		t.Fatalf("generated object missing from source inventory findings: %+v", missing)
	}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: revision, RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: ref, Reason: "restore tracked generated config"}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("app").Get(context.Background(), "startup", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("app").Get(context.Background(), "blocked", metav1.GetOptions{}); err == nil {
		t.Fatal("mount-blocked Pod was not recycled")
	}
	for _, name := range []string{"healthy", "unrelated"} {
		if _, err := client.CoreV1().Pods("app").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
			t.Fatalf("unrelated/healthy Pod recycled: %s", name)
		}
	}
	live, err := executor.Client.Get(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if findings := index.Findings([]*unstructured.Unstructured{live}, time.Unix(101, 0)); len(findings) != 0 {
		t.Fatalf("restored source inventory did not clear: %+v", findings)
	}
	if len(result.ObjectEffects) != 2 || result.ObjectEffects[1].UID != "blocked" {
		t.Fatalf("missing recycle receipt: %+v", result.ObjectEffects)
	}
	receipts := ProgramRepairReceipts([]RecordedPlanApplication{{ID: "generated", Result: result}}, nil)
	if len(receipts) != 1 || receipts[0].Reversible || len(receipts[0].Resources) != 2 {
		t.Fatalf("recycled Pod missing from irreversible repair receipt: %+v", receipts)
	}
	encoded, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	if len(planApplicationState(encoded).Result.ObjectEffects) != 2 {
		t.Fatal("receipt lost across persistence")
	}
	if err := executor.Rollback(context.Background(), result); err == nil || !strings.Contains(err.Error(), "irreversible") {
		t.Fatalf("rollback hid dependent deletion: %v", err)
	}
}

func TestDependencyRecoveryBudgetKeepsOriginalDeadline(t *testing.T) {
	backend := recoveryPod("store")
	backend.Labels = map[string]string{"tier": "store"}
	backend.Status.ContainerStatuses = nil
	consumer := recoveryPod("consumer")
	consumer.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "STORE", Value: "store:6379"}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"tier": "store"}}}
	client := fake.NewSimpleClientset(backend, consumer, service)
	executor := PlanExecutor{Rollouts: KubernetesPlanRollouts{Client: client}}
	result := PlanApplicationResult{Patches: []PlanPatch{{Ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store"}, Operation: "ensure_database_user"}}}
	deadline := time.Now().Add(20 * time.Millisecond)
	pending, err := executor.waitAfterSourceWrites(context.Background(), IncidentRequest{ResponseDeadline: deadline}, &result, nil)
	var typed *RolloutPendingError
	if pending == nil || !errors.As(err, &typed) {
		t.Fatalf("unready dependency not reported as pending: %v %v", pending, err)
	}
	if time.Since(deadline) > time.Second {
		t.Fatal("dependency recovery extended original deadline")
	}
	if len(result.ObjectEffects) != 0 {
		t.Fatal("unready backend caused a consumer restart")
	}
}

func TestDependencyRecoveryPreservesProducedAdmissionPrerequisite(t *testing.T) {
	ctx := context.Background()
	rollouts := admissionRolloutsFor("store")
	deployment, err := rollouts.Client.AppsV1().Deployments("app").Get(ctx, "store", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	deployment.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "store"}}
	if _, err := rollouts.Client.AppsV1().Deployments("app").Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	backend := recoveryPod("store-old")
	backend.Labels = map[string]string{"tier": "store"}
	backend.Status.Phase = corev1.PodRunning
	backend.Status.ContainerStatuses = nil
	backend.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	consumer := recoveryPod("consumer")
	consumer.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "STORE_ADDR", Value: "store:6379"}}
	for _, pod := range []*corev1.Pod{backend, consumer} {
		if _, err := rollouts.Client.CoreV1().Pods("app").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"tier": "store"}}}
	if _, err := rollouts.Client.CoreV1().Services("app").Create(ctx, service, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	executor := PlanExecutor{Rollouts: rollouts, RolloutTimeout: time.Second}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "store"}
	result := PlanApplicationResult{Applied: true, Patches: []PlanPatch{{Ref: ref, Authorization: "accepted-source"}}}
	pending, err := executor.waitAfterSourceWrites(ctx, IncidentRequest{Namespace: "app"}, &result, []sdk.ObjectRef{ref})
	var admission *AdmissionBlockedError
	if !errors.As(err, &admission) || pending == nil || pending.GetReasonCode() != "admission_blocked" || len(pending.GetAdmissionConfigurations()) != 1 {
		t.Fatalf("dependency recovery lost produced admission prerequisite: %+v %v", pending, err)
	}
	if _, err := rollouts.Client.CoreV1().Pods("app").Get(ctx, "consumer", metav1.GetOptions{}); err != nil {
		t.Fatal("consumer recycled before new backend generation became Ready")
	}
}

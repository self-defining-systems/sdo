package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func recoveryPod(name string) *corev1.Pod {
	controller := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", UID: types.UID(name), ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: name, Controller: &controller}}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example:1"}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}}}
}

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
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: rendered.Source{Repository: root, ManifestPaths: []string{"deploy"}}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{true}}
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

func TestRestoredBackendRecyclesStaleServiceConsumersOnlyAfterReady(t *testing.T) {
	for _, ready := range []bool{true, false} {
		t.Run(map[bool]string{true: "ready", false: "not-ready"}[ready], func(t *testing.T) {
			backend := recoveryPod("store")
			backend.Labels = map[string]string{"tier": "store"}
			backend.Status.ContainerStatuses = nil
			backend.Status.Phase = corev1.PodRunning
			backend.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
			consumer := recoveryPod("consumer")
			consumer.Status.Phase = corev1.PodRunning
			consumer.Status.ContainerStatuses = nil
			consumer.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "DATASTORE_ADDRESS", Value: "redis://store.app.svc.cluster.local:6379/0"}}
			unrelated := recoveryPod("unrelated")
			unrelated.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "DATASTORE_ADDRESS", Value: "store.foreign.svc:6379"}}
			roleOnly := recoveryPod("role-only")
			roleOnly.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "ROLE", Value: "store"}}
			service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"tier": "store"}}}
			client := fake.NewSimpleClientset(backend, consumer, unrelated, roleOnly, service)
			sampledReady := false
			client.PrependReactor("list", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if action.(ktesting.ListAction).GetListRestrictions().Labels.String() != "" && ready {
					backend.Status.Conditions[0].Status = corev1.ConditionTrue
					if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), backend, "app"); err != nil {
						t.Fatal(err)
					}
					sampledReady = true
				}
				return false, nil, nil
			})
			client.PrependReactor("delete", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
				if !sampledReady {
					t.Fatal("consumer recycled before backend Ready")
				}
				options := action.(ktesting.DeleteAction).GetDeleteOptions()
				if options.Preconditions == nil || *options.Preconditions.UID != "consumer" || *options.Preconditions.ResourceVersion != "1" {
					t.Fatal("recycle targeted an unrelated Pod or omitted UID/version preconditions")
				}
				return false, nil, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if !ready {
				cancel()
			}
			effects, err := (KubernetesPlanRollouts{Client: client}).RecoverDependencies(ctx, []sdk.ObjectRef{{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store"}})
			if ready {
				if err != nil || len(effects) != 1 || effects[0].Ref.Name != "consumer" || !effects[0].Confirmed {
					t.Fatalf("recovery: %+v %v", effects, err)
				}
			} else if err == nil || len(effects) != 0 {
				t.Fatalf("unready backend caused restart: %+v %v", effects, err)
			}
			for _, name := range []string{"unrelated", "role-only"} {
				if _, err := client.CoreV1().Pods("app").Get(context.Background(), name, metav1.GetOptions{}); err != nil {
					t.Fatal("unrelated consumer deleted")
				}
			}

		})
	}
}

func TestRestoredObjectRelationsCoverMountAndEnvironmentReferences(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		for _, relation := range []string{"volume", "projected", "env", "envFrom"} {
			t.Run(kind+"/"+relation, func(t *testing.T) {
				pod := recoveryPod("worker")
				switch relation {
				case "volume":
					source := corev1.VolumeSource{}
					if kind == "ConfigMap" {
						source.ConfigMap = &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}
					} else {
						source.Secret = &corev1.SecretVolumeSource{SecretName: "startup"}
					}
					pod.Spec.Volumes = []corev1.Volume{{Name: "input", VolumeSource: source}}
				case "projected":
					source := corev1.VolumeProjection{}
					if kind == "ConfigMap" {
						source.ConfigMap = &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}
					} else {
						source.Secret = &corev1.SecretProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}
					}
					pod.Spec.Volumes = []corev1.Volume{{Name: "input", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{source}}}}}
				case "env":
					source := &corev1.EnvVarSource{}
					if kind == "ConfigMap" {
						source.ConfigMapKeyRef = &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}, Key: "config"}
					} else {
						source.SecretKeyRef = &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}, Key: "password"}
					}
					pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "CONFIG", ValueFrom: source}}
				case "envFrom":
					source := corev1.EnvFromSource{}
					if kind == "ConfigMap" {
						source.ConfigMapRef = &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}
					} else {
						source.SecretRef = &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}
					}
					pod.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{source}
				}
				ref := sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: "app", Name: "startup"}
				if !podUsesRestoredObject(pod, ref) {
					t.Fatal("declared relation lost")
				}
				ref.Namespace = "foreign"
				if podUsesRestoredObject(pod, ref) {
					t.Fatal("cross-namespace relation admitted")
				}
			})
		}
	}
}

func TestRestoredDeploymentRecoversItsStaleConsumer(t *testing.T) {
	backend := recoveryPod("store-1")
	backend.Labels = map[string]string{"tier": "store"}
	backend.Status.Phase = corev1.PodRunning
	backend.Status.ContainerStatuses = nil
	backend.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	consumer := recoveryPod("consumer")
	consumer.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "DATASTORE_ENDPOINT", Value: "store:6379"}}
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1, Replicas: 1}, Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "store"}}}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"tier": "store"}}}
	client := fake.NewSimpleClientset(backend, consumer, deployment, service)
	effects, err := (KubernetesPlanRollouts{Client: client}).RecoverDependencies(context.Background(), []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "store"}})
	if err != nil || len(effects) != 1 || effects[0].Ref.Name != "consumer" {
		t.Fatalf("deployment recovery: %+v %v", effects, err)
	}
}

func TestDependencyRecoveryRetainsUnknownDeleteReceipts(t *testing.T) {
	pod := recoveryPod("worker")
	pod.Spec.Volumes = []corev1.Volume{{Name: "startup", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}}}}
	client := fake.NewSimpleClientset(pod)
	client.PrependReactor("delete", "pods", func(ktesting.Action) (bool, kruntime.Object, error) {
		return true, nil, errors.New("lost deletion reply")
	})
	effects, err := (KubernetesPlanRollouts{Client: client}).RecoverDependencies(context.Background(), []sdk.ObjectRef{{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "startup"}})
	if err == nil || len(effects) != 1 || effects[0].State != "unknown" || effects[0].Confirmed {
		t.Fatalf("lost reply became unrecorded/confirmed: %+v %v", effects, err)
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

func TestDatabaseRepairFlowsIntoReadyConsumerRecovery(t *testing.T) {
	action := databaseAction()
	ref := action.Target.Pod
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "mongo"}}})
	object.SetUID(types.UID(ref.Name))
	object.SetResourceVersion("1")
	database := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
	backend := recoveryPod(ref.Name)
	backend.Labels = map[string]string{"tier": "store"}
	backend.Status.Phase = corev1.PodRunning
	backend.Status.ContainerStatuses = nil
	backend.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	consumer := recoveryPod("consumer")
	consumer.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "STORE", Value: "mongodb://store.app:27017"}}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"tier": "store"}}}
	kube := fake.NewSimpleClientset(backend, consumer, service)
	executor := PlanExecutor{Client: database, Rollouts: KubernetesPlanRollouts{Client: kube}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action}}, false)
	if err != nil || database.writes != 1 || len(result.ObjectEffects) != 1 || result.ObjectEffects[0].Ref.Name != "consumer" {
		t.Fatalf("database repair recovery: %+v %v", result, err)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "canary-password") {
		t.Fatal("credential escaped repair record")
	}
}

func TestAcceptedInventoryReportsMissingDeclarationsAcrossSupportedKinds(t *testing.T) {
	for apiKind := range sourceInventoryKinds {
		t.Run(apiKind, func(t *testing.T) {
			separator := strings.LastIndex(apiKind, "/")
			api, kind := apiKind[:separator], apiKind[separator+1:]
			namespace := "app"
			if inventoryClusterKind(kind) {
				namespace = ""
			}
			declared := inventoryObject(api, kind, namespace, "synthetic")
			index := NewSourceInventory("app", "accepted", []*unstructured.Unstructured{declared})
			findings := index.Findings(nil, time.Unix(100, 0))
			if len(findings) != 1 || findings[0].PrimaryResource != inventoryRef(declared) || inventoryEvent(findings[0]).Source != "missing from live inventory" {
				t.Fatalf("missing declaration disappeared: %+v", findings)
			}
			if live := index.Findings([]*unstructured.Unstructured{declared}, time.Unix(101, 0)); len(live) != 0 {
				t.Fatalf("present declaration remained missing: %+v", live)
			}
		})
	}
}

func TestDeletingConfigMapDoesNotRecycleItsConsumers(t *testing.T) {
	pod := recoveryPod("worker")
	pod.Spec.Volumes = []corev1.Volume{{Name: "startup", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "startup"}}}}}
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "startup", Namespace: "app", UID: "config", ResourceVersion: "1"}, Data: map[string]string{"mode": "synthetic"}}
	client := fake.NewSimpleClientset(pod, config)
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: planFakeVerifier{true}}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "startup"}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "explicit removal"}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("app").Get(context.Background(), "worker", metav1.GetOptions{}); err != nil || len(result.ObjectEffects) != 1 {
		t.Fatalf("removal triggered restoration recovery: %+v %v", result.ObjectEffects, err)
	}
}

type countingDependencyRollouts struct {
	planFakeRollout
	recoveries int
}

func (w *countingDependencyRollouts) RecoverDependencies(context.Context, []sdk.ObjectRef) ([]PlanObjectEffect, error) {
	w.recoveries++
	return nil, nil
}

func TestMixedPlanRecoversDependenciesOnceAfterAllWrites(t *testing.T) {
	config := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "startup"}
	peer := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "peer"}
	accepted := planObject(config, nil)
	delete(accepted.Object, "spec")
	accepted.Object["data"] = map[string]any{"mode": "synthetic"}
	object := planObject(peer, map[string]any{"ports": []any{map[string]any{"port": float64(80)}}})
	object.SetUID("peer")
	object.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{peer: object}}}
	rollouts := &countingDependencyRollouts{}
	executor := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{config: accepted}}, Rollouts: rollouts, Verifier: planFakeVerifier{true}}
	plan := program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: config, Reason: "restore source"}, program.PatchObject{Ref: peer, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"repaired":"yes"}}}`), Reason: "repair peer"}}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, plan, false)
	if err != nil || len(result.ActionsApplied) != 2 || rollouts.recoveries != 1 {
		t.Fatalf("dependency recovery repeated within one batch: %d %+v %v", rollouts.recoveries, result, err)
	}
}

type databaseLifecycleTestClient struct {
	*lifecycleTestClient
	catalog *databaseFake
}

func (c databaseLifecycleTestClient) DatabaseUsers(ctx context.Context, target program.DatabaseTarget) ([]program.DatabaseUser, error) {
	return c.catalog.DatabaseUsers(ctx, target)
}
func (c databaseLifecycleTestClient) EnsureDatabaseUser(ctx context.Context, action program.EnsureDatabaseUser) error {
	return c.catalog.EnsureDatabaseUser(ctx, action)
}

func TestRuntimeBatchRecoversOnceAndPreservesSourceCreateReceipt(t *testing.T) {
	config := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "startup"}
	action := databaseAction()
	pod := planObject(action.Target.Pod, map[string]any{"containers": []any{map[string]any{"name": "mongo"}}})
	pod.SetUID("database")
	pod.SetResourceVersion("1")
	base := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{action.Target.Pod: pod}}
	client := databaseLifecycleTestClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: base}, catalog: &databaseFake{planFakeClient: base}}
	accepted := planObject(config, nil)
	delete(accepted.Object, "spec")
	accepted.Object["data"] = map[string]any{"mode": "synthetic"}
	rollouts := &countingDependencyRollouts{}
	executor := PlanExecutor{Client: client, Source: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{config: accepted}}, Rollouts: rollouts, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action, program.CreateFromSource{Ref: config, Reason: "restore source"}}}, false)
	if err != nil || len(result.ActionsApplied) != 2 || rollouts.recoveries != 1 || len(result.ObjectEffects) != 1 || result.ObjectEffects[0].UID != "created-uid" {
		t.Fatalf("runtime batch lost recovery/creation ownership: %+v %v", result, err)
	}
}

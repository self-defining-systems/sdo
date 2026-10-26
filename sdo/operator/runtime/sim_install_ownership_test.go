package runtime

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/install"
)

// Seeded install/restart schedules exercise the actual installer -> drift
// ownership contract. Application extras must remain visible throughout.
func TestSimInstallerOwnershipStaysOutOfApplicationDrift(t *testing.T) {
	cfg := install.Config{Namespace: "app", ControlNamespace: "app-sdo", Application: "app", Model: "gpt-fixture", TimeoutSeconds: 60, ControllerImage: "controller:fixture", ResponderImage: "responder:fixture", ValidatorImage: "validator:fixture", RepositoryPVC: "repository", CredentialsSecret: "fixture", AcceptedCommit: strings.Repeat("a", 40), NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryPushDockerConfig: []byte("{}")}
	cfg.PermissiveResponder = true
	plan, err := install.Resources(cfg, &metav1.APIResourceList{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "services", Kind: "Service", Namespaced: true, Verbs: []string{"get", "list"}}}})
	if err != nil {
		t.Fatal(err)
	}
	objects := []*unstructured.Unstructured{}
	add := func(value any, api, kind string) {
		encoded, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(value)
		if err != nil {
			t.Fatal(err)
		}
		object := &unstructured.Unstructured{Object: encoded}
		object.SetGroupVersionKind(schema.FromAPIVersionAndKind(api, kind))
		objects = append(objects, object)
	}
	for _, role := range plan.ClusterRoles {
		add(role, "rbac.authorization.k8s.io/v1", "ClusterRole")
	}
	for _, binding := range plan.ClusterBindings {
		add(binding, "rbac.authorization.k8s.io/v1", "ClusterRoleBinding")
	}
	for _, role := range plan.AppRoles {
		add(role, "rbac.authorization.k8s.io/v1", "Role")
	}
	for _, binding := range plan.AppBindings {
		add(binding, "rbac.authorization.k8s.io/v1", "RoleBinding")
	}
	// Negative control: each app grant must be visible without installer
	// ownership; names alone cannot explain responder-permissive/read grants.
	checkedGrants := 0
	for _, object := range objects {
		if object.GetNamespace() != cfg.Namespace || (!strings.HasSuffix(object.GetName(), "-responder-permissive") && !strings.HasSuffix(object.GetName(), "-permissive-read")) {
			continue
		}
		checkedGrants++
		stripped := object.DeepCopy()
		stripped.SetLabels(nil)
		if findings := NewSourceInventory(cfg.Namespace, strings.Repeat("a", 40), nil).Findings([]*unstructured.Unstructured{stripped}, time.Unix(1, 0)); len(findings) != 1 {
			t.Fatalf("unlabelled app grant disappeared: %s/%s %+v", object.GetKind(), object.GetName(), findings)
		}
	}
	if checkedGrants < 3 {
		t.Fatalf("permissive grant controls were not exercised: %d", checkedGrants)
	}
	worker := plan.Job.DeepCopy()
	worker.Name = "worker-123"
	worker.UID = types.UID("worker-uid")
	worker.Namespace = cfg.Namespace
	add(worker, "batch/v1", "Job")
	child := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"namespace": cfg.Namespace, "name": "generated-child", "ownerReferences": []any{map[string]any{"apiVersion": "batch/v1", "kind": "Job", "name": worker.Name, "uid": "worker-uid"}}}}}
	for seed := uint64(0); seed < 16; seed++ {
		rng := rand.New(rand.NewPCG(seed, 17))
		order := rng.Perm(len(objects))
		live := []*unstructured.Unstructured{}
		index := NewSourceInventory(cfg.Namespace, strings.Repeat("a", 40), nil)
		assertClear := func(tick int) {
			t.Helper()
			if findings := index.Findings(live, time.Unix(int64(tick), 0)); len(findings) != 0 {
				t.Fatalf("seed %d tick %d installed objects became app drift: %+v", seed, tick, findings)
			}
		}
		for tick, position := range order {
			live = append(live, objects[position])
			assertClear(tick)
		}
		live = append(live, child)
		assertClear(20)
		index = NewSourceInventory(cfg.Namespace, strings.Repeat("a", 40), nil)
		assertClear(21)
		foreign := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"namespace": cfg.Namespace, "name": "application-extra"}}}
		live = append(live, foreign)
		findings := index.Findings(live, time.Unix(22, 0))
		if len(findings) != 1 || findings[0].PrimaryResource.Name != "application-extra" {
			t.Fatalf("seed %d foreign app object hidden: %+v", seed, findings)
		}
		live = live[:len(live)-1]
		assertClear(23)
	}
}

package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestAcceptedClusterSourceCanBeInspectedAndReconciled(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	nativeGit(t, root, "init", "-q")
	nativeGit(t, root, "config", "user.name", "Test")
	nativeGit(t, root, "config", "user.email", "test@example.invalid")
	manifest := `apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: app-cleanup
rules:
- apiGroups: [""]
  resources: [configmaps]
  verbs: [get, patch, update]
`
	if err := os.WriteFile(filepath.Join(root, "role.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, root, "add", "role.yaml")
	nativeGit(t, root, "commit", "-qm", "source")
	source := GitPlanSource{Repository: root, ManifestPaths: []string{"role.yaml"}}
	commit := seedRenderedLock(t, source, nativeGit(t, root, "rev-parse", "HEAD"), "app")
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "app-cleanup", UID: "role-uid", ResourceVersion: "1"}, Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}}}
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: role.Name}
	live, err := planUnstructured(role, ref)
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*live}}, Source: source, SourceCommit: commit}
	diff, err := view.DiffAgainstSource(ref)
	if err != nil || !diff.Changed || !strings.Contains(diff.Text, "rules") {
		t.Fatalf("source RBAC drift: %+v %v", diff, err)
	}
	loop := NewResponderLoop(IncidentRequest{Namespace: "app", SourceCommit: commit}, nil, nil, nil, PlanExecutor{Source: source}, 1)
	if response := loop.Handle(ctx, ToolRequest{Command: "source", Ref: &ref}); response.Error != "" || response.Source == nil {
		t.Fatalf("cluster source tool: %+v", response)
	}
	declared, err := source.AcceptedManifests(ctx, commit, "app")
	if err != nil {
		t.Fatal(err)
	}
	if declared[0].GetNamespace() != "" {
		t.Fatal("accepted ClusterRole became namespaced")
	}
	findings := NewSourceInventory("app", commit, declared).Findings([]*unstructured.Unstructured{live}, time.Unix(100, 0))
	if len(findings) != 1 || findings[0].PrimaryResource != ref {
		t.Fatalf("source RBAC fault is invisible: %+v", findings)
	}
	client := fake.NewSimpleClientset(role)
	executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
	request := IncidentRequest{Namespace: "app", SourceCommit: commit, RepairPolicy: "recorded-actions", Findings: findings}
	result, err := executor.Apply(ctx, request, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false)
	if err != nil || !result.Applied {
		t.Fatalf("cluster source reconcile: %+v %v", result, err)
	}
	fixed, err := client.RbacV1().ClusterRoles().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fixedObject, _ := planUnstructured(fixed, ref)
	if got := NewSourceInventory("app", commit, declared).Findings([]*unstructured.Unstructured{fixedObject}, time.Unix(101, 0)); len(got) != 0 {
		t.Fatalf("restored role still drifted: %+v", got)
	}
	ref.Name = "undeclared-role"
	if _, err := executor.Apply(ctx, request, program.Plan{Actions: []program.Action{program.ReconcileFromSource{Ref: ref}}}, false); err == nil {
		t.Fatal("undeclared cluster role admitted")
	}
}

func TestSourceIdentityWithoutConfigurationNeedsNoAdoptionWrite(t *testing.T) {
	controller, _, source, closeServer := adoptionController(t, 8080)
	defer closeServer()
	source.object = &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": "app-identity", "namespace": "app"}}}
	client := fake.NewSimpleClientset()
	controller.drift.Client = client
	controller.drift.Source = source
	if err := controller.adoptOwnership(context.Background(), time.Unix(100, 0), sdktest.Snapshot{NamespaceName: "app"}); err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != 0 {
		t.Fatalf("identity-only source required API authority: %+v", client.Actions())
	}
}

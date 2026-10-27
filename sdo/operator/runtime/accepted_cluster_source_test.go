package runtime

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
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
	testutil.InitGit(t, root)
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
	commit := nativeGit(t, root, "rev-parse", "HEAD")
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

	assertClusterSourceStatusVerify(t, ref, view, false)
	// Source-absent RBAC objects use the application lock too, without a foreign
	// webhook ObservationTarget. Both status and verification consume this diff.
	for _, kind := range []string{"ClusterRole", "ClusterRoleBinding"} {
		oldRef := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: kind, Name: "old-object"}
		old := unstructured.Unstructured{Object: map[string]any{"apiVersion": oldRef.APIVersion, "kind": kind, "metadata": map[string]any{"name": oldRef.Name}}}
		oldView := view
		oldView.DetectionContext = sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{old}}
		if d, e := oldView.DiffAgainstSource(oldRef); e != nil || !d.Changed || !d.SourceAbsent {
			t.Fatalf("present old %s: %+v %v", kind, d, e)
		}
		assertClusterSourceStatusVerify(t, oldRef, oldView, false)
		oldView.DetectionContext = clusterEvidenceSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app"}, err: apierrors.NewNotFound(schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"}, oldRef.Name)}
		if d, e := oldView.DiffAgainstSource(oldRef); e != nil || d.Changed || !d.SourceAbsent {
			t.Fatalf("confirmed absent old %s: %+v %v", kind, d, e)
		}
		assertClusterSourceStatusVerify(t, oldRef, oldView, true)
		oldView.DetectionContext = clusterEvidenceSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app"}, err: sdk.ErrObjectNotCaptured}
		if d, e := oldView.DiffAgainstSource(oldRef); e == nil {
			t.Fatalf("uncaptured old %s became healthy: %+v", kind, d)
		}
		assertClusterSourceStatusVerify(t, oldRef, oldView, false)
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

type clusterEvidenceSnapshot struct {
	sdktest.Snapshot
	err error
}

func (s clusterEvidenceSnapshot) Object(sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return nil, s.err
}
func TestAcceptedClusterSourceMissingObservationIsNotAbsence(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "old-role"}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not-captured", sdk.ErrObjectNotCaptured},
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Group: "rbac.authorization.k8s.io", Resource: "clusterroles"}, ref.Name, errors.New("denied"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := LiveReadView{DetectionContext: clusterEvidenceSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app"}, err: tc.err}, Source: planFakeSource{}, SourceCommit: "accepted"}
			_, err := view.snapshotObject(ref)
			if !errors.Is(err, tc.err) {
				t.Fatalf("missing observation became absence: %v", err)
			}
		})
	}
}

func assertClusterSourceStatusVerify(t *testing.T, ref sdk.ObjectRef, view LiveReadView, healthy bool) {
	t.Helper()
	finding := unexplainedFinding()
	finding.PrimaryResource = ref
	finding.Metadata = map[string]any{"drift_event": sdk.DriftEvent{Ref: ref, Classification: sdk.DriftUnexplained}}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "cluster-source-parity", Namespace: "app", SourceCommit: view.SourceCommit, Findings: []sdk.Finding{finding}}, nil, staticProvider{snapshot: view.DetectionContext}, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, PlanExecutor{Source: view.Source}, 1)
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		copy := view
		copy.DetectionContext = snapshot
		return copy
	}
	for _, command := range []string{"status", "verify"} {
		response := loop.Handle(context.Background(), ToolRequest{Command: command})
		if response.Healthy != healthy {
			t.Fatalf("%s disagrees with source evidence for %+v: %+v", command, ref, response)
		}
	}
}

// Explicit deletion evidence, unlike an object omitted from an incomplete capture.
type confirmedAbsentSnapshot struct {
	sdktest.Snapshot
	Ref sdk.ObjectRef
}

func (s confirmedAbsentSnapshot) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref == s.Ref {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: ref.Kind}, ref.Name)
	}
	return s.Snapshot.Object(ref)
}

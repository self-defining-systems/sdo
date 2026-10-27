package runtime

import (
	"context"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func TestRemovalOwnershipIncludesUnrelatedLiveBindings(t *testing.T) {
	for _, mode := range []string{"cluster-binding", "foreign-rolebinding", "unrelated-aggregation", "matching-aggregation"} {
		t.Run(mode, func(t *testing.T) {
			objects := latentInventoryObjects()[3:]
			role, binding := objects[0], objects[1]
			other := binding.DeepCopy()
			other.SetName("unrelated")
			switch mode {
			case "cluster-binding":
				other.Object["subjects"] = []any{map[string]any{"kind": "User", "name": "outside"}}
			case "foreign-rolebinding":
				other.SetKind("RoleBinding")
				other.SetNamespace("outside")
			default:
				other = role.DeepCopy()
				other.SetName("aggregate")
				other.Object["aggregationRule"] = map[string]any{"clusterRoleSelectors": []any{map[string]any{"matchLabels": map[string]any{"export": "yes"}}}}
				if mode == "matching-aggregation" {
					role.SetLabels(map[string]string{"export": "yes"})
				}
			}
			c, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range []*unstructured.Unstructured{role, binding, other} {
				if err := c.informers[o.GetAPIVersion()+"/"+o.GetKind()].GetStore().Add(o); err != nil {
					t.Fatal(err)
				}
			}
			index := NewSourceInventory("app", "accepted", nil)
			// This fixture fills stores directly; production readiness is separately checked.
			findings := sourceInventoryFindingsFor(index, c, time.Now())
			allowed := false
			for _, f := range findings {
				if f.PrimaryResource.Kind == "ClusterRole" && f.PrimaryResource.Name == role.GetName() {
					allowed = f.Metadata["removal_blocker"] != "shared_ownership"
				}
			}
			if allowed != (mode == "unrelated-aggregation") {
				t.Fatalf("filtered ownership evidence mode=%s allowed=%v findings=%+v", mode, allowed, findings)
			}
		})
	}
}

func TestRemovalRoleBindingInformerObservesOtherNamespaces(t *testing.T) {
	client := fake.NewSimpleClientset(&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "outside", Namespace: "other"}})
	c, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: client, TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.Start(ctx)
	defer c.Stop()
	if err := c.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
	informer := c.informers["rbac.authorization.k8s.io/v1/RoleBinding"]
	if _, found, _ := informer.GetStore().GetByKey("other/outside"); !found {
		t.Fatal("ownership cache cannot observe foreign RoleBinding")
	}
	for _, object := range c.SourceDriftObjects() {
		if object.GetKind() == "RoleBinding" && object.GetNamespace() == "other" {
			t.Fatal("foreign binding leaked into application findings")
		}
	}
}

func TestSharedOwnershipEndsResponseWithoutContinuation(t *testing.T) {
	client := fake.NewSimpleClientset()
	request := goldenRequest(t)
	request.ResponseDeadline = time.Now().Add(time.Minute)
	request.RepositoryWorktree = t.TempDir() + "/proposal"
	request.MaxResponseLoops = 3
	objects := latentInventoryObjects()[3:]
	objects[0].SetName("cluster-admin")
	objects[1].Object["roleRef"].(map[string]any)["name"] = "cluster-admin"
	findings := NewSourceInventory("app", "accepted", nil).Findings(objects, time.Now())
	var blocked sdk.Finding
	for _, finding := range findings {
		if finding.PrimaryResource.Kind == "ClusterRole" {
			blocked = finding
		}
	}
	if blocked.Metadata["removal_blocker"] != "shared_ownership" {
		t.Fatal("fixture lacks typed blocker")
	}
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 3)
	loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse {
		return ToolResponse{Summary: "shared authorization requires escalation", Findings: []sdk.Finding{blocked}}
	}
	d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type reply struct {
		result IncidentResult
		err    error
	}
	done := make(chan reply, 1)
	go func() { result, err := d.Dispatch(ctx, request); done <- reply{result, err} }()
	job := awaitJob(t, client, IncidentJobName(request.IncidentID))
	publishContinuationResult(t, client, job, completedResult(request.IncidentID))
	outcome := <-done
	if outcome.err != nil || outcome.result.Status != IncidentFailed || outcome.result.Error != "shared_ownership" || outcome.result.ResponseLoopsUsed != 1 {
		t.Fatalf("shared ownership retried or claimed recovery: %+v error=%v", outcome.result, outcome.err)
	}
	resumed, err := d.ContinueAfterVerification(ctx, request, ToolResponse{Findings: []sdk.Finding{blocked}}, time.Now())
	if err != nil || resumed {
		t.Fatalf("terminal shared ownership resumed: %v %v", resumed, err)
	}
}

func TestRemovalForeignBindingDeletionOnlyWakesOwnership(t *testing.T) {
	c, err := newCacheForTest(t, KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	binding := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "other"}}
	handler := c.eventHandler(sdk.WatchKind{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding", Namespace: "app"})
	handler.OnDelete(cache.DeletedFinalStateUnknown{Key: "other/unrelated", Obj: binding})
	if changes := c.TakeObjectChanges(); len(changes) != 0 {
		t.Fatalf("foreign tombstone admitted as application drift: %+v", changes)
	}
	if len(c.TakeEvents()) != 1 {
		t.Fatal("ownership graph was not woken")
	}
}

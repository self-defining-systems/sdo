package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const holdsDataArchitecture = "# Architecture\n\n<!-- sdo:application-map:begin -->\n```yaml\nversion: 1\ncomponents:\n" +
	"  - {name: api, workload: Deployment/api, services: [api], holds_data: true}\n" +
	"  - {name: store, workload: StatefulSet/store, services: [store], holds_data: true}\n" +
	"  - {name: web, workload: Deployment/web, services: [web], holds_data: false}\n" +
	"```\n<!-- sdo:application-map:end -->\n"

func fakeArchitecture(document string) gitRead {
	return func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) == 2 && args[0] == "show" && args[1] == "synthetic-accepted:"+layout.ArchitectureFile && document != "" {
			return []byte(document), nil
		}
		return nil, errors.New("synthetic: not in the repository")
	}
}

func runHoldsDataStage(t *testing.T, document string) (*sameWriteWorld, map[string]string, map[string]string) {
	t.Helper()
	world, loop, state := sameWriteLoop(t)
	loop.GitRead = fakeArchitecture(document)
	state.Request.RepositoryWorktree = "synthetic-worktree"
	loop.Request.RepositoryWorktree = "synthetic-worktree"
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		recoveryWait: func(context.Context, time.Duration) error { return nil }}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	reasons, details := map[string]string{}, map[string]string{}
	for _, attempt := range report.Mitigations {
		reasons[attempt.PlaybookId] = attempt.SkipReason
		details[attempt.PlaybookId] = attempt.Error
		if attempt.SkipReason == stageHoldsData && (attempt.Result != "skipped" || attempt.GetApplyReceipt() != nil) {
			t.Fatalf("a refused program must be recorded as skipped with no receipt: %v", attempt)
		}
	}
	if attempts := len(report.Mitigations); attempts == 0 {
		t.Fatal("the stage attempted nothing")
	}
	if len(state.Findings) == 0 {
		t.Fatal("a refused program must leave its findings to the responder")
	}
	return world, reasons, details
}

// The opening incident of this rule: a matched program restarts a store the
// map says holds data. The stage writes nothing and tells the responder why.
func TestDeterministicStageRefusesRestartOfDataHoldingWorkload(t *testing.T) {
	world, reasons, details := runHoldsDataStage(t, holdsDataArchitecture)
	if got := world.restarts.Load(); got != 0 {
		t.Fatalf("the data-holding Deployment was written %d times", got)
	}
	if reasons["aware-restart"] != stageHoldsData || reasons["blind-restart"] != stageHoldsData {
		t.Fatalf("both matched programs must be refused: %v", reasons)
	}
	if !strings.Contains(details["aware-restart"], "Deployment/api") || !strings.Contains(details["aware-restart"], "holds_data") {
		t.Fatalf("the refusal must name the workload and the map fact: %q", details["aware-restart"])
	}
}

// Everything else runs as it did: no map, or a map that does not mark it.
func TestDeterministicStageRunsMatchedProgramsWithoutDataHoldingMark(t *testing.T) {
	for name, document := range map[string]string{
		"no architecture": "",
		"no map":          "# Architecture\n",
		"malformed map":   "<!-- sdo:application-map:begin -->\n```yaml\nnot: [a map\n```\n<!-- sdo:application-map:end -->\n",
		"not marked":      strings.ReplaceAll(holdsDataArchitecture, "holds_data: true", "holds_data: false"),
	} {
		t.Run(name, func(t *testing.T) {
			world, reasons, _ := runHoldsDataStage(t, document)
			if got := world.restarts.Load(); got != 1 {
				t.Fatalf("the Deployment was written %d times, want the one restart", got)
			}
			for id, reason := range reasons {
				if reason == stageHoldsData {
					t.Fatalf("%s was refused without a data-holding mark", id)
				}
			}
		})
	}
}

func TestDataWorkloadWriteClassifiesEveryPodReplacingAction(t *testing.T) {
	replicas := func(n int32) *int32 { return &n }
	controller := true
	owned := func(kind, name string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &controller}}
	}
	view := sdktest.Snapshot{
		NamespaceName: "app",
		DeploymentList: []appsv1.Deployment{
			{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Replicas: replicas(2)}},
			{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Replicas: replicas(2)}},
		},
		StatefulSetList: []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}, Spec: appsv1.StatefulSetSpec{Replicas: replicas(0)}}},
		ReplicaSetList: []appsv1.ReplicaSet{
			{ObjectMeta: metav1.ObjectMeta{Name: "api-1", Namespace: "app", OwnerReferences: owned("Deployment", "api")}},
			{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "app", OwnerReferences: owned("Deployment", "web")}},
		},
		PodList: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "api-1-a", Namespace: "app", OwnerReferences: owned("ReplicaSet", "api-1")}},
			{ObjectMeta: metav1.ObjectMeta{Name: "store-0", Namespace: "app", OwnerReferences: owned("StatefulSet", "store")}},
			{ObjectMeta: metav1.ObjectMeta{Name: "web-1-a", Namespace: "app", OwnerReferences: owned("ReplicaSet", "web-1")}},
		},
	}
	ref := func(apiVersion, kind, name string) sdk.ObjectRef {
		return sdk.ObjectRef{APIVersion: apiVersion, Kind: kind, Namespace: "app", Name: name}
	}
	api, store, web := ref("apps/v1", "Deployment", "api"), ref("apps/v1", "StatefulSet", "store"), ref("apps/v1", "Deployment", "web")
	apiPod, storePod, webPod := ref("v1", "Pod", "api-1-a"), ref("v1", "Pod", "store-0"), ref("v1", "Pod", "web-1-a")
	apiRS := ref("apps/v1", "ReplicaSet", "api-1")
	config := ref("v1", "ConfigMap", "api-config")
	for name, test := range map[string]struct {
		action  program.Action
		refused string
	}{
		"restart":                  {program.RestartRollout{Ref: api}, "Deployment/api"},
		"restart by pointer":       {&program.RestartRollout{Ref: api}, "Deployment/api"},
		"rollback":                 {program.Rollback{Ref: api, ToRevision: "1"}, "Deployment/api"},
		"reconcile":                {program.ReconcileFromSource{Ref: store}, "StatefulSet/store"},
		"delete workload":          {program.DeleteObject{Ref: store}, "StatefulSet/store"},
		"delete pod":               {program.DeletePod{Ref: apiPod}, "Deployment/api"},
		"delete stateful pod":      {program.DeletePod{Ref: storePod}, "StatefulSet/store"},
		"delete pod as object":     {program.DeleteObject{Ref: apiPod}, "Deployment/api"},
		"retire revision":          {program.RetireOldRevision{Ref: apiRS}, "Deployment/api"},
		"scale replicaset":         {program.ScaleWorkload{Ref: apiRS, Replicas: program.FromParam("n")}, "Deployment/api"},
		"scale from source":        {program.ScaleWorkload{Ref: api, Replicas: program.FromSource(api, "spec.replicas")}, "Deployment/api"},
		"template field":           {program.SetField{Ref: api, Path: "spec.template.spec.containers[0].image", Value: program.FromSource(api, "x")}, "Deployment/api"},
		"replicas field":           {program.SetField{Ref: api, Path: "spec.replicas", Value: program.FromSource(api, "spec.replicas")}, "Deployment/api"},
		"patch workload":           {program.PatchObject{Ref: api, Type: "merge"}, "Deployment/api"},
		"apply workload":           {program.ApplyObject{Ref: api}, "Deployment/api"},
		"evict pod by request":     {program.KubernetesRequest{Ref: apiPod, Method: "POST", Path: "/api/v1/namespaces/app/pods/api-1-a/eviction"}, "Deployment/api"},
		"delete by raw path":       {program.KubernetesRequest{Method: "DELETE", Path: "/apis/apps/v1/namespaces/app/statefulsets/store"}, "StatefulSet/store"},
		"evict by raw path":        {program.KubernetesRequest{Method: "POST", Path: "/api/v1/namespaces/app/pods/api-1-a/eviction"}, "Deployment/api"},
		"scale up from zero":       {program.ScaleWorkload{Ref: store, Replicas: program.FromSource(store, "spec.replicas")}, ""},
		"read by request":          {program.KubernetesRequest{Ref: api, Method: "GET", Path: "/apis/apps/v1/namespaces/app/deployments/api"}, ""},
		"wait":                     {program.WaitFor{Ref: api, Path: "/status/readyReplicas", Equals: 1}, ""},
		"exec keeps the pod":       {program.Exec{Pod: apiPod, Container: "api"}, ""},
		"recreate from source":     {program.CreateFromSource{Ref: api}, ""},
		"restart stateless":        {program.RestartRollout{Ref: web}, ""},
		"delete stateless pod":     {program.DeletePod{Ref: webPod}, ""},
		"unrelated object":         {program.DeleteObject{Ref: config}, ""},
		"same name other space":    {program.RestartRollout{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other", Name: "api"}}, ""},
		"pod no longer observable": {program.DeletePod{Ref: ref("v1", "Pod", "gone")}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			got := dataWorkloadWrite(view, program.Plan{Actions: []program.Action{test.action}}, map[string]bool{"Deployment/api": true, "StatefulSet/store": true}, "app")
			if test.refused == "" && got != "" {
				t.Fatalf("refused: %q", got)
			}
			if test.refused != "" && !strings.Contains(got, test.refused) {
				t.Fatalf("got %q, want a refusal naming %s", got, test.refused)
			}
		})
	}
	if got := dataWorkloadWrite(view, program.Plan{Actions: []program.Action{program.RestartRollout{Ref: api}}}, nil, "app"); got != "" {
		t.Fatalf("no map must refuse nothing: %q", got)
	}
}

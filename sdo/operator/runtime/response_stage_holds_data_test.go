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

// holdsDataView is the cluster of holdsDataArchitecture: api and store hold
// data, web does not.
func holdsDataView() sdktest.Snapshot {
	replicas := func(n int32) *int32 { return &n }
	controller := true
	owned := func(kind, name string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &controller}}
	}
	return sdktest.Snapshot{
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
		ServiceList: []corev1.Service{
			{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}},
		},
	}
}

func TestDataWorkloadWriteClassifiesEveryPodReplacingAction(t *testing.T) {
	view := holdsDataView()
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
		"exec into a data pod":     {program.Exec{Pod: apiPod, Container: "api"}, "Deployment/api"},
		"recreate from source":     {program.CreateFromSource{Ref: api}, ""},
		"restart stateless":        {program.RestartRollout{Ref: web}, ""},
		"delete stateless pod":     {program.DeletePod{Ref: webPod}, ""},
		"unrelated object":         {program.DeleteObject{Ref: config}, ""},
		"same name other space":    {program.RestartRollout{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other", Name: "api"}}, ""},
		"pod no longer observable": {program.DeletePod{Ref: ref("v1", "Pod", "gone")}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			got := dataWorkloadWrite(view, program.Plan{Actions: []program.Action{test.action}}, holdsDataHolders(), "app")
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

// holdsDataHolders reads holdsDataArchitecture the way the stage does.
func holdsDataHolders() dataHolders {
	return acceptedDataWorkloads(context.Background(), fakeArchitecture(holdsDataArchitecture), "synthetic-worktree", "synthetic-accepted")
}

func holdsDataRef(apiVersion, kind, name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: apiVersion, Kind: kind, Namespace: "app", Name: name}
}

// stageRefusal is what the automatic stage says about a one-action plan.
func stageRefusal(action program.Action) string {
	return dataWorkloadWrite(holdsDataView(), program.Plan{Actions: []program.Action{action}}, holdsDataHolders(), "app")
}

func requireHoldsDataRefusal(t *testing.T, got string, verb, workload string) {
	t.Helper()
	if !strings.Contains(got, verb) || !strings.Contains(got, workload) || !strings.Contains(got, stageHoldsData) {
		t.Fatalf("got %q, want a %s refusal naming %s and the holds_data mark", got, verb, workload)
	}
}

// boundValues are values whose text the plan resolved from its parameters,
// the way the controller binds a plan after Mitigate.
func boundValues(t *testing.T, words map[string]any) map[string]program.Value {
	t.Helper()
	spec := program.ProgramSpec{ID: "synthetic"}
	values := map[string]program.Value{}
	for name := range words {
		spec.Params = append(spec.Params, program.ParamSpec{Name: name, ValueKind: program.StringValue})
		values[name] = program.FromParam(name)
	}
	plan := program.BindPlanValues(program.Plan{Actions: []program.Action{program.RunScript{Script: ":", Env: values}}}, program.ValueInputs{Spec: spec, Bindings: program.Bindings{Values: words}})
	return plan.Actions[0].(program.RunScript).Env
}

// An opaque command inside a data store can do anything to its records, such
// as a flush. The automatic stage cannot see inside it, so it refuses.
func TestStageHoldsDataRefusesExecIntoDataPod(t *testing.T) {
	command := boundValues(t, map[string]any{"word": "true"})["word"]
	requireHoldsDataRefusal(t, stageRefusal(program.Exec{Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Command: []program.Value{command}}), "exec", "StatefulSet/store")
	requireHoldsDataRefusal(t, stageRefusal(&program.Exec{Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Command: []program.Value{command}}), "exec", "StatefulSet/store")
}

func TestStageHoldsDataRefusesEnsureExecIntoDataPod(t *testing.T) {
	observe := program.Command{Pod: holdsDataRef("v1", "Pod", "web-1-a"), Container: "web", Args: []string{"true"}}
	ensure := program.Command{Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Args: []string{"sh", "-c", "flush-everything"}}
	requireHoldsDataRefusal(t, stageRefusal(program.EnsureExec{Observe: observe, Ensure: ensure}), "exec", "StatefulSet/store")
	// The command runs in a stateless Pod but addresses the store by name.
	ensure = program.Command{Pod: holdsDataRef("v1", "Pod", "web-1-a"), Container: "web", Args: []string{"client", "-h", "store.app.svc", "flush"}}
	requireHoldsDataRefusal(t, stageRefusal(program.EnsureExec{Observe: observe, Ensure: ensure}), "exec", "StatefulSet/store")
}

func TestStageHoldsDataRefusesEnsureDatabaseUserInDataPod(t *testing.T) {
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Database: "app"}
	requireHoldsDataRefusal(t, stageRefusal(program.EnsureDatabaseUser{Target: target, User: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}, Password: program.CredentialRef{Env: "PASSWORD"}}), "database user", "StatefulSet/store")
}

func TestStageHoldsDataRefusesRunScriptBoundToDataService(t *testing.T) {
	service := holdsDataRef("v1", "Service", "store")
	observed := program.BindPlanValues(program.Plan{Actions: []program.Action{program.RunScript{Script: `client -h "$HOST" flush`, Env: map[string]program.Value{"HOST": program.FromObserved(service, "metadata.name")}}}},
		program.ValueInputs{Observed: func(sdk.ObjectRef) (map[string]any, error) {
			return map[string]any{"kind": "Service", "metadata": map[string]any{"name": "store", "resourceVersion": "1"}}, nil
		}}).Actions[0]
	for name, action := range map[string]program.Action{
		"observed service":      observed,
		"unbound service value": program.RunScript{Script: `client -h "$HOST" flush`, Env: map[string]program.Value{"HOST": program.FromObserved(service, "metadata.name")}},
		"parameter names it":    program.RunScript{Script: `client -h "$HOST" flush`, Env: boundValues(t, map[string]any{"HOST": "store.app.svc:6379"})},
		"script names it":       program.RunScript{Script: "client -h store flush"},
		"script names its pod":  program.RunScript{Script: "kubectl exec store-0 -- flush"},
		"workload object":       program.RunScript{Script: `kubectl delete pod -l "app=$APP"`, Env: map[string]program.Value{"APP": program.FromObserved(holdsDataRef("apps/v1", "StatefulSet", "store"), "metadata.name")}},
		"input unreadable here": program.RunScript{Script: `client -h "$HOST" flush`, Env: map[string]program.Value{"HOST": program.FromSource(holdsDataRef("v1", "ConfigMap", "settings"), "data.host")}},
	} {
		t.Run(name, func(t *testing.T) {
			requireHoldsDataRefusal(t, stageRefusal(action), "script", "StatefulSet/store")
		})
	}
}

func TestStageHoldsDataRefusesOtherOpaqueActionsReachingData(t *testing.T) {
	webPod := holdsDataRef("v1", "Pod", "web-1-a")
	job := holdsDataRef("batch/v1", "Job", "fixer")
	manifest := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"name": "fixer"}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "fix", "args": []any{"client", "-h", "store", "flush"}}}}}}}
	requireHoldsDataRefusal(t, stageRefusal(program.RunJob{Object: program.ApplyObject{Ref: job, Object: manifest}, Completion: program.WaitFor{Ref: job}}), "job", "StatefulSet/store")
	request := program.HTTPRequest{Pod: webPod, Container: "web", Method: "POST", URL: "http://store:8080/admin/flush"}
	requireHoldsDataRefusal(t, stageRefusal(program.EnsureHTTP{Observe: request, Ensure: request}), "http", "StatefulSet/store")
}

// The class stops at data: opaque actions that reach no data component run as before.
func TestStageHoldsDataAllowsExecIntoStatelessPod(t *testing.T) {
	webPod := holdsDataRef("v1", "Pod", "web-1-a")
	command := boundValues(t, map[string]any{"word": "true"})["word"]
	job := holdsDataRef("batch/v1", "Job", "fixer")
	manifest := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"name": "fixer"}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "fix", "args": []any{"true"}}}}}}}
	request := program.HTTPRequest{Pod: webPod, Container: "web", Method: "POST", URL: "http://web:8080/admin/reload"}
	for name, action := range map[string]program.Action{
		"exec":                program.Exec{Pod: webPod, Container: "web", Command: []program.Value{command}},
		"ensure exec":         program.EnsureExec{Observe: program.Command{Pod: webPod, Container: "web", Args: []string{"true"}}, Ensure: program.Command{Pod: webPod, Container: "web", Args: []string{"reload"}}},
		"script":              program.RunScript{Script: "curl -fsS http://web/healthz"},
		"script bound to web": program.RunScript{Script: `curl -fsS "http://$HOST/healthz"`, Env: boundValues(t, map[string]any{"HOST": "web"})},
		"store as substring":  program.RunScript{Script: "restore-web-config"},
		"job":                 program.RunJob{Object: program.ApplyObject{Ref: job, Object: manifest}, Completion: program.WaitFor{Ref: job}},
		"http":                program.EnsureHTTP{Observe: request, Ensure: request},
	} {
		t.Run(name, func(t *testing.T) {
			if got := stageRefusal(action); got != "" {
				t.Fatalf("refused: %q", got)
			}
		})
	}
	if got := dataWorkloadWrite(holdsDataView(), program.Plan{Actions: []program.Action{program.RunScript{Script: "client -h store flush"}}}, nil, "app"); got != "" {
		t.Fatalf("no map must refuse nothing: %q", got)
	}
}

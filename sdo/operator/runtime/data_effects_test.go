package runtime

import (
	"context"
	"errors"
	"testing"

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

// A learned restart of a workload the map marks holds_data runs on the
// automatic path like any other match: SDO refuses no repair for its kind.
func TestDeterministicStageRunsLearnedRestartOfDataHoldingWorkload(t *testing.T) {
	world, loop, state := sameWriteLoop(t)
	loop.GitRead = fakeArchitecture(holdsDataArchitecture)
	state.Request.RepositoryWorktree = "synthetic-worktree"
	loop.Request.RepositoryWorktree = "synthetic-worktree"
	instant := newInstantRecoveryClock()
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, KeepLoop: true,
		recoveryNow: instant.now, recoveryWait: instant.wait}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) == 0 {
		t.Fatal("the stage attempted nothing")
	}
	if got := world.restarts.Load(); got != 1 {
		t.Fatalf("the data-holding Deployment was written %d times, want the one restart", got)
	}
	for _, attempt := range report.Mitigations {
		if attempt.SkipReason == "holds_data" {
			t.Fatalf("%s was skipped for holds_data", attempt.PlaybookId)
		}
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

// holdsDataHolders reads holdsDataArchitecture the way the stage does.
func holdsDataHolders() dataHolders {
	return acceptedDataWorkloads(context.Background(), fakeArchitecture(holdsDataArchitecture), "synthetic-worktree", "synthetic-accepted")
}

func holdsDataRef(apiVersion, kind, name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: apiVersion, Kind: kind, Namespace: "app", Name: name}
}

// requireRecordedDataEffect holds when SDO records the action's data effect on
// workload: the class is evidence, not a gate.
func requireRecordedDataEffect(t *testing.T, action program.Action, verb, workload string, effect program.DataEffect) {
	t.Helper()
	holders := holdsDataHolders()
	effects := dataReach{view: holdsDataView(), holders: holders, namespace: "app"}.effects(action)
	if effects.verb != verb {
		t.Fatalf("verb = %q, want %q", effects.verb, verb)
	}
	for _, touch := range effects.touches {
		if holders.named(touch.Component).Workload == workload && touch.Effect == effect {
			return
		}
	}
	if effects.unreadInput && effect == program.EffectUnknown {
		return
	}
	t.Fatalf("effects = %+v, want %s on %s recorded", effects, effect, workload)
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
// as a flush. SDO cannot see inside it, so its effect is recorded unknown.
func TestDataEffectsRecordExecIntoDataPod(t *testing.T) {
	command := boundValues(t, map[string]any{"word": "true"})["word"]
	requireRecordedDataEffect(t, program.Exec{Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Command: []program.Value{command}}, "exec", "StatefulSet/store", program.EffectUnknown)
	requireRecordedDataEffect(t, &program.Exec{Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Command: []program.Value{command}}, "exec", "StatefulSet/store", program.EffectUnknown)
}

func TestDataEffectsRecordEnsureExecIntoDataPod(t *testing.T) {
	observe := program.Command{Pod: holdsDataRef("v1", "Pod", "web-1-a"), Container: "web", Args: []string{"true"}}
	ensure := program.Command{Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Args: []string{"sh", "-c", "flush-everything"}}
	requireRecordedDataEffect(t, program.EnsureExec{Observe: observe, Ensure: ensure}, "exec", "StatefulSet/store", program.EffectUnknown)
	// The command runs in a stateless Pod but addresses the store by name.
	ensure = program.Command{Pod: holdsDataRef("v1", "Pod", "web-1-a"), Container: "web", Args: []string{"client", "-h", "store.app.svc", "flush"}}
	requireRecordedDataEffect(t, program.EnsureExec{Observe: observe, Ensure: ensure}, "exec", "StatefulSet/store", program.EffectUnknown)
}

func TestDataEffectsRecordEnsureDatabaseUserInDataPod(t *testing.T) {
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: holdsDataRef("v1", "Pod", "store-0"), Container: "store", Database: "app"}
	requireRecordedDataEffect(t, program.EnsureDatabaseUser{Target: target, User: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}, Password: program.CredentialRef{Env: "PASSWORD"}}, "database user change", "StatefulSet/store", program.EffectStoreSetting)
}

func TestDataEffectsRecordRunScriptBoundToDataService(t *testing.T) {
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
			requireRecordedDataEffect(t, action, "script", "StatefulSet/store", program.EffectUnknown)
		})
	}
}

func TestDataEffectsRecordOtherOpaqueActionsReachingData(t *testing.T) {
	webPod := holdsDataRef("v1", "Pod", "web-1-a")
	job := holdsDataRef("batch/v1", "Job", "fixer")
	manifest := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"name": "fixer"}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "fix", "args": []any{"client", "-h", "store", "flush"}}}}}}}
	requireRecordedDataEffect(t, program.RunJob{Object: program.ApplyObject{Ref: job, Object: manifest}, Completion: program.WaitFor{Ref: job}}, "job", "StatefulSet/store", program.EffectUnknown)
	request := program.HTTPRequest{Pod: webPod, Container: "web", Method: "POST", URL: "http://store:8080/admin/flush"}
	requireRecordedDataEffect(t, program.EnsureHTTP{Observe: request, Ensure: request}, "http request", "StatefulSet/store", program.EffectUnknown)
}

// The class stops at data: opaque actions that reach no data component record
// no data effect.
func TestDataEffectsRecordNothingForStatelessPod(t *testing.T) {
	webPod := holdsDataRef("v1", "Pod", "web-1-a")
	command := boundValues(t, map[string]any{"word": "true"})["word"]
	job := holdsDataRef("batch/v1", "Job", "fixer")
	manifest := map[string]any{"apiVersion": "batch/v1", "kind": "Job", "metadata": map[string]any{"name": "fixer"}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "fix", "args": []any{"true"}}}}}}}
	request := program.HTTPRequest{Pod: webPod, Container: "web", Method: "POST", URL: "http://web:8080/admin/reload"}
	reach := dataReach{view: holdsDataView(), holders: holdsDataHolders(), namespace: "app"}
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
			if effects := reach.effects(action); len(effects.touches) != 0 || effects.unreadInput {
				t.Fatalf("effects = %+v, want none on a stateless Pod", effects)
			}
		})
	}
}

package replay

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type operationProgram struct {
	observe program.ExternalOperation
	action  program.Action
}

func (p operationProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "http-fixture"} }
func (p operationProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	code, err := v.Observe(p.observe)
	if err != nil {
		return program.Verdict{}, err
	}
	r := program.Match
	if code == 0 {
		r = program.NoMatch
	}
	return program.Verdict{Result: r}, nil
}
func (p operationProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{p.action}}, nil
}
func TestGeneralHTTPReplayRecordedTransition(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "admin"}
	observe := program.HTTPRequest{Pod: ref, Container: "app", Method: "GET", URL: "http://api.app.svc/ready"}
	ensure := program.HTTPRequest{Pod: ref, Container: "app", Method: "POST", URL: "http://api.app.svc/reload", Bearer: &program.CredentialRef{Env: "ADMIN_TOKEN"}}
	effect := program.ExternalEffect{Observe: program.ExternalOperation{HTTP: &observe}, Ensure: program.ExternalOperation{HTTP: &ensure}, Before: 22, After: 0}
	args := mustInventoryJSON(t, []any{effect.Observe, effect.Ensure})
	value := mustInventoryJSON(t, effect)
	s := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{inventoryObject(ref, map[string]any{"containers": []any{map[string]any{"name": "app", "image": "fixture"}}})}}
	p := operationProgram{observe: effect.Observe, action: program.EnsureHTTP{Observe: observe, Ensure: ensure}}
	c := Case{ID: "http", RepairOnly: true, Corpus: sdktest.CorpusCase{Open: s, Healthy: s, Fixed: s, RootCauseProgramIDs: []string{p.Spec().ID}, Recordings: []sdktest.PrimitiveCall{{Method: "ExternalEffect", Arguments: args, Result: value}}}}
	result := Check(context.Background(), p, p, nil, c)
	if result.Status != "replayed" {
		t.Fatalf("HTTP replay: %+v", result)
	}
	// A different endpoint cannot use the recorded reload effect.
	ensure.URL = "http://api.app.svc/other"
	p.action = program.EnsureHTTP{Observe: observe, Ensure: ensure}
	if bad := Check(context.Background(), p, p, nil, c); bad.Status == "replayed" {
		t.Fatal("invented HTTP endpoint effect")
	}
}
func TestGeneralJobReplayRecordedCompletion(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: "app", Name: "once"}
	object := inventoryObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"restartPolicy": "Never", "containers": []any{map[string]any{"name": "script", "image": "fixture", "command": []any{"sh", "-c", "fixture"}}}}}})
	job := program.RunJob{Object: program.ApplyObject{Ref: ref, Object: object.Object}, Completion: program.WaitFor{Ref: ref, Path: "/status/succeeded", Equals: int64(1), Timeout: time.Millisecond}}
	calls := []sdktest.PrimitiveCall{{Method: "JobCompletion", Arguments: mustInventoryJSON(t, []any{job}), Result: json.RawMessage(`{"succeeded":1}`)}}
	for _, recorded := range []bool{true, false} {
		t.Run(map[bool]string{true: "recorded", false: "missing"}[recorded], func(t *testing.T) {
			c, err := newCluster(sdktest.Snapshot{NamespaceName: "app"})
			if err != nil {
				t.Fatal(err)
			}
			evidence := calls
			if !recorded {
				evidence = nil
			}
			if err = c.seedExternal(evidence); err != nil {
				t.Fatal(err)
			}
			e := operator.PlanExecutor{Client: c, Rollouts: inventoryRollouts{}, Verifier: inventoryVerifier{}}
			result, err := e.Apply(context.Background(), operator.IncidentRequest{}, program.Plan{Actions: []program.Action{job}}, false)
			if !recorded {
				if err == nil || result.VerificationPassed {
					t.Fatal("invented job completion")
				}
				return
			}
			if err != nil || !result.VerificationPassed {
				t.Fatalf("job replay: %+v %v", result, err)
			}
			if err = checkWrites(context.Background(), c, result); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestGeneralAPIReplayRecordedEffect(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "drain-me"}
	object := inventoryObject(ref, map[string]any{"containers": []any{map[string]any{"name": "app", "image": "fixture"}}})
	request := program.KubernetesRequest{Ref: ref, Method: "POST", Path: "/api/v1/namespaces/app/pods/drain-me/eviction", Body: json.RawMessage(`{"apiVersion":"policy/v1","kind":"Eviction","metadata":{"name":"drain-me","namespace":"app"}}`), ContentType: "application/json"}
	effect := program.KubernetesEffect{Request: request, Before: object.Object, BeforeKnown: true, AfterKnown: true, Response: json.RawMessage(`{"kind":"Status","status":"Success"}`)}
	for _, recorded := range []bool{true, false} {
		t.Run(map[bool]string{true: "recorded", false: "missing"}[recorded], func(t *testing.T) {
			c, err := newCluster(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}})
			if err != nil {
				t.Fatal(err)
			}
			var calls []sdktest.PrimitiveCall
			if recorded {
				calls = []sdktest.PrimitiveCall{{Method: "KubernetesEffect", Arguments: mustInventoryJSON(t, []any{request}), Result: mustInventoryJSON(t, effect)}}
			}
			if err = c.seedExternal(calls); err != nil {
				t.Fatal(err)
			}
			e := operator.PlanExecutor{Client: c, Rollouts: inventoryRollouts{}, Verifier: inventoryVerifier{}}
			result, err := e.Apply(context.Background(), operator.IncidentRequest{}, program.Plan{Actions: []program.Action{request}}, false)
			if !recorded {
				if err == nil || result.VerificationPassed {
					t.Fatal("invented eviction")
				}
				return
			}
			if err != nil || !result.VerificationPassed {
				t.Fatalf("eviction replay: %+v %v", result, err)
			}
			if err = checkWrites(context.Background(), c, result); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func checkGeneralReadReplay(t *testing.T, op string) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}
	object := inventoryObject(ref, map[string]any{"containers": []any{map[string]any{"name": "app", "image": "fixture"}}})
	event := corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "observed", Namespace: "app"}, Message: "fixture event"}
	s := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{object}, EventList: []corev1.Event{event}}
	var calls []sdktest.PrimitiveCall
	query := program.KubernetesRequest{Method: "GET", Path: "/apis/metrics.k8s.io/v1beta1/namespaces/app/pods/api"}
	if op == "logs" {
		calls = []sdktest.PrimitiveCall{{Method: "Logs", Arguments: mustInventoryJSON(t, []any{ref, "app", time.Second}), Result: mustInventoryJSON(t, "fixture log")}}
	}
	if op == "top" {
		calls = []sdktest.PrimitiveCall{{Method: "KubernetesRead", Arguments: mustInventoryJSON(t, []any{query}), Result: json.RawMessage(`{"containers":[{"name":"app","usage":{"cpu":"1m"}}]}`)}}
	}
	view := newReadView(context.Background(), s, calls, &source{})
	switch op {
	case "get":
		if _, err := view.Object(ref); err != nil {
			t.Fatal(err)
		}
	case "describe":
		if _, err := view.Object(ref); err != nil {
			t.Fatal(err)
		}
		if len(view.Events()) != 1 {
			t.Fatal("missing events")
		}
	case "events":
		if len(view.Events()) != 1 || view.Events()[0].Message != "fixture event" {
			t.Fatal("event replay failed")
		}
	case "logs":
		if log, err := view.Logs(ref, "app", time.Second); err != nil || log != "fixture log" {
			t.Fatal("log replay failed")
		}
	case "top":
		if raw, err := view.KubernetesRead(query); err != nil || !json.Valid(raw) {
			t.Fatal("metrics API replay failed")
		}
	}
}

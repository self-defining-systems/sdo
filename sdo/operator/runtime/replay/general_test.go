package replay

import (
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type externalProgram struct{ action program.EnsureExec }

func (p externalProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "external-fixture"}
}
func (p externalProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	code, err := v.Observe(program.ExternalOperation{Exec: &p.action.Observe})
	if err != nil {
		return program.Verdict{}, err
	}
	r := program.Match
	if code == p.action.Expect {
		r = program.NoMatch
	}
	return program.Verdict{Result: r}, nil
}
func (p externalProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{p.action}}, nil
}
func TestGeneralExternalReplayRecordedTransition(t *testing.T) { testGeneralExternalReplay(t, "exec") }
func testGeneralExternalReplay(t *testing.T, op string) {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db"}
	observe := program.Command{Pod: pod, Container: "app", Args: []string{"test", "-f", "/tmp/ready"}}
	ensure := program.Command{Pod: pod, Container: "app", Args: []string{"touch", "/tmp/ready"}}
	switch op {
	case "cp":
		ensure.Args = []string{"tar", "xf", "-", "-C", "/tmp"}
		ensure.Stdin = "synthetic tar input"
	case "container-admin":
		ensure.Args = []string{"mongo", "--quiet", "--eval", "fixtureAdmin()"}
	case "container-reload":
		ensure.Args = []string{"kill", "-HUP", "1"}
	case "container-files":
		ensure.Args = []string{"sh", "-c", "cat > /tmp/ready"}
		ensure.Stdin = "fixture configuration"
	}
	p := externalProgram{action: program.EnsureExec{Observe: observe, Ensure: ensure}}
	effect := program.ExternalEffect{Observe: program.ExternalOperation{Exec: &observe}, Ensure: program.ExternalOperation{Exec: &ensure}, Before: 1, After: 0}
	args, _ := json.Marshal([]any{effect.Observe, effect.Ensure})
	value, _ := json.Marshal(effect)
	obsArgs, _ := json.Marshal([]any{effect.Observe})
	s := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": "db", "namespace": "app"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "fixture"}}}}}}}
	c := Case{ID: "external", RepairOnly: true, Corpus: sdktest.CorpusCase{Open: s, Healthy: s, Fixed: s, RootCauseProgramIDs: []string{p.Spec().ID}, Recordings: []sdktest.PrimitiveCall{{Method: "Observe", Arguments: obsArgs, Result: json.RawMessage("1")}, {Method: "ExternalEffect", Arguments: args, Result: value}}}}
	result := Check(context.Background(), p, p, nil, c)
	if result.Status != "replayed" {
		t.Fatalf("recorded effect was refused: %+v", result)
	}
	for _, mode := range []string{"missing", "different-command", "different-before", "wrong-after", "failed-command"} {
		t.Run(mode, func(t *testing.T) {
			bad := c
			bad.Corpus.Recordings = append([]sdktest.PrimitiveCall(nil), c.Corpus.Recordings...)
			e := effect
			switch mode {
			case "missing":
				bad.Corpus.Recordings = bad.Corpus.Recordings[:1]
			case "different-command":
				replacement := ensure
				replacement.Args = []string{"false"}
				e.Ensure = program.ExternalOperation{Exec: &replacement}
			case "different-before":
				e.Before = 2
			case "wrong-after":
				e.After = 1
			case "failed-command":
				e.ExitCode = 7
			}
			if mode != "missing" {
				raw, _ := json.Marshal(e)
				bad.Corpus.Recordings[1].Result = raw
			}
			r := Check(context.Background(), p, p, nil, bad)
			if r.Status == "replayed" {
				t.Fatalf("invented success from %s: %+v", mode, r)
			}
		})
	}
}
func TestGeneralReplayBindsCompleteObserveEnsurePair(t *testing.T) {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db"}
	a := program.Command{Pod: pod, Container: "app", Args: []string{"observe-a"}}
	b := a
	b.Args = []string{"observe-b"}
	ensure := a
	ensure.Args = []string{"ensure-shared"}
	c, err := newCluster(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{inventoryObject(pod, map[string]any{})}})
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	for _, observe := range []program.Command{a, b} {
		effect := program.ExternalEffect{Observe: program.ExternalOperation{Exec: &observe}, Ensure: program.ExternalOperation{Exec: &ensure}, Before: 1, After: 0}
		calls = append(calls, sdktest.PrimitiveCall{Method: "ExternalEffect", Arguments: mustInventoryJSON(t, []any{effect.Observe, effect.Ensure}), Result: mustInventoryJSON(t, effect)})
	}
	if err = c.seedExternal(calls); err != nil {
		t.Fatal(err)
	}
	e := operator.PlanExecutor{Client: c, Rollouts: inventoryRollouts{}, Verifier: inventoryVerifier{}}
	if _, err = e.Apply(context.Background(), operator.IncidentRequest{}, program.Plan{Actions: []program.Action{program.EnsureExec{Observe: b, Ensure: ensure}}}, false); err != nil {
		t.Fatalf("selected unrelated observe pair: %v", err)
	}
	if code, _ := c.RunOperation(context.Background(), program.ExternalOperation{Exec: &a}); code != 1 {
		t.Fatal("changed unrelated observation")
	}
}
func TestGeneralAPIReplayRequiresExactQueryEvidence(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}
	c, err := newCluster(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{inventoryObject(ref, map[string]any{})}})
	if err != nil {
		t.Fatal(err)
	}
	q := program.KubernetesRequest{Ref: ref, Method: "GET", Path: "/api/v1/namespaces/app/pods/api", Query: map[string]string{"resourceVersion": "not-a-number"}}
	if _, err = c.KubernetesRequest(context.Background(), q); err == nil {
		t.Fatal("ignored unsupported API query")
	}
}
func TestGeneralAPIReplayHonorsExplicitDeleteIdentity(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}
	original := inventoryObject(ref, map[string]any{})
	original.SetUID("original")
	original.SetResourceVersion("3")
	replacement := original.DeepCopy()
	replacement.SetUID("replacement")
	replacement.SetResourceVersion("9")
	request := program.KubernetesRequest{Ref: ref, Method: "DELETE", Path: "/api/v1/namespaces/app/pods/api", Body: []byte(`{"preconditions":{"uid":"original","resourceVersion":"3"}}`)}
	effect := program.KubernetesEffect{Request: request, Before: original.Object, BeforeKnown: true, AfterKnown: true, Response: json.RawMessage(`{}`)}
	for _, current := range []*unstructured.Unstructured{original.DeepCopy(), replacement} {
		c, err := newCluster(sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*current}})
		if err != nil {
			t.Fatal(err)
		}
		if err = c.seedExternal([]sdktest.PrimitiveCall{{Method: "KubernetesEffect", Arguments: mustInventoryJSON(t, []any{request}), Result: mustInventoryJSON(t, effect)}}); err != nil {
			t.Fatal(err)
		}
		_, err = c.KubernetesRequest(context.Background(), request)
		if current.GetUID() == "replacement" && err == nil {
			t.Fatal("replayed stale conditional delete on a replacement Pod")
		}
		if current.GetUID() == "original" && err != nil {
			t.Fatalf("rejected exact conditional delete: %v", err)
		}
	}
}

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type externalTestClient struct {
	*lifecycleTestClient
	code        int32
	ensureCalls int
	fail        bool
}

func (c *externalTestClient) RunOperation(_ context.Context, o program.ExternalOperation) (int32, error) {
	if o.Exec.Args[0] == "observe" {
		return c.code, nil
	}
	c.ensureCalls++
	if c.fail {
		return 0, errors.New("connection lost after dispatch")
	}
	c.code = 0
	return 0, nil
}
func TestGeneralExternalProductionRecordsMeasuredEffect(t *testing.T) {
	pod := databaseAction().Target.Pod
	observe := program.Command{Pod: pod, Container: "db", Args: []string{"observe"}}
	ensure := program.Command{Pod: pod, Container: "db", Args: []string{"ensure"}, Stdin: "fixture-script"}
	object := planObject(pod, map[string]any{})
	object.SetUID("pod")
	object.SetResourceVersion("1")
	c := &externalTestClient{lifecycleTestClient: &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{pod: object}}}, code: 1}
	var calls []sdktest.PrimitiveCall
	e := PlanExecutor{Client: c, Rollouts: generalNoRollouts{}, Verifier: generalHealthy{}, RecordPrimitive: func(call sdktest.PrimitiveCall) error { calls = append(calls, call); return nil }}
	plan := program.Plan{Actions: []program.Action{program.EnsureExec{Observe: observe, Ensure: ensure}}}
	r, err := e.Apply(context.Background(), IncidentRequest{}, plan, false)
	if err != nil || !r.VerificationPassed || len(calls) != 1 || c.ensureCalls != 1 {
		t.Fatalf("%+v %v calls=%+v", r, err, calls)
	}
	var effect program.ExternalEffect
	if json.Unmarshal(calls[0].Result, &effect) != nil || effect.Before != 1 || effect.After != 0 || effect.Ensure.Exec.Stdin != "fixture-script" {
		t.Fatal("effect not measured")
	}
	if _, err = e.Apply(context.Background(), IncidentRequest{}, plan, false); err != nil || c.ensureCalls != 1 {
		t.Fatal("ensure was not idempotent")
	}
	c.code = 1
	c.fail = true
	r, err = e.Apply(context.Background(), IncidentRequest{}, plan, false)
	if err == nil || !r.Applied || r.ActionReports[0].Status != "unknown" || len(calls) != 1 {
		t.Fatalf("lost dispatch became success: %+v %v", r, err)
	}
}

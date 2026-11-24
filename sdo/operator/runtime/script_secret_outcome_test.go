package runtime

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/outcome_derivation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type returningScriptClient struct {
	TypedPlanClient
	result ScriptExecutionResult
}

func (c returningScriptClient) RunScript(context.Context, ScriptExecutionRequest) (ScriptExecutionResult, error) {
	return c.result, nil
}

func TestScriptSecretCanaryAbsentFromReceiptsAndOutcome(t *testing.T) {
	const canary = "synthetic-secret-canary-13291"
	const incident = "synthetic-incident"
	ctx := t.Context()
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credentials"}
	exit := int32(0)
	script := program.RunScript{Script: "true", Reason: "repair synthetic fault"}
	result := ScriptExecutionResult{
		ApplicationID: "synthetic-app", JobName: "synthetic-job", JobUID: "synthetic-uid",
		ExitCode: &exit, DurationMS: 12, Receipts: []ScriptAPIReceipt{{Method: "patch", Status: 200, Written: []sdk.ObjectRef{ref}}}, ScriptDigest: program.ScriptDigest(script.Script),
		Objects: []ScriptObjectCapture{{Ref: ref, Before: &unstructured.Unstructured{
			Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "data": map[string]any{"key": canary}},
		}}},
	}
	kube := fake.NewSimpleClientset()
	client := returningScriptClient{TypedPlanClient: TypedPlanClient{Client: kube, ScriptImage: "synthetic"}, result: result}
	var capture []byte
	executor := PlanExecutor{Client: client, RecordPrimitive: func(call sdktest.PrimitiveCall) error {
		capture = call.Result
		return nil
	}}
	applied, err := executor.scriptStep(ctx, IncidentRequest{IncidentID: incident, Namespace: "app"}, script, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(capture) == 0 || bytes.Contains(capture, []byte(canary)) {
		t.Fatal("script receipt missing or retained canary")
	}
	at := time.Unix(1_000, 0).UTC()
	applications := []RecordedPlanApplication{{
		ID: "synthetic-app", ProgramIDs: []string{"script-fix"}, StartedAt: at, CompletedAt: at,
		Result: applied,
	}}
	if len(applied.ObjectEffects) != 1 || applied.ObjectEffects[0].Operation != "patch" || len(touchedObjects(applied)) != 1 || touchedObjects(applied)[0].GetName() != ref.Name {
		t.Fatal("API write receipt did not reach stage object effects")
	}
	stageReceipt, err := recordedPlanContract(applications[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.Validate(stageReceipt); err != nil {
		t.Fatalf("stage receipt cannot persist: %v", err)
	}
	loop := ResponderLoop{applicationsLog: applications}
	found := false
	for _, change := range loop.incidentWrites() {
		found = found || change.ReaderOrObject == "Secret/credentials" && change.After == "script ran in 12 ms; exit 0"
	}
	if !found {
		t.Fatal("responder cannot see script exit and API write")
	}
	receipts := ProgramRepairReceipts(applications, nil)
	projection, err := incidentResultToProto(IncidentResult{IncidentID: incident, RepairActions: receipts, PlanApplications: applications})
	if err != nil {
		t.Fatal(err)
	}
	if len(projection.GetRepairActions()) < 2 {
		t.Fatalf("expected program and script receipts, got %d", len(projection.GetRepairActions()))
	}
	out, err := outcome_derivation.Derive(ctx, &contracts.IncidentClosure{
		Request: &contracts.IncidentRequest{IncidentId: incident}, Result: projection,
		DetectedAt: timestamppb.New(at), DispatchedAt: timestamppb.New(at), ResponderCompletedAt: timestamppb.New(at),
	}, "", "synthetic", "synthetic", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Record.GetRepairActions()) != len(receipts) {
		t.Fatal("outcome dropped repair receipts")
	}
	for _, data := range []proto.Message{projection, out.Record} {
		raw, err := protojson.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(canary)) {
			t.Fatal("repair receipt or outcome retained canary")
		}
	}
}

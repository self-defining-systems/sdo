package runtime

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

func TestRecoverySupersessionUsesWriteEvidenceNotNarrative(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "broken"}
	start := time.Unix(10, 0)
	action := program.DeleteObject{Ref: ref}
	app := RecordedPlanApplication{ID: "automatic", StartedAt: start, CompletedAt: start.Add(time.Second), Result: PlanApplicationResult{Applied: true, Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}}}
	loop := &ResponderLoop{RecoverySessionRoot: "/synthetic"}
	result := IncidentResult{ResponderSessionID: "synthetic-session", RepairChanges: []string{"The automatic stage removed the broken webhook", "Restored an independent ConfigMap"}}
	if loop.recoverySuperseded(context.Background(), app, result, []RecordedPlanApplication{app}, nil, true) {
		t.Fatal("narrative summaries superseded an independently recorded application")
	}
	withoutSession := result
	withoutSession.ResponderSessionID = ""
	if !loop.recoverySuperseded(context.Background(), app, withoutSession, []RecordedPlanApplication{app}, nil, true) {
		t.Fatal("unscoped repair narrative without responder history granted attribution")
	}
	withoutHistory := &ResponderLoop{}
	if !withoutHistory.recoverySuperseded(context.Background(), app, result, []RecordedPlanApplication{app}, nil, true) {
		t.Fatal("repair narrative without a history source granted attribution")
	}
	for _, tc := range []struct {
		name string
		ref  sdk.ObjectRef
		want bool
	}{{"overlap", ref, true}, {"independent", sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "config"}, false}, {"opaque", sdk.ObjectRef{Kind: "*"}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			writes := []RepairActionReceipt{{Resources: []sdk.ObjectRef{tc.ref}, StartedAt: start.Add(2 * time.Second), CompletedAt: start.Add(3 * time.Second)}}
			if got := loop.recoverySuperseded(context.Background(), app, result, []RecordedPlanApplication{app}, writes, true); got != tc.want {
				t.Fatalf("superseded = %v, want %v", got, tc.want)
			}
		})
	}
}

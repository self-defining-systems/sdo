package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sdo.dev/operator/sdk/sdktest"
)

func TestEvidenceGapsInspectEveryCallAndBothProtectedOpenBoundaries(t *testing.T) {
	resource := repeatService
	args, _ := json.Marshal([]any{objectRefState(resource)})
	for _, name := range []string{"open@detect", "open@pre-apply"} {
		t.Run(name, func(t *testing.T) {
			capture := t.TempDir()
			calls := make([]sdktest.PrimitiveCall, 27)
			calls = append(calls, sdktest.PrimitiveCall{Method: "DiffAgainstSource", Arguments: args, Error: "recorded failure"}, sdktest.PrimitiveCall{Phase: "fixed", Method: "DiffAgainstSource", Arguments: args})
			raw, _ := json.Marshal(calls)
			if err := os.WriteFile(filepath.Join(capture, name+"-recordings.json"), raw, 0644); err != nil {
				t.Fatal(err)
			}
			work := []*contracts.ReflectionCauseWork{{Resources: []*contracts.ObjectRef{resource}}}
			if count := appendEvidenceGaps(work, capture); count != 0 || len(work[0].Items) != 0 {
				t.Fatalf("late observation became a gap: %v", work)
			}
		})
	}
}

func TestEvidenceGapsNameOnlyTheMissingResourceAndPhase(t *testing.T) {
	capture := t.TempDir()
	args, _ := json.Marshal([]any{objectRefState(repeatService)})
	raw, _ := json.Marshal([]sdktest.PrimitiveCall{{Method: "Logs", Arguments: args}})
	if err := os.WriteFile(filepath.Join(capture, "open@detect-recordings.json"), raw, 0644); err != nil {
		t.Fatal(err)
	}
	work := []*contracts.ReflectionCauseWork{{CauseIndex: 2, Resources: []*contracts.ObjectRef{repeatService}}}
	if count := appendEvidenceGaps(work, capture); count != 1 {
		t.Fatalf("gap count=%d", count)
	}
	item := work[0].Items[0]
	if item.Reason != "evidence_gap" || item.EvidencePhase != "fixed" || item.Resources[0].GetName() != repeatService.GetName() || work[0].CauseIndex != 2 {
		t.Fatalf("gap=%v", work)
	}
}

func TestEvidenceGapDeclineNeedsGoFlag(t *testing.T) {
	decision := &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "evidence gap", DeclineReason: "evidence_gap"}
	if len(lifecyclecore.EvidenceGapDiagnostics(decision, 0)) != 1 || len(lifecyclecore.EvidenceGapDiagnostics(decision, 1)) != 0 {
		t.Fatal("evidence decline escaped Go flags")
	}
}

func TestUnflaggedGapDeclineGetsTypedBrokerRetryFeedback(t *testing.T) {
	driver := lifecycleio.ReflectionDriver{Repository: &broker.Broker{}, Now: func() time.Time { return time.Unix(1, 0) }}
	effect := lifecyclecore.ReflectionEffect{Kind: lifecyclecore.ReviewReflection, ID: "review", Decision: &contracts.ReflectionDecision{
		LearningDecision: "no_change", NoChangeReason: "evidence gap", DeclineReason: "evidence_gap",
	}}
	rejected, err := driver.Execute(context.Background(), effect)
	if err != nil || rejected.Kind != lifecyclecore.ReflectionRejected || len(rejected.Diagnostics) != 1 || rejected.Diagnostics[0].Rule != "reflection.evidence_gap" {
		t.Fatalf("unflagged absence escaped review: %v %v", rejected, err)
	}
	effect.EvidenceGapCount = 1
	accepted, err := driver.Execute(context.Background(), effect)
	if err != nil || accepted.Kind != lifecyclecore.ReflectionReviewed {
		t.Fatalf("flagged gap rejected: %v %v", accepted, err)
	}
}

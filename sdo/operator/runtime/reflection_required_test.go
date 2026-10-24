package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/runtime/lifecyclecore"
)

func TestReadmeOnlyVerifiedReflectionIsRejectedAndRetried(t *testing.T) {
	root := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(root, ".sdo"))
	state := &contracts.BrokerLedger{IncidentId: "first", OutcomeCommit: "outcome", ReflectionPhase: contracts.ReflectionPhase_REFLECTION_PHASE_REVIEW, ReflectionMaxRetries: 2, ReflectionWorkspace: &contracts.ProposalWorkspace{CheckoutPath: root}, ReflectionDecision: &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}, ReflectionPaths: []string{".sdo/playbooks/base/README.md"}, OutcomeDocument: &contracts.OutcomeRecord{IncidentId: "first", Classification: "success", ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Resources: []*contracts.ObjectRef{t1Ref("api")}}}}}
	_, effects := lifecyclecore.StepReflection(state, lifecyclecore.ReflectionEvent{Kind: lifecyclecore.ReflectionDrive})
	driver := lifecycleio.ReflectionDriver{Repository: &broker.Broker{}, Now: func() time.Time { return time.Unix(1, 0) }}
	event, err := driver.Execute(context.Background(), effects[0])
	if err != nil || event.Kind != lifecyclecore.ReflectionRejected || len(event.Diagnostics) != 1 || event.Diagnostics[0].Rule != "reflection.program-required" {
		t.Fatalf("README-only repair was not rejected: %+v %v", event, err)
	}
	next, _ := lifecyclecore.StepReflection(state, event)
	attempt, diagnostics := lifecyclecore.ReflectionFeedback(next)
	if next.ReflectionPhase != contracts.ReflectionPhase_REFLECTION_PHASE_RESET || attempt != 2 || len(diagnostics) != 1 || diagnostics[0].Rule != "reflection.program-required" {
		t.Fatalf("missing typed retry diagnostics: %v", next)
	}
}

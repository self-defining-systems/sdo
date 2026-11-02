package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

type peerBlockedRollout struct {
	entered    chan struct{}
	peerBroken func() bool
}

func (r peerBlockedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	for _, ref := range refs {
		if ref.Kind == "Deployment" && (r.peerBroken == nil || r.peerBroken()) {
			select {
			case r.entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
	}
	return nil
}

type rolloutFunc func(context.Context, []sdk.ObjectRef) error

func (f rolloutFunc) Wait(ctx context.Context, refs []sdk.ObjectRef) error { return f(ctx, refs) }

func TestPendingRolloutToolRecordIsBoundedAndNotVerified(t *testing.T) {
	p := &contracts.RolloutPending{Reason: strings.Repeat("blocked ", 2000), Blockers: []*contracts.RolloutBlocker{{Ref: planRefProto(menuDeployment), Reason: strings.Repeat("TLS failed ", 1000), WarningEvents: []string{strings.Repeat("FailedCreate ", 1000)}}}}
	record := recordedToolResponseJSON(ToolResponse{Command: "mitigate", Summary: strings.Repeat("rollout_pending ", 1000), Error: strings.Repeat("pending ", 1000), RolloutPending: p}, nil)
	if len(record) > maxRecordedToolResponse {
		t.Fatalf("pending evidence overflowed tool bound: %d", len(record))
	}
	var decoded struct {
		Truncated bool                      `json:"truncated"`
		Healthy   bool                      `json:"healthy"`
		Pending   *contracts.RolloutPending `json:"rollout_pending"`
	}
	if err := json.Unmarshal(record, &decoded); err != nil || !decoded.Truncated || decoded.Healthy || decoded.Pending == nil {
		t.Fatalf("pending record lost classification: %s %v", record, err)
	}
	if len(p.Reason) < 1000 {
		t.Fatal("bounded record mutated full native evidence")
	}
}

// A pending rollout has no independent repair verdict. It must not erase a
// previously verified revision's trust, nor mask a separate false match.
func TestPendingRolloutDoesNotCountAsDurableTrustFailure(t *testing.T) {
	for _, falseMatch := range []bool{false, true} {
		t.Run(fmt.Sprint(falseMatch), func(t *testing.T) {
			prior := &contracts.OutcomeRecord{Classification: "success", SourceCommit: "revision-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "repair", ProgramRevision: "revision-a"}}, ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Unassigned: []*contracts.ReflectionWorkItem{{Disposition: "covered", ProgramId: "repair", ProgramRevision: "revision-a"}}}}
			pending := &contracts.OutcomeRecord{Classification: "failed", SourceCommit: "revision-a", ProgramRuns: []*contracts.ProgramRun{{ProgramId: "repair", ProgramRevision: "revision-a"}}, PlanApplications: []*contracts.PlanApplication{{ApplicationId: "application-pending", Applied: true, ProgramIds: []string{"repair"}, Error: "rollout_pending", RolloutPending: &contracts.RolloutPending{Reason: "peer admission blocks rollout"}}}}
			if falseMatch {
				pending.ReflectionEligibility = &contracts.ReflectionEligibility{Version: 1, Unassigned: []*contracts.ReflectionWorkItem{{Disposition: "false_match", ProgramId: "repair", ProgramRevision: "revision-a"}}}
			}
			state := ProgramTrustStats([]*contracts.OutcomeRecord{prior, pending})["repair"]
			if falseMatch {
				if state.Failures != 1 || (state.VerifiedSuccesses > 0 && state.Failures == 0) {
					t.Fatalf("pending masked independent false match: %+v", state)
				}
			} else if state.Failures != 0 || state.VerifiedSuccesses != 1 || !(state.VerifiedSuccesses > 0 && state.Failures == 0) {
				t.Fatalf("pending alone changed durable trust: %+v", state)
			}
		})
	}
}


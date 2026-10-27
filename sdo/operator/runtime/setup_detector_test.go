package runtime

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/outcome_derivation"
	"testing"
	"time"

	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
)

func TestSetupLearnedDetectorQuarantine(t *testing.T) {
	for _, unhealthy := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "health-firing"}[unhealthy], func(t *testing.T) {
			health := inventoryHealth().(*sequenceDetector)
			if unhealthy {
				health.samples = [][]sdk.Finding{{stateFinding("health")}}
			}
			learned := controllerDetector("learned", time.Second, stateFinding("learned"))
			learned.spec.Class, learned.spec.Owner = sdk.DetectorClassIncident, sdk.DetectorOwnerResponder
			learned.spec.OriginatingIncident, learned.spec.OriginatingCommit = "prior-incident", "prior-commit"
			learned.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
			learned.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
			c, client := stagedInstallerController(t, health, learned)
			ctx := context.Background()
			if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
				t.Fatal(err)
			}
			record, err := onboarding.Load(ctx, client, "control", "app")
			if err != nil {
				t.Fatal(err)
			}
			if unhealthy {
				if record != nil {
					t.Fatal("firing health detector granted onboarding")
				}
				return
			}
			if record == nil {
				t.Fatal("learned false positive blocked healthy onboarding")
			}
			if c.incidentOpen || len(c.batcher.Snapshot().Findings) != 0 {
				t.Fatal("learned false positive created incident work")
			}
			state := exportState(t, c)
			if len(state.GetDisabledSetupDetectors()) != 1 {
				t.Fatal("missing durable setup reason")
			}
			disabled := state.DisabledSetupDetectors[0]
			if disabled.DetectorId != "learned" || disabled.AcceptedRevision != c.config.SourceCommit || len(disabled.Fingerprints) != 1 || disabled.DisabledAt == nil || disabled.Reason != "fired on the confirmed-healthy app at setup" {
				t.Fatalf("incomplete reason: %v", disabled)
			}
			if len(record.GetDisabledSetupDetectors()) != 1 {
				t.Fatal("receipt lost disabled detector")
			}
			// Feed the real controller request into the real outcome producer, which
			// is the reflection request's authoritative evidence source.
			request, err := incidentRequestContract(c.incidentRequest(time.Unix(102, 0), []sdk.Finding{stateFinding("later")}))
			if err != nil {
				t.Fatal(err)
			}
			at := timestamppb.New(time.Unix(102, 0))
			derived, err := outcome_derivation.Derive(ctx, &contracts.IncidentClosure{Request: request, DetectedAt: at, DispatchedAt: at, ResponderCompletedAt: at}, "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if len(derived.Record.DisabledSetupDetectors) != 1 || !proto.Equal(derived.Record.DisabledSetupDetectors[0], disabled) {
				t.Fatal("outcome lost setup audit evidence")
			}
			calls := learned.calls
			// The create-once receipt also survives a crash before the driver saves
			// the next controller-state checkpoint.
			receiptOnly, err := NewController(c.config, []sdk.Detector{health, learned}, c.provider, &recordingDispatcher{requests: make(chan IncidentRequest, 10)}, time.Unix(102, 0))
			if err != nil {
				t.Fatal(err)
			}
			if err := receiptOnly.AttachPendingOnboarding(ctx, client, "control"); err != nil {
				t.Fatal(err)
			}
			if err := receiptOnly.EvaluateAll(ctx, time.Unix(103, 0)); err != nil {
				t.Fatal(err)
			}
			if learned.calls != calls {
				t.Fatal("receipt-only restart evaluated disabled detector")
			}
			restarted, err := NewController(c.config, []sdk.Detector{health, learned}, c.provider, &recordingDispatcher{requests: make(chan IncidentRequest, 10)}, time.Unix(102, 0))
			if err != nil {
				t.Fatal(err)
			}
			if err := restarted.RestoreState(state); err != nil {
				t.Fatal(err)
			}
			if err := restarted.EvaluateAll(ctx, time.Unix(103, 0)); err != nil {
				t.Fatal(err)
			}
			if learned.calls != calls {
				t.Fatal("restart re-evaluated disabled detector")
			}
			restarted.config.SourceCommit = "new-memory-revision"
			// Restore a realistic skewed schedule: the learned detector is due before
			// health. Revision adoption must still take one fresh complete setup sweep.
			if err := restarted.scheduler.RestoreDeadlines(map[string]time.Time{"health": time.Unix(1000, 0), "learned": time.Unix(104, 0)}); err != nil {
				t.Fatal(err)
			}
			if err := restarted.Step(ctx, time.Unix(104, 0), nil); err != nil {
				t.Fatal(err)
			}
			if learned.calls == calls {
				t.Fatal("new revision did not re-enable detector")
			}
			if len(exportState(t, restarted).DisabledSetupDetectors) != 2 {
				t.Fatal("new revision failed to take a confirmed-health setup sweep")
			}
		})
	}
}

// Sweep health failure modes and registry orders: suppression is authority
// derived from a complete health sample, never from incident-detector order.
func TestSetupDetectorHealthAuthority(t *testing.T) {
	for _, mode := range []string{"clear", "firing", "error", "already-open", "not-learned", "later-finding", "resolved"} {
		for _, learnedFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/learned-first=%t", mode, learnedFirst), func(t *testing.T) {
				health := inventoryHealth().(*sequenceDetector)
				id := "z-learned"
				if learnedFirst {
					id = "a-learned"
				}
				learned := controllerDetector(id, time.Second, stateFinding(id))
				learned.spec.Class, learned.spec.Owner = sdk.DetectorClassIncident, sdk.DetectorOwnerResponder
				learned.spec.OriginatingIncident, learned.spec.OriginatingCommit = "prior-incident", "prior-commit"
				learned.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
				learned.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
				if mode == "firing" {
					health.samples = [][]sdk.Finding{{stateFinding("health")}}
				}
				if mode == "error" {
					health.errors = map[int]error{0: errors.New("health read unavailable")}
				}
				if mode == "not-learned" {
					learned = controllerDetector("legacy", time.Second, stateFinding("legacy"))
				}
				if mode == "resolved" {
					learned.samples[0][0].Status = sdk.FindingResolved
				}
				if mode == "later-finding" {
					learned.samples = [][]sdk.Finding{{}, {stateFinding(id)}}
				}
				c, _ := stagedInstallerController(t, health, learned)
				if mode == "already-open" {
					c.incidentDetectedAt = time.Unix(99, 0)
				}
				if err := c.EvaluateAll(context.Background(), time.Unix(101, 0)); err != nil {
					t.Fatal(err)
				}
				disabled := len(exportState(t, c).DisabledSetupDetectors)
				if mode == "clear" {
					if disabled != 1 {
						t.Fatal("healthy learned detector not disabled")
					}
				} else if disabled != 0 {
					t.Fatalf("%s disabled a detector without setup authority", mode)
				}
				if mode == "later-finding" {
					if err := c.EvaluateAll(context.Background(), time.Unix(102, 0)); err != nil {
						t.Fatal(err)
					}
					if len(exportState(t, c).DisabledSetupDetectors) != 0 {
						t.Fatal("steady-state finding disabled as setup")
					}
				}
			})
		}
	}
}

func TestSetupExclusionReachesOutcomeWhenOtherFindingsBlockOnboarding(t *testing.T) {
	learned := controllerDetector("learned", time.Second, stateFinding("learned"))
	learned.spec.Class, learned.spec.Owner = sdk.DetectorClassIncident, sdk.DetectorOwnerResponder
	learned.spec.OriginatingIncident, learned.spec.OriginatingCommit = "prior-incident", "prior-commit"
	learned.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	learned.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	legacy := controllerDetector("legacy", time.Second, stateFinding("legacy"))
	c, client := stagedInstallerController(t, inventoryHealth(), learned, legacy)
	ctx := context.Background()
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(ctx, client, "control", "app")
	if err != nil || record != nil {
		t.Fatalf("other findings must block onboarding: %v %v", record, err)
	}
	if len(exportState(t, c).DisabledSetupDetectors) != 1 {
		t.Fatal("missing setup exclusion")
	}
	request, err := incidentRequestContract(c.incidentRequest(time.Unix(102, 0), []sdk.Finding{stateFinding("legacy")}))
	if err != nil {
		t.Fatal(err)
	}
	at := timestamppb.New(time.Unix(102, 0))
	derived, err := outcome_derivation.Derive(ctx, &contracts.IncidentClosure{Request: request, DetectedAt: at, DispatchedAt: at, ResponderCompletedAt: at}, "", "synthetic", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if len(derived.Record.DisabledSetupDetectors) != 1 {
		t.Fatal("outcome lost setup exclusion when onboarding failed")
	}
	calls := learned.calls
	roundtrip := incidentRequestState(request)
	loop := NewResponderLoop(*roundtrip, nil, c.provider, []sdk.Detector{inventoryHealth(), learned, legacy}, PlanExecutor{}, 3)
	response := loop.Handle(ctx, ToolRequest{Command: "detect"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	if learned.calls != calls {
		t.Fatal("responder self-check evaluated a setup-disabled detector")
	}
}

func TestSetupLearnedDetectorWaitsForConfirmedHealth(t *testing.T) {
	confirmed := false
	health := installWindowDetector{Detector: inventoryHealth(), confirmed: &confirmed}
	learned := controllerDetector("learned", time.Second, stateFinding("learned"))
	learned.spec.Class, learned.spec.Owner = sdk.DetectorClassIncident, sdk.DetectorOwnerResponder
	learned.spec.OriginatingIncident, learned.spec.OriginatingCommit = "prior-incident", "prior-commit"
	learned.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	learned.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	c, client := stagedInstallerController(t, health, learned)
	ctx := context.Background()
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if learned.calls != 0 || len(exportState(t, c).DisabledSetupDetectors) != 0 || c.incidentOpen || len(c.batcher.Snapshot().Findings) != 0 {
		t.Fatal("unconfirmed health consumed learned detector setup opportunity")
	}
	confirmed = true
	if err := c.EvaluateAll(ctx, time.Unix(102, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(ctx, client, "control", "app")
	if err != nil || record == nil || len(record.DisabledSetupDetectors) != 1 {
		t.Fatalf("confirmed health did not complete setup: %v %v", record, err)
	}
	// Even a stale exclusion with this ID can never switch off a judge-owned
	// health detector when accepted detector ownership changes.
	replacement := inventoryHealth().(*sequenceDetector)
	replacement.spec.ID = learned.spec.ID
	replacement.samples = [][]sdk.Finding{{stateFinding(learned.spec.ID)}}
	restarted, err := NewController(c.config, []sdk.Detector{inventoryHealth(), replacement}, c.provider, &recordingDispatcher{requests: make(chan IncidentRequest, 10)}, time.Unix(103, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.RestoreState(exportState(t, c)); err != nil {
		t.Fatal(err)
	}
	if err := restarted.EvaluateAll(ctx, time.Unix(104, 0)); err != nil {
		t.Fatal(err)
	}
	if replacement.calls == 0 {
		t.Fatal("stale learned exclusion switched off health detector")
	}
}

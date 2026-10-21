package runtime

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestOrdinaryDriftAdmissionBudgetsBatchingAndNewKeyPriority(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, DispatchesPerHour: 1, TokensPerHour: 10})
	findings := []sdk.Finding{}
	for i := 0; i < 10; i++ {
		f := unexplainedFinding()
		f.Summary = "unexplained drift"
		f.Evidence = "source comparison changed"
		event := driftEventsForFindings(nil, []sdk.Finding{f})[0]
		event.Ref.Name = string(rune('a' + i))
		f.PrimaryResource = event.Ref
		f.Fingerprint = ""
		f.Metadata = map[string]any{"drift_event": event}
		findings = append(findings, f)
	}
	first := c.admitDriftBatch(start, findings)
	if len(first) != 8 || len(c.batcher.Snapshot().Findings) != 2 {
		t.Fatal("ordinary drift batch did not retain overflow beyond eight events")
	}
	// A previously seen key waits; new keys bypass the spent retry budget.
	if batch := c.admitDriftBatch(start.Add(time.Second), []sdk.Finding{findings[0]}); len(batch) != 0 {
		t.Fatal("repeat bypassed dispatch budget")
	}
	if batch := c.admitDriftBatch(start.Add(time.Second), []sdk.Finding{findings[9]}); len(batch) != 1 {
		t.Fatal("new key waited for repeat budget")
	}
	health := stateFinding("health")
	if batch := c.admitDriftBatch(start.Add(2*time.Second), []sdk.Finding{health, findings[0]}); len(batch) != 2 {
		t.Fatal("drift budget delayed health")
	}
	c.recordEvaluation("health", start, nil)
	request := c.incidentRequest(start, first)
	request.IncidentID = "ordinary-drift"
	c.currentIncidentRequest = &request
	c.incidentDispatchedAt = start
	result := completedResult("ordinary-drift")
	result.Usage.InputTokens = 8
	result.Usage.OutputTokens = 2
	c.handleDispatchCompletion(dispatchCompletion{incidentID: result.IncidentID, result: result}, start.Add(3*time.Second))
	c.handleDispatchCompletion(dispatchCompletion{incidentID: result.IncidentID, result: result}, start.Add(4*time.Second))
	if len(c.driftAdmission.TokenUsage) != 1 || c.driftAdmission.TokenUsage[0].Tokens != 10 {
		t.Fatal("merged responder usage was charged more than once")
	}
	payload, err := encodeControllerState(c.ExportState())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := decodeControllerState(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.DriftAdmission.GetDispatchAt()) != 2 || restored.DriftAdmission.GetTokenUsage()[0].Tokens != 10 {
		t.Fatal("ordinary drift admission lost restart accounting")
	}
	if batch := c.admitDriftBatch(start.Add(time.Hour+5*time.Second), []sdk.Finding{findings[0]}); len(batch) != 1 {
		t.Fatal("repeat budget never expired")
	}
}

package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func TestExternalIncidentHasNoDetectorAuthorityAndClosesOnResponderDone(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	cfg := testControllerConfig()
	cfg.ExternalIncidentsOnly = true
	c, err := NewController(cfg, []sdk.Detector{externalHealthDetector()}, externalUnavailableSnapshots{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Step(context.Background(), now, nil); err != nil {
		t.Fatal("external idle tried to observe health", err)
	}
	alert := ExternalIncident{ID: "external-1", Namespace: cfg.Namespace, Message: "Something is wrong. Find and fix it."}
	if err := c.OpenExternalIncident(context.Background(), now, alert); err != nil {
		t.Fatal(err)
	}
	if err := c.OpenExternalIncident(context.Background(), now, alert); err != nil {
		t.Fatal("retry", err)
	}
	request := c.currentIncidentRequest
	if request.ExternalMessage != alert.Message || len(request.Findings) != 0 || len(request.DetectorHistory) != 0 || len(c.detectors) != 0 || request.StateChanges != nil {
		t.Fatalf("external request contains detector authority: %+v", request)
	}
	if err := validateIncidentRequest(*request); err != nil {
		t.Fatal(err)
	}
	c.maybeCloseIncident(context.Background(), now, nil)
	if c.pendingClosure != nil {
		t.Fatal("closed before responder returned")
	}
	result := completedResult(request.IncidentID)
	c.currentIncidentResult = &result
	c.responderDone = true
	c.responderCompletedAt = now.Add(time.Second)
	c.maybeCloseIncident(context.Background(), now.Add(time.Second), nil)
	if c.pendingClosure == nil || len(c.pendingClosure.FinalDetectorStates) != 0 || c.pendingClosure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_EXTERNAL_COMPLETION {
		t.Fatalf("missing responder-completion closure: %+v", c.pendingClosure)
	}
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewController(cfg, nil, externalUnavailableSnapshots{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if restored.pendingClosure == nil || restored.pendingClosure.Request.ExternalMessage != alert.Message {
		t.Fatal("external identity lost on restart")
	}
}

func TestExternalIncidentContractRejectsFabricatedDetectorEvidence(t *testing.T) {
	request := externalGoldenRequest(t)
	request.Findings = nil
	request.DetectorHistory = nil
	if err := validateIncidentRequest(request); err == nil {
		t.Fatal("detected incident accepted without trigger evidence")
	}
	request.ExternalMessage = "Investigate the namespace"
	if err := validateIncidentRequest(request); err != nil {
		t.Fatal(err)
	}
	request.Findings = externalGoldenRequest(t).Findings
	if err := validateIncidentRequest(request); err == nil {
		t.Fatal("external alert accepted detector evidence")
	}
}

func TestExternalToolsCannotExecuteDetectorsOrClaimHealthy(t *testing.T) {
	request := externalGoldenRequest(t)
	request.ExternalMessage = "Investigate"
	request.Findings = nil
	request.DetectorHistory = nil
	loop := NewResponderLoop(request, nil, staticProvider{}, []sdk.Detector{externalHealthDetector()}, PlanExecutor{}, 2)
	for _, command := range []string{"detect", "check", "verify", "status"} {
		response := loop.Handle(context.Background(), ToolRequest{Command: command})
		if len(response.DetectorStates) != 0 || response.Healthy || response.IncidentVerified != nil || response.VerificationStatus != VerificationAwaitingResponderCompletion {
			t.Fatalf("%s grants detector authority: %+v", command, response)
		}
	}
}

func externalHealthDetector() sdk.Detector {
	health := controllerDetector("health", time.Second, sdk.Finding{})
	health.spec.Class = sdk.DetectorClassHealth
	health.spec.Owner = sdk.DetectorOwnerHealthJudge
	health.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	health.spec.OriginatingCommit = "health-objective"
	health.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	return health
}

// externalUnavailableSnapshots fails every read, so an external-only
// controller that tries to observe health fails the test.
type externalUnavailableSnapshots struct{}

func (externalUnavailableSnapshots) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return nil, errors.New("api server unavailable")
}

// externalGoldenRequest loads the canonical incident request fixture.
func externalGoldenRequest(t *testing.T) IncidentRequest {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", "incident_request.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	message := &contracts.IncidentRequest{}
	if err := wire.Unmarshal(content, message); err != nil {
		t.Fatalf("fixture is not canonical ProtoJSON: %v", err)
	}
	return *incidentRequestState(message)
}

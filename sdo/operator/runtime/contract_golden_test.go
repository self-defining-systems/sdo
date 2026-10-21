package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Cross-language contract fixtures. The Go controller writes each shape
// Python parses into tests/fixtures/sdo/contracts/go/, and Python's strict
// models parse the same files (tests/unit/sdo/contracts/
// test_go_contract_fixtures.py, the broker and the SREGym receipt tests).
// Any Go encoding change fails here until the fixtures are regenerated with
//
//	SDO_UPDATE_GO_CONTRACT_FIXTURES=1 go test -run TestGoContractFixtures ./...
//
// and then has to pass the Python side too.

const updateGoContractFixtures = "SDO_UPDATE_GO_CONTRACT_FIXTURES"

var goContractFixtureDir = filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", "go")

func checkGoContractFixture(t *testing.T, name string, message proto.Message) {
	t.Helper()
	compact, err := wire.Canonical(message)
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, compact, "", "  "); err != nil {
		t.Fatalf("indent %s: %v", name, err)
	}
	encoded := append(indented.Bytes(), '\n')
	path := filepath.Join(goContractFixtureDir, name)
	if os.Getenv(updateGoContractFixtures) == "1" {
		if err := os.MkdirAll(goContractFixtureDir, 0o755); err != nil {
			t.Fatalf("create fixture dir: %v", err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		return
	}
	golden, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s (regenerate with %s=1): %v", path, updateGoContractFixtures, err)
	}
	if !bytes.Equal(golden, encoded) {
		t.Fatalf("%s differs from the Go encoding; regenerate with %s=1 and rerun the Python contract tests.\n"+
			"got:\n%s", path, updateGoContractFixtures, encoded)
	}
}

// fixedResultDispatcher answers every request with a deterministic result.
type fixedResultDispatcher struct {
	requests chan IncidentRequest
	result   func(IncidentRequest) IncidentResult
}

func (d *fixedResultDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	return d.result(request), nil
}

// goldenIncident runs one incident through a real controller: the cause
// fires at t=0-1 (dispatch at t=1), clears from t=2, and the baseline diff
// follows timeline. It returns the dispatched request, the incident view
// published at t=2, and the closure handed to the broker.
func goldenIncident(
	t *testing.T, timeline func(time.Time) []StateChange, result func(IncidentRequest) IncidentResult,
) (IncidentRequest, *IncidentView, IncidentClosure) {
	t.Helper()
	cause := controllerDetector(
		"cause", time.Second, stateFinding("cause"), stateFinding("cause"),
		sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{}, sdk.Finding{},
	)
	dispatcher := &fixedResultDispatcher{requests: make(chan IncidentRequest, 1), result: result}
	config := testControllerConfig()
	config.RepairPolicy = "recorded-actions"
	controller, err := NewController(
		config, []sdk.Detector{cause}, staticProvider{snapshot: sdktest.Snapshot{}}, dispatcher, time.Unix(0, 0),
	)
	if err != nil {
		t.Fatalf("new controller: %v", err)
	}
	controller.Baseline = &scriptedBaseline{changes: timeline}
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("firing step: %v", err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	awaitDispatchCompletionQueued(t, controller)
	if err := controller.Step(context.Background(), time.Unix(2, 0), nil); err != nil {
		t.Fatalf("step: %v", err)
	}
	view := controller.ExportState().IncidentView
	for sample := 3; sample < 8; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatalf("step %d: %v", sample, err)
		}
		if closure, ok := controller.PendingIncidentClosure(); ok {
			return request, view, closure
		}
	}
	t.Fatal("verified incident did not emit a closure")
	return IncidentRequest{}, nil, IncidentClosure{}
}

func goldenResult(request IncidentRequest, cause ConfirmedRootCause, action RepairActionReceipt) IncidentResult {
	return IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted,
		ConfirmedRootCauses: []ConfirmedRootCause{cause},
		RepairActions:       []RepairActionReceipt{action},
		FinalDetectorStates: []DetectorEvaluation{},
		VerificationEvidence: []VerificationEvidence{{
			Name: "sdo-incident-status", Passed: true, Details: "healthy", ObservedAt: time.Unix(2, 0).UTC(),
		}},
		Timing: TimingMetrics{StartedAt: time.Unix(1, 0).UTC(), CompletedAt: time.Unix(2, 0).UTC()},
	}
}

func mustProject[S any, M proto.Message](t *testing.T, project func(S) (M, error), value S) M {
	t.Helper()
	message, err := project(value)
	if err != nil {
		t.Fatalf("project contract: %v", err)
	}
	return message
}

func TestGoContractFixtures(t *testing.T) {
	at := time.Unix(1, 0).UTC()
	finding := stateFinding("cause")
	finding.DetectorID = "health-objective"

	// Incident views, as `sdo incident status` reads them.
	checkGoContractFixture(t, "incident_view_empty.json", mustProject(t, incidentViewContract, &IncidentView{
		IncidentID: "demo-1", ObservedAt: at, StateChanges: &StateChanges{BaselineAt: at, ObservedAt: at},
	}))
	clearing := finding
	clearing.Fingerprint = "clearing"
	checkGoContractFixture(t, "incident_view_populated.json", mustProject(t, incidentViewContract, &IncidentView{
		IncidentID: "demo-1", ObservedAt: at,
		BlockingDetectors: []string{"health-objective"}, BlockingFindings: []sdk.Finding{finding},
		ClearingFindings: []sdk.Finding{clearing},
		StateChanges: &StateChanges{
			BaselineAt: at, ObservedAt: at, Omitted: 1, UnobservedKinds: []string{"Secret"},
			Changes: []StateChange{
				{Kind: "Service", Name: "frontend", Change: StateChangeModified, Fields: []StateFieldChange{
					{Field: "selector", Before: "app=frontend", After: "app=frontend,x=y"},
				}},
				{Kind: "ConfigMap", Name: "geo-config", Change: StateChangeRemoved},
			},
		},
	}))

	// A correct repair: the selector fault is in the dispatch diff and the
	// responder's own action reverts it, so the closing view is empty.
	selectorTimeline := func(now time.Time) []StateChange {
		if now.Before(time.Unix(2, 0)) {
			return []StateChange{selectorFault}
		}
		return []StateChange{}
	}
	request, view, repaired := goldenIncident(t, selectorTimeline, func(request IncidentRequest) IncidentResult {
		return goldenResult(request,
			ConfirmedRootCause{
				Summary:   "frontend's selector matches no pods",
				Resources: []sdk.ObjectRef{{Kind: "Service", Name: "frontend"}},
				Evidence: []RootCauseEvidence{
					{Kind: "state-change", Source: "Service/frontend", Observation: "selector gained a label"},
				},
				ExplainedDetectors: []string{"cause"},
			},
			RepairActionReceipt{
				ActionID: "restore-selector", Kind: "kubectl", Target: "Service/frontend",
				Resources: []sdk.ObjectRef{{Kind: "Service", Name: "frontend"}},
				Summary:   "restore the selector", Details: "kubectl apply", StartedAt: time.Unix(1, 500_000_000).UTC(),
				CompletedAt: time.Unix(1, 600_000_000).UTC(), Success: true, Reversible: true,
			},
		)
	})
	checkGoContractFixture(t, "incident_request.json", mustProject(t, incidentRequestContract, request))
	checkGoContractFixture(t, "incident_view_open.json", mustProject(t, incidentViewContract, view))
	checkGoContractFixture(t, "closure_repaired.json", mustProject(t, incidentClosureContract, repaired))

	// A wrong fix citing its own edit: the real fault is invisible to the
	// diff and recovers by other means; the responder restarts frontend at
	// t=1.5 and cites the restart, which the diff first shows at t=2.
	restartTimeline := func(now time.Time) []StateChange {
		if now.Before(time.Unix(2, 0)) {
			return []StateChange{}
		}
		return []StateChange{{Kind: "Deployment", Name: "frontend", Change: StateChangeModified, Fields: []StateFieldChange{
			{Field: "template.annotations.kubectl.kubernetes.io/restartedAt", After: "1970-01-01T00:00:01Z"},
		}}}
	}
	_, _, ownEdit := goldenIncident(t, restartTimeline, func(request IncidentRequest) IncidentResult {
		return goldenResult(request,
			ConfirmedRootCause{
				Summary:   "a stale frontend rollout broke the service",
				Resources: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Name: "frontend"}},
				Evidence: []RootCauseEvidence{
					{Kind: "state-change", Source: "Deployment/frontend", Observation: "frontend rollout changed"},
				},
				ExplainedDetectors: []string{"cause"},
			},
			RepairActionReceipt{
				ActionID: "restart-frontend", Kind: "kubectl", Target: "deployment/frontend",
				Resources: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Name: "frontend"}},
				Summary:   "restart frontend", Details: "kubectl rollout restart", StartedAt: time.Unix(1, 500_000_000).UTC(),
				CompletedAt: time.Unix(1, 600_000_000).UTC(), Success: true, Reversible: true,
			},
		)
	})
	checkGoContractFixture(t, "closure_own_edit.json", mustProject(t, incidentClosureContract, ownEdit))
	open := wireState()
	open.PendingClosure = &repaired
	open.ClosureFailure.IncidentID = repaired.Request.IncidentID
	openRequest := repaired.Request
	open.IncidentRequest = &openRequest
	open.IncidentResult = repaired.Result
	state, err := controllerStateContract(open)
	if err != nil {
		t.Fatal(err)
	}
	checkGoContractFixture(t, "controller_state.json", state)
	closure, err := incidentClosureContract(repaired)
	if err != nil {
		t.Fatal(err)
	}
	checkGoContractFixture(t, "broker_ledger.json", &contracts.BrokerLedger{
		IncidentId: closure.GetRequest().GetIncidentId(), Closure: closure, ProposalCommit: "abc", OutcomeCommit: "def",
		Receipt:      &contracts.ClosureReceipt{IncidentId: closure.GetRequest().GetIncidentId(), OutcomeCommit: "def", AckToken: "token"},
		Acknowledged: true,
	})
	if len(ownEdit.ObservedStateChanges) != 1 || !ownEdit.ObservedStateChanges[0].FirstObservedAt.Equal(time.Unix(2, 0).UTC()) {
		t.Fatalf("the restart must first be observed after the repair started: %+v", ownEdit.ObservedStateChanges)
	}
}

package runtime

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// hiddenRequestKeeps are the IncidentRequest fields the agent's task keeps under
// hide_findings_from_agent: identity, workspace, deadline, the page and the
// application's own context. Every other field must be empty, so a new field
// fails this test until it is classified here or cleared.
var hiddenRequestKeeps = map[string]bool{
	"ExternalMessage": true, "Page": true, "Telemetry": true, "NamespaceOnboarding": true, "ObservationGeneration": true,
	"DetectedAt": true, "SchemaVersion": true, "Application": true, "Namespace": true, "IncidentID": true,
	"SourceCommit": true, "DeployedCommit": true, "DeploymentInFlight": true, "RepositoryWorktree": true,
	"RepositoryBaseCommit": true, "ResponseDeadline": true, "CancellationToken": true, "RepairPolicy": true,
	"MaxResponseLoops": true, "PreparedAgentTask": true,
}

// filledIncidentRequest sets every field of an IncidentRequest to a non-zero
// synthetic value carrying the marker text.
func filledIncidentRequest(t *testing.T, marker string) IncidentRequest {
	t.Helper()
	var request IncidentRequest
	value := reflect.ValueOf(&request).Elem()
	for i := 0; i < value.NumField(); i++ {
		fill(value.Field(i), marker)
		if value.Field(i).IsZero() {
			t.Fatalf("cannot fill IncidentRequest.%s", value.Type().Field(i).Name)
		}
	}
	return request
}

func fill(field reflect.Value, marker string) {
	switch field.Kind() {
	case reflect.String:
		field.SetString(marker)
	case reflect.Bool:
		field.SetBool(true)
	case reflect.Int, reflect.Int32, reflect.Int64:
		field.SetInt(1)
	case reflect.Slice:
		slice := reflect.MakeSlice(field.Type(), 1, 1)
		fill(slice.Index(0), marker)
		field.Set(slice)
	case reflect.Pointer:
		pointer := reflect.New(field.Type().Elem())
		if pointer.Elem().Kind() == reflect.Struct {
			fill(pointer.Elem(), marker)
		}
		field.Set(pointer)
	case reflect.Struct:
		if field.Type() == reflect.TypeOf(time.Time{}) {
			field.Set(reflect.ValueOf(time.Unix(1000, 0).UTC()))
			return
		}
		for i := 0; i < field.NumField(); i++ {
			if field.Field(i).CanSet() {
				fill(field.Field(i), marker)
			}
		}
	case reflect.Map:
		field.Set(reflect.MakeMap(field.Type()))
	}
}

func TestHideFindingsTaskCarriesNothingADetectorFound(t *testing.T) {
	request := filledIncidentRequest(t, "synthetic-finding-marker")
	told := hiddenFindingsRequest(request)
	value, kept := reflect.ValueOf(told), 0
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		if hiddenRequestKeeps[name] {
			kept++
			if value.Field(i).IsZero() {
				t.Errorf("the task lost %s", name)
			}
			continue
		}
		if !value.Field(i).IsZero() {
			t.Errorf("the task still carries %s", name)
		}
	}
	if kept != len(hiddenRequestKeeps) {
		t.Fatalf("hiddenRequestKeeps names %d fields, IncidentRequest has %d of them", len(hiddenRequestKeeps), kept)
	}
	// The dispatcher encodes this request with the setting on, the whole incident with it off.
	if got := (KubernetesJobDispatcher{HideFindings: true}).agentTaskRequest(request); len(got.Findings) != 0 || len(got.DetectorHistory) != 0 {
		t.Fatal("the dispatcher's task keeps findings with the setting on")
	}
	if got := (KubernetesJobDispatcher{}).agentTaskRequest(request); !reflect.DeepEqual(got, request) {
		t.Fatal("the setting off changed the agent's task")
	}
}

func TestHideFindingsNamesTheControlNamespaceOnlyWhenOn(t *testing.T) {
	if got := (KubernetesJobDispatcher{Namespace: "demo-sdo"}).hiddenNamespaces(); got != nil {
		t.Fatalf("setting off hides %v", got)
	}
	if got := (KubernetesJobDispatcher{Namespace: "demo-sdo", HideFindings: true}).hiddenNamespaces(); len(got) != 1 || got[0] != "demo-sdo" {
		t.Fatalf("setting on hides %v, want the control namespace", got)
	}
}

// Every sdo command that shows detector output answers the fixed text; the
// allowed ones never return a structured finding field.
func TestHideFindingsSdoCommands(t *testing.T) {
	hidden := []string{"status", "detect", "diagnose", "menu", "mitigate", "plan", "apply", "rollback", "act-result", "report",
		"changes", "timeline", "map", "investigations", "check", "unknown-future-command"} // none names a playbook
	loop := &ResponderLoop{HideFindings: true}
	for _, command := range hidden {
		response := loop.Handle(context.Background(), ToolRequest{Command: command})
		if response.Text != FindingsHiddenText || response.Error != "" || len(response.Findings) != 0 {
			t.Errorf("%s with findings hidden answered %+v", command, response)
		}
	}
	full := ToolResponse{Command: "logs", Text: "app log line", Findings: []sdk.Finding{{DetectorID: "synthetic"}},
		ControllerFindings: []sdk.Finding{{DetectorID: "synthetic"}}, BeforeFindings: []sdk.Finding{{DetectorID: "synthetic"}},
		NewSinceDispatch: []sdk.Finding{{DetectorID: "synthetic"}}, Drift: []sdk.Finding{{DetectorID: "synthetic"}},
		DriftEvents: []sdk.DriftEvent{{}}, DetectorStates: []DetectorEvaluation{{}}, ProgramRuns: []ProgramRun{{}},
		Progress: &FindingProgress{}, PriorStages: []StageNote{{}}, VerificationBlockers: []VerificationBlocker{{}},
		MitigationHealth: &MitigationHealth{}, Healthy: true}
	stripped := withoutDetectorFields(full)
	encoded, err := json.Marshal(stripped)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "synthetic") || stripped.Text != "app log line" || stripped.Healthy || stripped.MitigationHealth != nil || stripped.Progress != nil {
		t.Fatalf("an allowed command keeps detector fields: %s", encoded)
	}
}

// detect --wait with findings hidden returns at Recovered and names nothing else.
func TestHideFindingsDetectWaitOnlyWaitsForRecovered(t *testing.T) {
	recovered := false
	loop := &ResponderLoop{HideFindings: true, IncidentStatus: func() *apihistory.IncidentVerified {
		if recovered {
			return &apihistory.IncidentVerified{}
		}
		return nil
	}}
	recovered = true
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 5})
	if !strings.Contains(response.Text, "recovered: SDO is ending your incident work") || len(response.Findings) != 0 {
		t.Fatalf("wait answered %+v", response)
	}
	recovered = false
	response = loop.Handle(context.Background(), ToolRequest{Command: "detect", WaitSeconds: 0.01})
	if !strings.HasPrefix(response.Text, "wait: not recovered after") {
		t.Fatalf("wait answered %+v", response)
	}
}

// With the setting on a page opens its incident at once (no evidence wait) and
// with no clue, whatever is active; with pages_only alone it waits or carries the clue.
func TestHideFindingsPageOpensAtOnceWithNoClue(t *testing.T) {
	for _, tc := range []struct {
		name         string
		hide, active bool
		findings     int
		opensAtOnce  bool
	}{
		{name: "hidden, a clue is active", hide: true, active: true, findings: 0, opensAtOnce: true},
		{name: "hidden, nothing active", hide: true, findings: 0, opensAtOnce: true},
		{name: "pages only, a clue is active", active: true, findings: 1, opensAtOnce: true},
		{name: "pages only, nothing active", findings: 0, opensAtOnce: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			probe := &switchedProbe{id: memory.TrafficDetectorPrefix + "web", scenario: "browse"}
			slow := &switchedProbe{id: memory.TrafficDetectorPrefix + "slow", scenario: "checkout"}
			clue := &switchedProbe{id: "synthetic-drift", scenario: "drifted"}
			cfg := testControllerConfig()
			cfg.PagesOnly, cfg.HideFindings = true, tc.hide
			c, err := NewController(cfg, []sdk.Detector{probe, slowProbe{slow}, learnedClue{clue}}, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return now }
			var page *ExternalIncident
			c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) { return page, nil }
			if tc.active {
				clue.failing.Store(true)
				stepFor(t, c, &now, 3)
			}
			page = &ExternalIncident{ID: "page-1", Namespace: cfg.Namespace, Message: syntheticPage}
			stepFor(t, c, &now, 1)
			if c.incidentOpen != tc.opensAtOnce {
				t.Fatalf("incident open %v after one step, want %v", c.incidentOpen, tc.opensAtOnce)
			}
			if !tc.opensAtOnce {
				return
			}
			request := c.currentIncidentRequest
			if request.Page != syntheticPage || len(request.Findings) != tc.findings || (tc.hide && len(request.StandingKeys) != 0) {
				t.Fatalf("page opened with %d findings, standing %v; want %d", len(request.Findings), request.StandingKeys, tc.findings)
			}
		})
	}
}

func TestHideFindingsSettings(t *testing.T) {
	config := settings.Default()
	config.Agent.Model = "synthetic-model"
	config.Response.Investigators = 2
	digest := config.DetectionDigest()
	if (ControllerConfig{}).WithSettings(config).HideFindings || agentInvestigators(config) != config.Response.Investigators {
		t.Fatal("hide_findings_from_agent is on by default")
	}
	config.Detection.HideFindingsFromAgent = true
	if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "requires detection.pages_only") {
		t.Fatalf("hide_findings_from_agent without pages_only: %v", err)
	}
	config.Detection.PagesOnly = true
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	if !(ControllerConfig{}).WithSettings(config).HideFindings || agentInvestigators(config) != 0 {
		t.Fatal("the setting did not reach the controller or still starts investigators")
	}
	off := settings.Default()
	off.Agent.Model = "synthetic-model"
	off.Response.Investigators = 2
	if off.DetectionDigest() != digest {
		t.Fatal("the default detection digest changed")
	}
}

// A playbook the agent names runs as usual with findings hidden (the agent's
// way to use learned memory by hand); without a name the same commands stay
// hidden. The loop never pulls the controller's current findings in.
func TestHideFindingsNamedPlaybookCommandsRun(t *testing.T) {
	refreshed := false
	loop := &ResponderLoop{HideFindings: true, CurrentEvidence: func() *IncidentRequest {
		refreshed = true
		return &IncidentRequest{IncidentID: "synthetic", Findings: []sdk.Finding{{DetectorID: "synthetic"}}}
	}}
	loop.Request.IncidentID = "synthetic"
	for _, request := range []ToolRequest{
		{Command: "diagnose", ProgramID: "synthetic-playbook"},
		{Command: "menu", ProgramID: "synthetic-playbook"},
		{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: "synthetic-playbook"}},
	} {
		response := loop.Handle(context.Background(), request)
		if response.Text == FindingsHiddenText || response.Summary == FindingsHiddenText {
			t.Errorf("%s %s was hidden", request.Command, request.ProgramID)
		}
		if len(response.Findings) != 0 || len(loop.Request.Findings) != 0 {
			t.Errorf("%s pulled findings into the loop or response", request.Command)
		}
	}
	if refreshed {
		t.Fatal("the loop read the controller's current evidence with findings hidden")
	}
	if !playbookScoped(ToolRequest{Command: "diagnose", ProgramID: "x"}) || playbookScoped(ToolRequest{Command: "diagnose"}) ||
		playbookScoped(ToolRequest{Command: "menu"}) || playbookScoped(ToolRequest{Command: "mitigate"}) {
		t.Fatal("playbookScoped misclassifies a command")
	}
}

// countingStage is an automatic stage that records whether it ran.
type countingStage struct{ runs *int }

func (countingStage) Name() string { return "deterministic" }
func (s countingStage) Run(context.Context, *IncidentState) (*contracts.StageReport, error) {
	*s.runs++
	return &contracts.StageReport{}, nil
}

// The 'Generic page + manual playbooks' arm: with the code path's response
// stages overridden to the agent alone (agent.code_path_overrides), a page with
// findings hidden goes straight to the agent and the automatic playbook stage
// never runs; with both stages it runs first.
func TestHideFindingsAgentOnlyStagesNeverRunTheAutomaticStage(t *testing.T) {
	for _, stages := range [][]string{{"agent"}, {"deterministic", "agent"}} {
		runs := 0
		cfg := testControllerConfig()
		cfg.PagesOnly, cfg.HideFindings = true, true
		dispatcher := &stagedDispatcher{recordingDispatcher: recordingDispatcher{requests: make(chan IncidentRequest, 4)}, stage: countingStage{&runs}}
		c, err := NewController(cfg, nil, staticProvider{}, dispatcher, time.Unix(1000, 0).UTC())
		if err != nil {
			t.Fatal(err)
		}
		c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: stages}}
		request := IncidentRequest{IncidentID: "synthetic-page", Application: "demo", Namespace: cfg.Namespace, Page: syntheticPage,
			ResponseDeadline: time.Now().Add(time.Hour)}
		c.incidentOpen, c.currentIncidentRequest = true, cloneIncidentRequest(&request)
		if _, _, err := c.runResponsePipeline(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		wantRuns := map[int]int{1: 0, 2: 1}[len(stages)]
		if dispatcher.calls != 1 || runs != wantRuns {
			t.Fatalf("stages %v: agent dispatched %d times, automatic stage ran %d times; want 1 and %d", stages, dispatcher.calls, runs, wantRuns)
		}
	}
}

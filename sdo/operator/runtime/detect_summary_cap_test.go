package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// A stage that asked many playbooks for a judgement leaves one long
// "tried by sdo" line per playbook, so the lines above the candidate view
// alone exceed the summary cap and leave the view a negative byte budget.
// `sdo detect` must still answer within the cap.
func TestDetectSummaryAnswersWhenTheHeaderAloneExceedsTheCap(t *testing.T) {
	request := detectRequest()
	request.Findings = []sdk.Finding{probeFinding("create")}
	request.OpeningKeys = []string{findingKey(request.Findings[0])}
	stage := &contracts.StageReport{Stage: "deterministic", HandoffSelected: true}
	for i := range 20 {
		stage.Diagnoses = append(stage.Diagnoses, &contracts.PlaybookVerdict{
			PlaybookId: fmt.Sprintf("synthetic-feature-disabled-%02d", i),
			Result:     "needs_judgement",
			Reason:     strings.Repeat("the setting differs from the recorded baseline ", 4),
		})
	}
	request.PriorStages = []*contracts.StageReport{stage}
	text := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: detectApp("create"), Now: detectNow()}).Text
	if len(text) > detectSummaryCap || !utf8.ValidString(text) {
		t.Fatalf("summary is %d bytes (valid UTF-8 %v)", len(text), utf8.ValidString(text))
	}
	if !strings.Contains(text, "incident inc-1") {
		t.Fatalf("summary lost its lead:\n%s", text)
	}
}

// render keeps within any byte budget, including none or a negative one,
// and never splits a multi-byte character.
func TestCandidateRenderKeepsWithinAnyBudget(t *testing.T) {
	views := []candidateView{
		{in: candidateInput{MapLines: []string{strings.Repeat("界", 10)}}},
		{in: candidateInput{MapLines: []string{strings.Repeat("x", 300), strings.Repeat("y ", 200)}}},
	}
	for _, view := range views {
		for _, budget := range []int{-1139, -1, 0, 1, 2, 3, 4, 5, 80, 400} {
			text := view.render(budget)
			if len(text) > max(0, budget) || !utf8.ValidString(text) {
				t.Errorf("budget %d: got %d bytes (%q)", budget, len(text), text)
			}
		}
	}
}

// A handler that panics answers that one request with an error; the server
// keeps serving the next request instead of taking the controller down.
func TestToolServerAnswersAPanickingHandlerWithAnError(t *testing.T) {
	session := filepath.Join(t.TempDir(), "tools")
	server := &SessionToolServer{Directory: session, Handle: func(_ context.Context, request ToolRequest) ToolResponse {
		if request.Command == "detect" {
			var lines []string
			_ = lines[:len(lines)-1]
		}
		return ToolResponse{Command: request.Command, Summary: "ok"}
	}}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for name, command := range map[string]string{"a.json": "detect", "b.json": "menu"} {
		if err := os.WriteFile(filepath.Join(session, "requests", name), []byte(`{"command":"`+command+`"}`), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := server.process(context.Background()); err != nil {
		t.Fatalf("process stopped on a handler panic: %v", err)
	}
	read := func(name string) ToolResponse {
		data, err := os.ReadFile(filepath.Join(session, "responses", name))
		if err != nil {
			t.Fatal(err)
		}
		var response ToolResponse
		if err := json.Unmarshal(data, &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	if failed := read("a.json"); !strings.Contains(failed.Error, "sdo detect failed inside SDO") {
		t.Fatalf("panicking request answered %+v", failed)
	}
	if ok := read("b.json"); ok.Error != "" || ok.Summary != "ok" {
		t.Fatalf("next request answered %+v", ok)
	}
}

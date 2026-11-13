package runtime

import (
	"context"
	"testing"
)

// An external incident has the candidate view only with external clues.
func TestExternalIncidentsHaveACandidateViewOnlyWithClues(t *testing.T) {
	external := IncidentRequest{IncidentID: "page-1", ExternalMessage: "users report errors"}
	if !(&ResponderLoop{ExternalClues: true}).hasCandidateView(external) {
		t.Fatal("an external incident with clues has the candidate view")
	}
	if (&ResponderLoop{}).hasCandidateView(external) {
		t.Fatal("an external incident without clues has no candidate view")
	}
	if !(&ResponderLoop{}).hasCandidateView(IncidentRequest{IncidentID: "i"}) {
		t.Fatal("an alert-opened incident has the candidate view")
	}
	if (*ResponderLoop)(nil).hasCandidateView(external) {
		t.Fatal("no loop, no view")
	}
}

func TestExternalIncidentWithCluesAndNoWriteStartsInvestigatorsAndWithoutCluesNone(t *testing.T) {
	for _, tc := range []struct {
		name  string
		clues bool
		want  int
	}{{"clues", true, 2}, {"no clues", false, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			loop := &ResponderLoop{ExternalClues: tc.clues}
			record := &launchRecord{}
			registry := &InvestigatorRegistry{Count: 2, Wait: immediate}
			if loop.hasCandidateView(IncidentRequest{ExternalMessage: "page"}) {
				registry.begin(context.Background(), investigatorPlan{
					IncidentID: "page-1", Board: &InvestigationBoard{},
					HasWrite: func() bool { return false },
					Briefs:   func(_ context.Context, max int) []investigatorBrief { return briefFixture(t)[:max] },
					Launch:   record.launch,
				})
				registry.mu.Lock()
				run := registry.runs["page-1"]
				registry.mu.Unlock()
				<-run.done
			}
			if len(record.ids) != tc.want {
				t.Fatalf("started %v, want %d", record.ids, tc.want)
			}
		})
	}
}

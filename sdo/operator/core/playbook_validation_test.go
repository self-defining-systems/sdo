package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
)

type capturedFindingSink struct{ finding sdk.Finding }

func (s *capturedFindingSink) Emit(_ context.Context, finding sdk.Finding) error {
	s.finding = finding
	return nil
}

func TestPlaybookResolvingSinkKeepsCoreStorageNeutral(t *testing.T) {
	inner := &capturedFindingSink{}
	sink := PlaybookResolvingSink{Inner: inner, Resolve: func(references []string) ([]string, error) {
		if len(references) != 1 || references[0] != "old-reference" {
			t.Fatalf("unexpected references: %v", references)
		}
		return []string{"service-endpoints"}, nil
	}}
	if err := sink.Emit(context.Background(), sdk.Finding{RuleID: "fault", Playbooks: []string{"old-reference"}}); err != nil {
		t.Fatal(err)
	}
	if len(inner.finding.Playbooks) != 1 || inner.finding.Playbooks[0] != "service-endpoints" {
		t.Fatalf("unresolved finding: %+v", inner.finding)
	}
}

func TestPlaybookResolvingSinkRejectsResolverFailure(t *testing.T) {
	sink := PlaybookResolvingSink{Inner: &capturedFindingSink{}, Resolve: func([]string) ([]string, error) {
		return nil, errors.New("unknown playbook")
	}}
	if err := sink.Emit(context.Background(), sdk.Finding{RuleID: "fault", Playbooks: []string{"bad"}}); err == nil || !strings.Contains(err.Error(), "unknown playbook") {
		t.Fatalf("resolver failure was lost: %v", err)
	}
}

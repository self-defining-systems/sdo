package core

import (
	"context"
	"fmt"
	"regexp"

	"sdo.dev/operator/sdk"
)

// PlaybookResolver is supplied by the operational-memory edge. Core does not
// interpret detector references or know their storage layout.
type PlaybookResolver func([]string) ([]string, error)

var logicalPlaybookID = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

func logicalPlaybookIDs(references []string) ([]string, error) {
	ids := make([]string, 0, len(references))
	for _, reference := range references {
		if !logicalPlaybookID.MatchString(reference) {
			return nil, fmt.Errorf("invalid logical playbook ID %q", reference)
		}
		ids = append(ids, reference)
	}
	return ids, nil
}

type PlaybookResolvingSink struct {
	Inner   Sink
	Resolve PlaybookResolver
}

func (s PlaybookResolvingSink) Emit(ctx context.Context, finding sdk.Finding) error {
	if s.Inner == nil || s.Resolve == nil {
		return fmt.Errorf("inner sink and playbook resolver are required")
	}
	ids, err := s.Resolve(finding.Playbooks)
	if err != nil {
		return fmt.Errorf("finding %q playbooks: %w", finding.RuleID, err)
	}
	if len(ids) != len(finding.Playbooks) {
		return fmt.Errorf("finding %q playbook resolver changed reference count", finding.RuleID)
	}
	finding.Playbooks = ids
	return s.Inner.Emit(ctx, finding)
}

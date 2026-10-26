package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

// SourceRepairPaths excludes working playbook code from a responder's source
// submission. These drafts remain isolated until reflection submits them to
// the complete memory validator; other memory edits retain ordinary rejection.
func SourceRepairPaths(paths []string) []string {
	var source []string
	for _, path := range paths {
		if !workingProgramPath(path) {
			source = append(source, path)
		}
	}
	return source
}
func workingProgramPath(path string) bool {
	return strings.HasPrefix(path, layout.PlaybooksDir+"/") || strings.HasPrefix(path, layout.LibrariesDir+"/")
}

// carryWorkingPrograms seeds the resumed session's reflection checkout once.
// Rejoining a durable reflection proposal never copies over later refinements.
func (b *Broker) carryWorkingPrograms(ctx context.Context, incidentID, target string) error {
	responder, err := b.proposal(incidentID, ProposalPhase)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	paths, err := validate.ChangedPaths(ctx, responder.Path, responder.BaseCommit)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if workingProgramPath(path) {
			if err := copyChange(responder.Path, target, path); err != nil {
				return fmt.Errorf("preserve working playbook %s: %w", path, err)
			}
		}
	}
	return nil
}

package broker

import (
	"context"
	"errors"
	"sort"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

// ChangePartition splits a proposal's actual changes into the part that can be
// published and the scratch edits that never are. SDO holds the paths under
// .sdo/ (against the base commit); Scratch the app files whose working content
// is not what the agent committed. Both are sorted.
type ChangePartition struct {
	SDO     []string
	Scratch []string
	// Source are the app paths the agent committed: what a source
	// publication carries.
	Source []string
}

// PartitionChanges is the actual change set of a materialized checkout.
func PartitionChanges(ctx context.Context, checkout, baseCommit string) (ChangePartition, error) {
	paths, err := validate.ChangedPaths(ctx, checkout, baseCommit)
	if err != nil {
		return ChangePartition{}, err
	}
	return partitionPaths(paths), nil
}

func partitionPaths(paths []string) ChangePartition {
	var p ChangePartition
	for _, path := range paths {
		if layout.IsMemoryPath(path) {
			p.SDO = append(p.SDO, path)
		} else {
			p.Scratch = append(p.Scratch, path)
		}
	}
	sort.Strings(p.SDO)
	sort.Strings(p.Scratch)
	return p
}

// ProposalPartition is PartitionChanges for a prepared proposal.
func (b *Broker) ProposalPartition(ctx context.Context, incidentID string, phase Phase) (ChangePartition, error) {
	proposal, err := b.proposal(incidentID, phase)
	if err != nil {
		return ChangePartition{}, err
	}
	partition, err := PartitionChanges(ctx, proposal.Path, proposal.BaseCommit)
	if err != nil {
		return partition, err
	}
	change, err := SourceChangeOf(ctx, proposal.Path, proposal.BaseCommit)
	if err != nil {
		return ChangePartition{}, err
	}
	partition.Source, partition.Scratch = change.Kept, change.Scratch
	return partition, nil
}

// UncapturedPaths are the app files a prepared proposal's checkout holds that the agent has not committed: the
// scratch a source publication discards (SourceChangeOf), in sorted order. A history the agent rewrote below its
// base has no scratch to report; publication records that.
func (b *Broker) UncapturedPaths(ctx context.Context, incidentID string, phase Phase) ([]string, error) {
	change, err := b.SourceChange(ctx, incidentID, phase)
	var failure *validate.Failure
	if errors.As(err, &failure) {
		return nil, nil
	}
	return change.Scratch, err
}

// SourceRuleDiagnostics are the messages of the source rules (source.policy, source.size, the Secret rules) the
// committed source of a prepared proposal breaks right now, one line each. They are the same checks a publication
// applies, so the closing turn can ask the agent to fix a commit before the publication would refuse it.
func (b *Broker) SourceRuleDiagnostics(ctx context.Context, incidentID string, phase Phase) ([]string, error) {
	proposal, err := b.proposal(incidentID, phase)
	if err != nil {
		return nil, err
	}
	change, err := SourceChangeOf(ctx, proposal.Path, proposal.BaseCommit)
	if err != nil {
		var failure *validate.Failure
		if errors.As(err, &failure) {
			return diagnosticLines(failure.Diagnostics), nil
		}
		return nil, err
	}
	err = b.requireSourceRules(ctx, proposal.Path, proposal.BaseCommit, change)
	var failure *validate.Failure
	if errors.As(err, &failure) {
		return diagnosticLines(failure.Diagnostics), nil
	}
	return nil, err
}

func diagnosticLines(diagnostics []validate.Diagnostic) []string {
	lines := make([]string, 0, len(diagnostics))
	for _, d := range diagnostics {
		lines = append(lines, strings.TrimSpace(d.Rule+" "+d.Path+": "+d.Message+" "+d.Hint))
	}
	return lines
}

// ProposalCheckout is the path of a prepared proposal's checkout.
func (b *Broker) ProposalCheckout(incidentID string, phase Phase) (string, error) {
	proposal, err := b.proposal(incidentID, phase)
	if err != nil {
		return "", err
	}
	return proposal.Path, nil
}

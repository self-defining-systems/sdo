package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/patchset"
)

// materializesPatches reports which proposal checkouts get the effective view:
// the phases whose agents read, run and edit app files (the deployer's initial
// phase and the responder's proposal). Reflection and import checkouts are
// memory-only.
func materializesPatches(phase Phase) bool {
	return phase == InitialPhase || phase == ProposalPhase
}

// ChangePartition splits a proposal's actual changes into the part that can be
// published and the scratch edits that never are. SDO holds the paths under
// .sdo/ (against the base commit); Scratch the app files that differ from the
// checkout's recorded effective view, so files the patch series put there
// are not reported. Both are sorted.
type ChangePartition struct {
	SDO     []string
	Scratch []string
	// Source are the app paths a responder committed (ProposalPhase only):
	// what a source publication carries.
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
	if err != nil || phase != ProposalPhase {
		return partition, err
	}
	change, err := SourceChangeOf(ctx, proposal.Path, proposal.BaseCommit)
	if err != nil {
		return ChangePartition{}, err
	}
	partition.Source, partition.Scratch = change.Kept, change.Scratch
	return partition, nil
}

// UncapturedScratch is the scratch-consistency function behind the
// `patch.uncaptured` rule: the app files of the checkout that the checkout's
// own .sdo/deployment/patches series does not reproduce from the base tree. A
// series that does not apply is a *patchset.PatchError.
func UncapturedScratch(ctx context.Context, checkout string) ([]string, error) {
	return patchset.Uncaptured(ctx, checkout)
}

// UncapturedRule names the rule in diagnostics and correction feedback.
const UncapturedRule = validate.RulePatchUncaptured

// ScratchFeedback is the worker-finalization hook: the correction text for a
// turn that ends with app-file edits no patch reproduces, or "" when the
// checkout is consistent. It is sent back to the agent as a correction; it is
// never a publish failure raised late.
func ScratchFeedback(ctx context.Context, checkout string) (string, error) {
	paths, err := UncapturedScratch(ctx, checkout)
	if err != nil {
		return "", err
	}
	return uncapturedMessage(paths), nil
}

func uncapturedMessage(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	const shown = 20
	listed := paths
	if len(listed) > shown {
		listed = listed[:shown]
	}
	more := ""
	if len(paths) > shown {
		more = fmt.Sprintf(" (and %d more)", len(paths)-shown)
	}
	return fmt.Sprintf("%s: %d app file(s) differ from what your patches produce: %s%s. "+
		"App files are never published; capture each intended change with `sdo-operator patch create --reason \"<why>\" [paths...]`, "+
		"or revert the file. Then run `sdo-operator patch check`.", UncapturedRule, len(paths), strings.Join(listed, ", "), more)
}

// scratchChecked reports whether an actor's publication is bound by the
// scratch-consistency rule: the deployer, whose app edits are still patches. A
// responder commits its source instead (SourceChangeOf), and what it leaves
// uncommitted is scratch that is discarded, never a rejection.
func scratchChecked(actor Actor, phase Phase) bool {
	return actor == Deployer && phase == InitialPhase
}

// requireCapturedScratch enforces `patch.uncaptured` at publish and returns
// the change set restricted to .sdo/ paths (the only publishable ones). App
// files that the series reproduces are consistent scratch and are dropped from
// the set; any other app-file difference rejects the publication. A series
// that does not apply is left to the shared check, which reports patch.invalid.
func requireCapturedScratch(ctx context.Context, checkout string, paths []string) ([]string, error) {
	uncaptured, err := UncapturedScratch(ctx, checkout)
	var invalid *patchset.PatchError
	if err != nil && !errors.As(err, &invalid) {
		return nil, err
	}
	if len(uncaptured) > 0 {
		return nil, validate.Err([]validate.Diagnostic{{
			Rule: UncapturedRule, Path: uncaptured[0], Message: uncapturedMessage(uncaptured),
			Hint: "run `sdo-operator patch create --reason \"<why>\"` for each intended change, or revert the file",
		}})
	}
	return partitionPaths(paths).SDO, nil
}

// ScratchFeedback is the correction text for a prepared proposal whose
// checkout holds uncaptured app edits, or "" when it is consistent (and for
// phases whose checkout is not materialized).
func (b *Broker) ScratchFeedback(ctx context.Context, incidentID string, phase Phase) (string, error) {
	if !materializesPatches(phase) {
		return "", nil
	}
	proposal, err := b.proposal(incidentID, phase)
	if err != nil {
		return "", err
	}
	if phase == ProposalPhase {
		change, err := SourceChangeOf(ctx, proposal.Path, proposal.BaseCommit)
		if err != nil {
			return "", err
		}
		return CleanupMessage(change.Scratch), nil
	}
	return ScratchFeedback(ctx, proposal.Path)
}

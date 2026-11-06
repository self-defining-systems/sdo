package broker

import (
	"context"
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
	return PartitionChanges(ctx, proposal.Path, proposal.BaseCommit)
}

// UncapturedScratch is the scratch-consistency function behind the
// `patch.uncaptured` rule: the app files of the checkout that the checkout's
// own .sdo/deployment/patches series does not reproduce from the base tree. A
// series that does not apply is a *patchset.PatchError.
func UncapturedScratch(ctx context.Context, checkout string) ([]string, error) {
	return patchset.Uncaptured(ctx, checkout)
}

// UncapturedRule names the rule in diagnostics and correction feedback.
const UncapturedRule = "patch.uncaptured"

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

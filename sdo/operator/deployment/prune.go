package deployment

import (
	"context"
	"fmt"
	"sort"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// Pruner is the part of an Applier that removes generated-name objects a
// release superseded. An Applier that does not implement it leaves them.
type Pruner interface {
	// AppliedJobs lists the Jobs in namespace the deployer applied itself:
	// its field manager owns them and they carry its source-tree stamp. A Job
	// someone else created is never listed, whatever its name.
	AppliedJobs(ctx context.Context, namespace string) ([]LiveObject, error)
	// Delete deletes ref and returns once it is gone.
	Delete(ctx context.Context, ref sdk.ObjectRef, propagation Propagation) error
}

// pruneSuperseded deletes the finished Jobs this deployer applied in an earlier
// revision whose generated name (rendered.StableWorkloadName: `<prefix>-<hash>`)
// this revision replaced with another name of the same stable identity, hooks
// included. Such a Job is dead weight and, left behind, is a stranger to the
// accepted source. Anything else stays: a name this revision still renders, a
// Job whose stable identity this revision does not render (the deployer does
// not delete what a release merely stopped declaring), a running Job, and
// every object the deployer did not apply. It returns the pruned refs.
func (r *applyRun) pruneSuperseded(ctx context.Context, set rendered.DesiredSet) ([]sdk.ObjectRef, error) {
	pruner, ok := r.deployer.Applier.(Pruner)
	if !ok {
		return nil, nil
	}
	names := map[string]bool{}
	stable := map[string]bool{}
	for _, phase := range set.Phases {
		for _, object := range phase.Objects {
			if object.Ref.Kind != "Job" || object.Ref.Namespace != r.deployer.Namespace {
				continue
			}
			names[object.Ref.Name] = true
			if rendered.StableWorkloadName("Job", object.Ref.Name) != object.Ref.Name {
				stable[rendered.StableWorkloadName("Job", object.Ref.Name)] = true
			}
		}
	}
	if len(stable) == 0 {
		return nil, nil
	}
	live, err := pruner.AppliedJobs(ctx, r.deployer.Namespace)
	if err != nil {
		return nil, fmt.Errorf("list applied Jobs to prune superseded ones: %w", err)
	}
	var superseded []LiveObject
	for _, job := range live {
		name := job.Ref.Name
		if names[name] || !job.Finished || job.Ref.Kind != "Job" || !stable[rendered.StableWorkloadName("Job", name)] {
			continue
		}
		superseded = append(superseded, job)
	}
	sort.Slice(superseded, func(i, j int) bool { return superseded[i].Ref.Name < superseded[j].Ref.Name })
	var pruned []sdk.ObjectRef
	for _, job := range superseded {
		ref := job.Ref
		if ref.Namespace == "" {
			ref.Namespace = r.deployer.Namespace
		}
		if err := pruner.Delete(ctx, ref, PropagationBackground); err != nil {
			return pruned, fmt.Errorf("prune superseded Job %s: %w", ref.Name, err)
		}
		pruned = append(pruned, ref)
	}
	return pruned, nil
}

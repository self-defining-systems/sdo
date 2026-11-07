package deployment

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	"sdo.dev/operator/deployment/internal/deploymentio"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// ImmutableConflict is an object the API server refuses to update in place
// because the update changes a field that cannot change after creation.
type ImmutableConflict struct {
	// Ref names the refused object; Namespace may be empty when the server's
	// answer does not say it.
	Ref sdk.ObjectRef
	// Fields are the immutable field paths the server named ("spec.template").
	Fields []string
	// Detail is the server's message, for the hold text.
	Detail string
}

// LiveObject is what the recreate policy reads of a live object.
type LiveObject struct {
	Ref    sdk.ObjectRef
	UID    string
	Labels map[string]string
	// Controlled reports a controller owner reference.
	Controlled bool
	// Finished is a Job's Complete or Failed condition.
	Finished bool
	// AppliedByDeployer reports that the deployer's field manager applied the
	// object (server-side apply). Only such an object is SDO's to delete;
	// Recreator.Get fills it.
	AppliedByDeployer bool
	// Orphan is the stamp MarkOrphan left; nil when the object has none.
	Orphan *OrphanStamp
}

// OrphanStamp marks a dependent of a workload the deployer is about to delete
// with orphan propagation. It is written before the delete so a failed apply
// or a crash afterwards cannot lose track of what must be cleaned up.
type OrphanStamp struct {
	// From is the UID of the workload that was deleted.
	From string
	// Owner is the deleted workload's Kind/name: its replacement is the live
	// object of the same Kind and name with another UID.
	Owner string
	// Selector is the replacement's label selector; an orphan it matches is
	// adopted and rolled, not deleted. Empty adopts nothing.
	Selector string
}

const (
	// LabelOrphanedFrom is the label (value: the deleted workload's UID) that
	// MarkOrphan sets on a dependent and Orphaned selects by.
	LabelOrphanedFrom = "sdo.dev/orphaned-from"
	// AnnotationOrphanedOwner and AnnotationOrphanedSelector carry the rest of
	// an OrphanStamp.
	AnnotationOrphanedOwner    = "sdo.dev/orphaned-owner"
	AnnotationOrphanedSelector = "sdo.dev/orphaned-selector"
)

// Propagation is a delete's propagation policy.
type Propagation string

const (
	// PropagationBackground deletes the object and lets the garbage collector
	// delete its dependents (a finished Job's pods).
	PropagationBackground Propagation = "Background"
	// PropagationOrphan deletes the object and keeps its dependents running
	// (a workload's ReplicaSets and pods) for the recreated object to adopt or
	// replace.
	PropagationOrphan Propagation = "Orphan"
)

// Recreator is the part of an Applier that replaces objects whose immutable
// fields changed. An Applier that does not implement it keeps the old
// behavior: the apply fails and the error names the refused objects.
type Recreator interface {
	// ImmutableConflicts server-side dry-run applies documents exactly as
	// Apply would and returns the objects the API server refuses because an
	// immutable field changed. The API server is the authority: no field list
	// is kept here. Any other refusal is not a conflict; Apply reports it.
	ImmutableConflicts(ctx context.Context, documents []byte) ([]ImmutableConflict, error)
	// Get reads a live object; found is false when it does not exist.
	Get(ctx context.Context, ref sdk.ObjectRef) (object LiveObject, found bool, err error)
	// Dependents lists the ReplicaSets, Pods and ControllerRevisions in ref's
	// namespace whose controller is the object with uid. It never lists data
	// (PersistentVolumeClaims).
	Dependents(ctx context.Context, ref sdk.ObjectRef, uid string) ([]LiveObject, error)
	// MarkOrphan stamps a live dependent before its owner is deleted.
	MarkOrphan(ctx context.Context, ref sdk.ObjectRef, stamp OrphanStamp) error
	// Orphaned lists the objects in namespace that carry an OrphanStamp,
	// whatever run stamped them.
	Orphaned(ctx context.Context, namespace string) ([]LiveObject, error)
	// Delete deletes ref with the propagation and returns once it is gone.
	Delete(ctx context.Context, ref sdk.ObjectRef, propagation Propagation) error
}

// ReplicasHandoff is the part of an Applier that keeps a workload's replica
// count when SDO stops applying it to an autoscaled workload.
type ReplicasHandoff interface {
	// HandOffReplicas makes the deployer stop being the sole owner of ref's
	// spec.replicas without changing the value, so omitting the field from the
	// next apply does not delete it (and reset the count to the API default).
	// It does nothing when the object has no replica count or another manager
	// already owns it.
	HandOffReplicas(ctx context.Context, ref sdk.ObjectRef) error
}

// RecreateReason is the recreate policy's typed decision for one conflict.
type RecreateReason string

const (
	// RecreateFinishedJob: a Complete or Failed Job is deleted (background
	// propagation) and created again in its phase.
	RecreateFinishedJob RecreateReason = "finished-job"
	// RecreateJobFinishedAfterWait: a running Job finished within the wait
	// bound and was then recreated.
	RecreateJobFinishedAfterWait RecreateReason = "job-finished-after-wait"
	// RecreateWorkloadOrphan: a pod controller (Deployment, StatefulSet,
	// DaemonSet, ReplicaSet, ...) is deleted with orphan propagation and created again; its
	// orphaned pods are adopted or replaced by the normal rollout.
	RecreateWorkloadOrphan RecreateReason = "workload-orphan-recreate"

	// RefuseJobRunning: a Job is still running after the wait bound. It is
	// never killed mid-run; the deployer decides.
	RefuseJobRunning RecreateReason = "job-still-running"
	// RefuseDataBearing: the object holds data (a PersistentVolumeClaim).
	// SDO never deletes data; a person decides.
	RefuseDataBearing RecreateReason = "data-bearing"
	// RefuseNoPolicy: no recreate policy covers the kind (a Service, whose
	// replacement changes its address; a bare Pod; any other kind). The
	// deployer decides.
	RefuseNoPolicy RecreateReason = "no-recreate-policy"
	// RefuseNotOurs: the live object was not applied by the deployer (it was
	// created by someone else or by hand). SDO deletes only what it can prove
	// it created; the deployer decides (rename, or hand it over).
	RefuseNotOurs RecreateReason = "not-created-by-sdo"
)

// RefusedObject is one conflict the recreate policy did not resolve.
type RefusedObject struct {
	Conflict ImmutableConflict
	Reason   RecreateReason
}

// ImmutableChangeError is a phase whose objects the API server refuses to
// update in place and the recreate policy did not replace. Nothing of the
// phase was deleted or applied.
type ImmutableChangeError struct {
	Phase   string
	Refused []RefusedObject
}

func (e *ImmutableChangeError) Error() string {
	parts := make([]string, 0, len(e.Refused))
	for _, refused := range e.Refused {
		ref := refused.Conflict.Ref
		parts = append(parts, fmt.Sprintf("%s/%s (%s; %s)", ref.Kind, ref.Name, strings.Join(refused.Conflict.Fields, ","), refused.Reason))
	}
	return fmt.Sprintf("phase %q changes immutable fields the deployer does not recreate: %s", e.Phase, strings.Join(parts, "; "))
}

// Cause is data_bearing_change when any refused object holds data, else
// immutable_change.
func (e *ImmutableChangeError) Cause() operability.Cause {
	for _, refused := range e.Refused {
		if refused.Reason == RefuseDataBearing {
			return operability.CauseDataBearingChange
		}
	}
	return operability.CauseImmutableChange
}

// DefaultJobWaitLimit bounds the wait for a running Job whose template changed.
const DefaultJobWaitLimit = 10 * time.Minute

// jobWaitPoll is the interval between reads of a running Job.
const jobWaitPoll = 5 * time.Second

// recreation is one object being recreated in this apply.
type recreation struct {
	record rendered.Recreation
	uid    string // the deleted workload's UID, the orphan stamp's value
}

// pendingDelete is a decided recreate not yet carried out.
type pendingDelete struct {
	ref         sdk.ObjectRef
	conflict    ImmutableConflict
	reason      RecreateReason
	propagation Propagation
	selector    labels.Selector
	uid         string
}

// orphanRecreatable reports a pod controller (rendered.WorkloadKind other
// than a Job, whose pods are its run, or a bare Pod): deleting it with orphan
// propagation keeps its pods serving until the recreated controller adopts or
// replaces them.
func orphanRecreatable(kind string) bool {
	return rendered.WorkloadKind(kind) && kind != "Job" && kind != "Pod"
}

// resolveConflicts runs the recreate policy for one phase before it is
// applied: it asks the API server which objects it refuses for an immutable
// change, decides every conflict and, only when all are resolvable, deletes
// them so the apply that follows creates them again.
func (r *applyRun) resolveConflicts(ctx context.Context, phase rendered.DesiredPhase, documents []byte) error {
	recreator, ok := r.deployer.Applier.(Recreator)
	if !ok {
		return nil
	}
	conflicts, err := recreator.ImmutableConflicts(ctx, documents)
	if err != nil {
		return fmt.Errorf("dry-run phase %q: %w", phase.Name, err)
	}
	var refused []RefusedObject
	var pending []pendingDelete
	for _, conflict := range conflicts {
		object, ok := phaseObject(phase, conflict.Ref, r.deployer.Namespace)
		if !ok {
			continue // not this phase's object: the apply reports it
		}
		conflict.Ref = object.Ref
		decided, reason, err := r.decide(ctx, recreator, object, conflict)
		if err != nil {
			return err
		}
		if decided == nil {
			if reason != "" {
				refused = append(refused, RefusedObject{Conflict: conflict, Reason: reason})
			}
			continue
		}
		pending = append(pending, *decided)
	}
	if len(refused) > 0 {
		return &ImmutableChangeError{Phase: phase.Name, Refused: refused}
	}
	for _, p := range pending {
		if p.propagation == PropagationOrphan {
			if err := r.stampDependents(ctx, recreator, p); err != nil {
				return err
			}
		}
		if err := recreator.Delete(ctx, p.ref, p.propagation); err != nil {
			return fmt.Errorf("delete %s/%s to recreate it: %w", p.ref.Kind, p.ref.Name, err)
		}
		r.recreated = append(r.recreated, &recreation{
			record: rendered.Recreation{
				Ref: p.ref, Fields: slices.Clone(p.conflict.Fields), Propagation: string(p.propagation), Reason: string(p.reason),
			},
			uid: p.uid,
		})
	}
	return nil
}

// stampDependents marks every dependent the delete will orphan, so the sweep
// finds them in any later run. It completes before the delete: a failure here
// leaves the workload in place.
func (r *applyRun) stampDependents(ctx context.Context, recreator Recreator, p pendingDelete) error {
	dependents, err := recreator.Dependents(ctx, p.ref, p.uid)
	if err != nil {
		return fmt.Errorf("list dependents of %s/%s: %w", p.ref.Kind, p.ref.Name, err)
	}
	stamp := OrphanStamp{From: p.uid, Owner: p.ref.Kind + "/" + p.ref.Name}
	if p.selector != nil {
		stamp.Selector = p.selector.String()
	}
	for _, dependent := range dependents {
		if err := recreator.MarkOrphan(ctx, dependent.Ref, stamp); err != nil {
			return fmt.Errorf("mark %s/%s orphaned before deleting %s/%s: %w", dependent.Ref.Kind, dependent.Ref.Name, p.ref.Kind, p.ref.Name, err)
		}
	}
	return nil
}

// decide returns the delete to carry out for a conflict, or the reason it is
// refused (an empty reason: the object is gone and the apply creates it).
func (r *applyRun) decide(ctx context.Context, recreator Recreator, object rendered.DesiredObject, conflict ImmutableConflict) (*pendingDelete, RecreateReason, error) {
	ref := object.Ref
	switch {
	case ref.Kind == "PersistentVolumeClaim":
		return nil, RefuseDataBearing, nil
	case ref.Kind == "Job":
		reason := RecreateFinishedJob
		limit := r.deployer.JobWaitLimit
		if limit <= 0 {
			limit = DefaultJobWaitLimit
		}
		for waited := time.Duration(0); ; waited += jobWaitPoll {
			live, found, err := recreator.Get(ctx, ref)
			if err != nil {
				return nil, "", fmt.Errorf("read %s/%s: %w", ref.Kind, ref.Name, err)
			}
			if !found {
				return nil, "", nil
			}
			if !live.AppliedByDeployer {
				return nil, RefuseNotOurs, nil
			}
			if live.Finished {
				return &pendingDelete{ref: ref, conflict: conflict, reason: reason, propagation: PropagationBackground, uid: live.UID}, "", nil
			}
			if waited >= limit {
				return nil, RefuseJobRunning, nil
			}
			reason = RecreateJobFinishedAfterWait
			if err := r.deployer.sleep(ctx, jobWaitPoll); err != nil {
				return nil, "", err
			}
		}
	case orphanRecreatable(ref.Kind):
		selector, err := objectSelector(object.Object.Object)
		if err != nil {
			return nil, "", fmt.Errorf("%s/%s selector: %w", ref.Kind, ref.Name, err)
		}
		live, found, err := recreator.Get(ctx, ref)
		if err != nil {
			return nil, "", fmt.Errorf("read %s/%s: %w", ref.Kind, ref.Name, err)
		}
		if !found {
			return nil, "", nil
		}
		if !live.AppliedByDeployer {
			return nil, RefuseNotOurs, nil
		}
		return &pendingDelete{ref: ref, conflict: conflict, reason: RecreateWorkloadOrphan, propagation: PropagationOrphan, selector: selector, uid: live.UID}, "", nil
	}
	return nil, RefuseNoPolicy, nil
}

// afterApply removes, right after a StatefulSet recreated by this or an
// earlier run is applied, the orphaned pods its selector does not adopt: they
// hold the ordinal names the recreated StatefulSet must create. Their volumes
// are kept.
func (r *applyRun) afterApply(ctx context.Context) error {
	return r.sweepOrphans(ctx, func(stamp OrphanStamp) bool { return strings.HasPrefix(stamp.Owner, "StatefulSet/") })
}

// replaceRemainingOrphans runs once every workload is observed ready: the
// orphans of Deployments, DaemonSets and ReplicaSets are removed only after
// their replacements serve.
func (r *applyRun) replaceRemainingOrphans(ctx context.Context) error {
	return r.sweepOrphans(ctx, func(OrphanStamp) bool { return true })
}

// sweepOrphans deletes the stamped dependents of deleted workloads that
// want selects, driven by the cluster's stamps and not by this run's memory.
// An orphan is kept while its workload has no replacement, while the
// replacement's selector adopts it and when a controller took it over or it
// was recreated under the same name. The sweep never touches data: stamps are
// only ever set on ReplicaSets, Pods and ControllerRevisions.
func (r *applyRun) sweepOrphans(ctx context.Context, want func(OrphanStamp) bool) error {
	recreator, ok := r.deployer.Applier.(Recreator)
	if !ok {
		return nil // nothing was recreated without one
	}
	orphans, err := recreator.Orphaned(ctx, r.deployer.Namespace)
	if err != nil {
		return fmt.Errorf("list orphaned dependents: %w", err)
	}
	replaced := map[string]bool{} // owner -> its replacement exists
	for _, orphan := range orphans {
		if orphan.Orphan == nil || !want(*orphan.Orphan) {
			continue
		}
		stamp := *orphan.Orphan
		if _, seen := replaced[stamp.Owner+"/"+stamp.From]; !seen {
			ok, err := replacementExists(ctx, recreator, orphan.Ref.Namespace, stamp)
			if err != nil {
				return err
			}
			replaced[stamp.Owner+"/"+stamp.From] = ok
		}
		if !replaced[stamp.Owner+"/"+stamp.From] {
			continue
		}
		if stamp.Selector != "" {
			selector, err := labels.Parse(stamp.Selector)
			if err != nil {
				return fmt.Errorf("orphan %s/%s has an unreadable selector %q: %w", orphan.Ref.Kind, orphan.Ref.Name, stamp.Selector, err)
			}
			if selector.Matches(labels.Set(orphan.Labels)) {
				continue // the replacement adopts it and rolls it
			}
		}
		live, found, err := recreator.Get(ctx, orphan.Ref)
		if err != nil {
			return fmt.Errorf("read orphan %s/%s: %w", orphan.Ref.Kind, orphan.Ref.Name, err)
		}
		if !found || live.UID != orphan.UID || live.Controlled {
			continue
		}
		if err := recreator.Delete(ctx, orphan.Ref, PropagationBackground); err != nil {
			return fmt.Errorf("delete orphan %s/%s: %w", orphan.Ref.Kind, orphan.Ref.Name, err)
		}
		for _, rec := range r.recreated {
			if rec.uid == stamp.From {
				rec.record.Replaced = append(rec.record.Replaced, orphan.Ref.Kind+"/"+orphan.Ref.Namespace+"/"+orphan.Ref.Name)
			}
		}
	}
	return nil
}

// replacementExists reports that the workload an orphan was stamped for has a
// live replacement: the same Kind and name with another UID.
func replacementExists(ctx context.Context, recreator Recreator, namespace string, stamp OrphanStamp) (bool, error) {
	kind, name, ok := strings.Cut(stamp.Owner, "/")
	if !ok {
		return false, nil
	}
	live, found, err := recreator.Get(ctx, sdk.ObjectRef{Kind: kind, Namespace: namespace, Name: name})
	if err != nil {
		return false, fmt.Errorf("read replacement %s: %w", stamp.Owner, err)
	}
	return found && live.UID != stamp.From, nil
}

func (r *applyRun) recreations() []rendered.Recreation {
	var out []rendered.Recreation
	for _, rec := range r.recreated {
		out = append(out, rec.record)
	}
	return out
}

// phaseObject finds the phase object a conflict names. The server's answer
// may omit the namespace; a name shared by two namespaces is ambiguous and
// left to the apply.
func phaseObject(phase rendered.DesiredPhase, ref sdk.ObjectRef, namespace string) (rendered.DesiredObject, bool) {
	var found []rendered.DesiredObject
	for _, object := range phase.Objects {
		if object.Ref.Kind != ref.Kind || object.Ref.Name != ref.Name {
			continue
		}
		if ref.Namespace != "" && object.Ref.Namespace != ref.Namespace {
			continue
		}
		found = append(found, object)
	}
	if len(found) == 1 {
		return found[0], true
	}
	for _, object := range found {
		if object.Ref.Namespace == namespace {
			return object, true
		}
	}
	return rendered.DesiredObject{}, false
}

func objectSelector(object map[string]any) (labels.Selector, error) {
	spec, _ := object["spec"].(map[string]any)
	raw, _ := spec["selector"].(map[string]any)
	if raw == nil {
		return nil, nil // no selector (a CronJob): nothing is adopted by labels
	}
	var selector metav1.LabelSelector
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &selector); err != nil {
		return nil, err
	}
	return metav1.LabelSelectorAsSelector(&selector)
}

func (d Deployer) sleep(ctx context.Context, interval time.Duration) error {
	if d.Sleep != nil {
		return d.Sleep(ctx, interval)
	}
	return deploymentio.Wait(ctx, interval)
}

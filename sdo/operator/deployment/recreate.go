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
}

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
	record   rendered.Recreation
	selector labels.Selector // the recreated workload's selector; nil for Jobs
	orphans  []LiveObject
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
		var orphans []LiveObject
		if p.propagation == PropagationOrphan {
			if orphans, err = recreator.Dependents(ctx, p.ref, p.uid); err != nil {
				return fmt.Errorf("list dependents of %s/%s: %w", p.ref.Kind, p.ref.Name, err)
			}
		}
		if err := recreator.Delete(ctx, p.ref, p.propagation); err != nil {
			return fmt.Errorf("delete %s/%s to recreate it: %w", p.ref.Kind, p.ref.Name, err)
		}
		r.recreated = append(r.recreated, &recreation{
			record: rendered.Recreation{
				Ref: p.ref, Fields: slices.Clone(p.conflict.Fields), Propagation: string(p.propagation), Reason: string(p.reason),
			},
			selector: p.selector, orphans: orphans,
		})
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
		return &pendingDelete{ref: ref, conflict: conflict, reason: RecreateWorkloadOrphan, propagation: PropagationOrphan, selector: selector, uid: live.UID}, "", nil
	}
	return nil, RefuseNoPolicy, nil
}

// afterApply removes, right after a recreated StatefulSet is applied, the
// orphaned pods its selector does not adopt: they hold the ordinal names the
// recreated StatefulSet must create. Their volumes are kept.
func (r *applyRun) afterApply(ctx context.Context) error {
	for _, rec := range r.recreated {
		if rec.record.Ref.Kind == "StatefulSet" {
			if err := r.replaceOrphans(ctx, rec); err != nil {
				return err
			}
		}
	}
	return nil
}

// replaceOrphans deletes the orphaned dependents of a recreated workload that
// its selector does not adopt and that no controller has taken over.
func (r *applyRun) replaceOrphans(ctx context.Context, rec *recreation) error {
	recreator, ok := r.deployer.Applier.(Recreator)
	if !ok {
		return nil // nothing was recreated without one
	}
	for _, orphan := range rec.orphans {
		if rec.selector != nil && rec.selector.Matches(labels.Set(orphan.Labels)) {
			continue // the recreated workload adopts it and rolls it
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
		rec.record.Replaced = append(rec.record.Replaced, orphan.Ref.Kind+"/"+orphan.Ref.Namespace+"/"+orphan.Ref.Name)
	}
	rec.orphans = nil
	return nil
}

// replaceRemainingOrphans runs once every recreated workload is observed
// ready: orphans of Deployments, DaemonSets and ReplicaSets are removed only
// after their replacements serve.
func (r *applyRun) replaceRemainingOrphans(ctx context.Context) error {
	for _, rec := range r.recreated {
		if rec.record.Ref.Kind != "StatefulSet" && len(rec.orphans) > 0 {
			if err := r.replaceOrphans(ctx, rec); err != nil {
				return err
			}
		}
	}
	return nil
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

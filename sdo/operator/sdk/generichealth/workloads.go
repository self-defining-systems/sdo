package generichealth

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	apps "k8s.io/api/apps/v1"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

// Kubernetes allows standard five-field schedules and descriptors; time zones
// are explicit. Loading UTC rather than Local makes replay host independent.
func (e *evaluation) workloads() {
	ns := e.ctx.Namespace()
	for _, v := range e.ctx.StatefulSets() {
		if v.Namespace != ns || v.DeletionTimestamp != nil || v.Generation > v.Status.ObservedGeneration {
			continue
		}
		desired := int32(1)
		if v.Spec.Replicas != nil {
			desired = *v.Spec.Replicas
		}
		unready, created := e.ownedPodTimes("StatefulSet", v.Name, v.CreationTimestamp.Time)
		r := ref("StatefulSet", "apps/v1", &v)
		if v.Status.ReadyReplicas < desired && sdk.HeldPastSelfHeal(e.ctx, unready) {
			e.emit("statefulset-unready", r, fmt.Sprintf("ready=%d desired=%d unready since %s", v.Status.ReadyReplicas, desired, unready.UTC().Format(time.RFC3339)), true)
		}
		// OnDelete and partitioned updates intentionally retain old revisions.
		if v.Spec.UpdateStrategy.Type != apps.OnDeleteStatefulSetStrategyType && (v.Spec.UpdateStrategy.RollingUpdate == nil || v.Spec.UpdateStrategy.RollingUpdate.Partition == nil || *v.Spec.UpdateStrategy.RollingUpdate.Partition == 0) && v.Status.CurrentRevision != "" && v.Status.UpdateRevision != "" && v.Status.CurrentRevision != v.Status.UpdateRevision && sdk.HeldPastSelfHeal(e.ctx, created) {
			e.emit("statefulset-old-revision", r, fmt.Sprintf("current=%s update=%s; no Pod replaced since %s", v.Status.CurrentRevision, v.Status.UpdateRevision, created.UTC().Format(time.RFC3339)), false)
		}
	}
	for _, v := range e.ctx.DaemonSets() {
		if v.Namespace != ns || v.DeletionTimestamp != nil || v.Generation > v.Status.ObservedGeneration {
			continue
		}
		unready, created := e.ownedPodTimes("DaemonSet", v.Name, v.CreationTimestamp.Time)
		r := ref("DaemonSet", "apps/v1", &v)
		if v.Status.NumberReady < v.Status.DesiredNumberScheduled && sdk.HeldPastSelfHeal(e.ctx, unready) {
			e.emit("daemonset-unready", r, fmt.Sprintf("ready=%d desired=%d unready since %s", v.Status.NumberReady, v.Status.DesiredNumberScheduled, unready.UTC().Format(time.RFC3339)), true)
		}
		if v.Spec.UpdateStrategy.Type != apps.OnDeleteDaemonSetStrategyType && v.Status.UpdatedNumberScheduled < v.Status.DesiredNumberScheduled && sdk.HeldPastSelfHeal(e.ctx, created) {
			e.emit("daemonset-old-revision", r, fmt.Sprintf("updated=%d desired=%d; no Pod replaced since %s", v.Status.UpdatedNumberScheduled, v.Status.DesiredNumberScheduled, created.UTC().Format(time.RFC3339)), false)
		}
	}
	for _, v := range e.ctx.Jobs() {
		// A CronJob's failed Job keeps Failed=True until history GC, so it
		// never reconciles; cronjob-overdue covers CronJobs.
		if v.Namespace != ns || v.DeletionTimestamp != nil || ownedBy(v.OwnerReferences, "CronJob") {
			continue
		}
		for _, c := range v.Status.Conditions {
			if c.Type == batch.JobFailed && c.Status == core.ConditionTrue {
				e.emit("job-failed", ref("Job", "batch/v1", &v), fmt.Sprintf("failed=%d reason=%s message=%s", v.Status.Failed, c.Reason, c.Message), false)
			}
		}
	}
	for _, v := range e.ctx.CronJobs() {
		if v.Namespace != ns || v.DeletionTimestamp != nil || v.Spec.Suspend != nil && *v.Spec.Suspend || e.now.IsZero() {
			continue
		}
		zone := "UTC"
		if v.Spec.TimeZone != nil {
			zone = *v.Spec.TimeZone
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			continue
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		schedule, err := parser.Parse(v.Spec.Schedule)
		if err != nil {
			continue
		}
		last := v.CreationTimestamp.Time
		if v.Status.LastSuccessfulTime != nil {
			last = v.Status.LastSuccessfulTime.Time
		}
		if last.IsZero() {
			continue
		}
		first := schedule.Next(last.In(loc))
		second := schedule.Next(first)
		deadline := second.Add(time.Minute)
		if !first.IsZero() && !second.IsZero() && e.now.After(deadline) {
			e.emit("cronjob-overdue", ref("CronJob", "batch/v1", &v), fmt.Sprintf("last success (or creation)=%s schedule=%q timezone=%s two scheduled runs elapsed at %s", last, v.Spec.Schedule, zone, second), false)
		}
	}
}

// ownedPodTimes returns, for the Pods a workload owns, the newest unready
// Pod's Ready transition (the hold of an unready rule) and the newest Pod
// creation (the hold of an old-revision rule: no Pod was replaced since). A
// workload with no unready Pod, or no Pod, falls back to the newest creation
// and then to the workload's own creation.
func (e *evaluation) ownedPodTimes(kind, name string, workloadCreated time.Time) (unready, created time.Time) {
	created = workloadCreated
	for _, p := range e.ctx.Pods() {
		if p.Namespace != e.ctx.Namespace() || p.DeletionTimestamp != nil || !ownedByName(p.OwnerReferences, kind, name) {
			continue
		}
		if p.CreationTimestamp.After(created) {
			created = p.CreationTimestamp.Time
		}
		for _, c := range p.Status.Conditions {
			if c.Type == core.PodReady && c.Status != core.ConditionTrue && c.LastTransitionTime.After(unready) {
				unready = c.LastTransitionTime.Time
			}
		}
	}
	if unready.IsZero() {
		unready = created
	}
	return unready, created
}

func ownedBy(owners []meta.OwnerReference, kind string) bool {
	for _, o := range owners {
		if o.Kind == kind {
			return true
		}
	}
	return false
}

func ownedByName(owners []meta.OwnerReference, kind, name string) bool {
	for _, o := range owners {
		if o.Kind == kind && o.Name == name {
			return true
		}
	}
	return false
}

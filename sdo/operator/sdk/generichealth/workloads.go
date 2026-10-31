package generichealth

import (
	"fmt"
	"github.com/robfig/cron/v3"
	apps "k8s.io/api/apps/v1"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	"time"
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
		old := e.old(v.CreationTimestamp)
		for _, p := range e.ctx.Pods() {
			if p.Namespace != ns {
				continue
			}
			for _, o := range p.OwnerReferences {
				if o.Kind == "StatefulSet" && o.Name == v.Name && !e.old(p.CreationTimestamp) {
					old = false
				}
			}
		}
		if !old {
			continue
		}
		if v.Status.ReadyReplicas < desired {
			e.emit("statefulset-unready", ref("StatefulSet", "apps/v1", &v), fmt.Sprintf("ready=%d desired=%d past %s", v.Status.ReadyReplicas, desired, ProgressWindow), true)
		}
		// OnDelete and partitioned updates intentionally retain old revisions.
		if v.Spec.UpdateStrategy.Type != apps.OnDeleteStatefulSetStrategyType && (v.Spec.UpdateStrategy.RollingUpdate == nil || v.Spec.UpdateStrategy.RollingUpdate.Partition == nil || *v.Spec.UpdateStrategy.RollingUpdate.Partition == 0) && v.Status.CurrentRevision != "" && v.Status.UpdateRevision != "" && v.Status.CurrentRevision != v.Status.UpdateRevision {
			e.emit("statefulset-old-revision", ref("StatefulSet", "apps/v1", &v), fmt.Sprintf("current=%s update=%s past %s", v.Status.CurrentRevision, v.Status.UpdateRevision, ProgressWindow), false)
		}
	}
	for _, v := range e.ctx.DaemonSets() {
		if v.Namespace != ns || v.DeletionTimestamp != nil || v.Generation > v.Status.ObservedGeneration || !e.old(v.CreationTimestamp) {
			continue
		}
		recent := false
		for _, p := range e.ctx.Pods() {
			for _, o := range p.OwnerReferences {
				if p.Namespace == ns && o.Kind == "DaemonSet" && o.Name == v.Name && !e.old(p.CreationTimestamp) {
					recent = true
				}
			}
		}
		if recent {
			continue
		}
		if v.Status.NumberReady < v.Status.DesiredNumberScheduled {
			e.emit("daemonset-unready", ref("DaemonSet", "apps/v1", &v), fmt.Sprintf("ready=%d desired=%d past %s", v.Status.NumberReady, v.Status.DesiredNumberScheduled, ProgressWindow), true)
		}
		if v.Spec.UpdateStrategy.Type != apps.OnDeleteDaemonSetStrategyType && v.Status.UpdatedNumberScheduled < v.Status.DesiredNumberScheduled {
			e.emit("daemonset-old-revision", ref("DaemonSet", "apps/v1", &v), fmt.Sprintf("updated=%d desired=%d past %s", v.Status.UpdatedNumberScheduled, v.Status.DesiredNumberScheduled, ProgressWindow), false)
		}
	}
	for _, v := range e.ctx.Jobs() {
		if v.Namespace != ns || v.DeletionTimestamp != nil {
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

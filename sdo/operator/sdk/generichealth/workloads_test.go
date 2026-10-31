package generichealth_test

import (
	"context"
	apps "k8s.io/api/apps/v1"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/generichealth"
	"testing"
	"time"
)

func TestWorkloadKinds(t *testing.T) {
	for _, rule := range []string{"statefulset-unready", "statefulset-old-revision", "daemonset-unready", "daemonset-old-revision", "job-failed", "cronjob-overdue"} {
		for _, state := range []string{"matching", "healthy", "near-miss"} {
			t.Run(rule+"/"+state, func(t *testing.T) {
				s := healthy()
				m := s.DeploymentList[0].ObjectMeta
				s.StatefulSetList = []apps.StatefulSet{{ObjectMeta: m, Spec: apps.StatefulSetSpec{Replicas: ptr(int32(2))}, Status: apps.StatefulSetStatus{ReadyReplicas: 2, CurrentRevision: "new", UpdateRevision: "new"}}}
				s.DaemonSetList = []apps.DaemonSet{{ObjectMeta: m, Status: apps.DaemonSetStatus{NumberReady: 2, DesiredNumberScheduled: 2, UpdatedNumberScheduled: 2}}}
				s.JobList = []batch.Job{{ObjectMeta: m}}
				s.CronJobList = []batch.CronJob{{ObjectMeta: m, Spec: batch.CronJobSpec{Schedule: "*/5 * * * *"}, Status: batch.CronJobStatus{LastSuccessfulTime: ptr(meta.NewTime(*s.ObservedAtTime))}}}
				if state != "healthy" {
					switch rule {
					case "statefulset-unready":
						s.StatefulSetList[0].Status.ReadyReplicas = 1
					case "statefulset-old-revision":
						s.StatefulSetList[0].Status.CurrentRevision = "old"
					case "daemonset-unready":
						s.DaemonSetList[0].Status.NumberReady = 1
					case "daemonset-old-revision":
						s.DaemonSetList[0].Status.UpdatedNumberScheduled = 1
					case "job-failed":
						s.JobList[0].Status.Conditions = []batch.JobCondition{{Type: batch.JobFailed, Status: core.ConditionTrue}}
					case "cronjob-overdue":
						s.CronJobList[0].Status.LastSuccessfulTime = ptr(meta.NewTime(s.ObservedAtTime.Add(-time.Hour)))
					}
				}
				if state == "near-miss" {
					switch rule {
					case "statefulset-unready":
						s.StatefulSetList[0].Generation = 2
					case "statefulset-old-revision":
						s.StatefulSetList[0].Spec.UpdateStrategy.Type = apps.OnDeleteStatefulSetStrategyType
					case "daemonset-unready":
						s.DaemonSetList[0].CreationTimestamp = meta.NewTime(*s.ObservedAtTime)
					case "daemonset-old-revision":
						s.DaemonSetList[0].Spec.UpdateStrategy.Type = apps.OnDeleteDaemonSetStrategyType
					case "job-failed":
						s.JobList[0].Status.Conditions[0].Status = core.ConditionFalse
					case "cronjob-overdue":
						s.CronJobList[0].Spec.Suspend = ptr(true)
					}
				}
				fs, err := generichealth.NewWorkloads(sdk.DetectorSpec{}).Detect(context.Background(), s)
				if err != nil {
					t.Fatal(err)
				}
				got := false
				for _, f := range fs {
					got = got || f.RuleID == rule
				}
				if got != (state == "matching") {
					t.Fatalf("%+v", fs)
				}
			})
		}
	}
}

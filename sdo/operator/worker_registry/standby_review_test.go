package worker_registry

import (
	"context"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"strings"
	"testing"
)

func TestRWSWarmContinuationCreatesColdJob(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "same", true: "restart"}[restart], func(t *testing.T) {
			p, e, task := standbyFixture(t)
			readyStandby(t, p)
			bound, err := p.TryBind(context.Background(), e, task)
			if err != nil {
				t.Fatal(err)
			}
			if err := bound.publishWarmTask(context.Background(), task); err != nil {
				t.Fatal(err)
			}
			response, _ := anypb.New(&contracts.IncidentResult{IncidentId: "incident", Status: "completed"})
			result := &contracts.AgentResult{SchemaVersion: SchemaVersion, TaskId: task.TaskId, Status: "completed", ProposalId: task.Workspace.ProposalId, Response: response}
			data, err := wire.Canonical(result)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := wire.Canonical(task)
			if err != nil {
				t.Fatal(err)
			}
			if err := bound.ensureRequest(context.Background(), bound.WarmBinding.JobName+"-request", string(payload)); err != nil {
				t.Fatal(err)
			}
			_, err = e.Client.CoreV1().ConfigMaps(e.Namespace).Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: bound.WarmBinding.JobName + "-result"}, Data: map[string]string{ResultDataKey: string(data)}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			job, _ := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), bound.WarmBinding.JobName, metav1.GetOptions{})
			job.Status.Succeeded = 1
			e.Client.BatchV1().Jobs(e.Namespace).UpdateStatus(context.Background(), job, metav1.UpdateOptions{})
			if _, err := bound.Execute(context.Background(), task); err != nil {
				t.Fatal("initial warm result was not recoverable", err)
			}
			file := filepath.Join(e.RepositoryMountPath, "session/startup/initial")
			os.WriteFile(file, []byte("initial"), 0600)
			if restart {
				bound, err = p.TryBind(context.Background(), e, task)
				if err != nil {
					t.Fatal(err)
				}
			}
			next := proto.Clone(task).(*contracts.AgentTask)
			next.TaskId += "-continue-1"
			continued := func() bool {
				name, err := e.taskJobName(context.Background(), next)
				return err == nil && jobExists(e, name)()
			}
			executeUntil(t, continued, func(ctx context.Context) (*contracts.AgentResult, error) { return bound.Execute(ctx, next) })
			name, _ := e.taskJobName(context.Background(), next)
			if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
				t.Fatalf("continuation Job missing: %v", err)
			}
			if got, _ := os.ReadFile(file); string(got) != "initial" {
				t.Fatal("initial clock lost")
			}
		})
	}
}
func TestRWSLateFindingsHasNoWritableAlias(t *testing.T) {
	p, e, _ := standbyFixture(t)
	slot := readyStandby(t, p)
	job, _ := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), slot.JobName, metav1.GetOptions{})
	for _, target := range []string{"/sdo/late-findings/report.json", "/sdo/session/late-findings/report.json"} {
		var selected corev1.VolumeMount
		for _, m := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
			if strings.HasPrefix(target, m.MountPath+"/") && len(m.MountPath) > len(selected.MountPath) {
				selected = m
			}
		}
		if !selected.ReadOnly {
			t.Errorf("writable alias %s", target)
		}
	}
}
func TestRWSUnpublishedExpiredClaimFallsBack(t *testing.T) {
	p, e, task := standbyFixture(t)
	slot := readyStandby(t, p)
	bound, err := p.TryBind(context.Background(), e, task)
	if err != nil {
		t.Fatal(err)
	}
	job, _ := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), slot.JobName, metav1.GetOptions{})
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	e.Client.BatchV1().Jobs(e.Namespace).UpdateStatus(context.Background(), job, metav1.UpdateOptions{})
	replaced := func() bool {
		name, err := e.taskJobName(context.Background(), task)
		return err == nil && name != slot.JobName && jobExists(e, name)()
	}
	executeUntil(t, replaced, func(ctx context.Context) (*contracts.AgentResult, error) { return bound.Execute(ctx, task) })
	if _, err := os.Stat(filepath.Join(slot.Directory, "control", TaskDataKey)); !os.IsNotExist(err) {
		t.Fatal("published to dead worker")
	}
	name, _ := e.taskJobName(context.Background(), task)
	if name == slot.JobName {
		t.Fatal("dead claim retained")
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatal("cold Job missing", err)
	}
}
func TestRWSWarmReceiptIdentityDoesNotRequireRequestConfigMap(t *testing.T) {
	p, e, task := standbyFixture(t)
	readyStandby(t, p)
	bound, err := p.TryBind(context.Background(), e, task)
	if err != nil {
		t.Fatal(err)
	}
	if err := bound.publishWarmTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	for _, action := range e.Client.(*fake.Clientset).Actions() {
		if action.GetVerb() == "update" && action.GetResource().Resource == "jobs" {
			t.Fatal("claim requires absent Jobs update permission")
		}
	}
	name, err := IncidentResponderJobName(context.Background(), e.Client, e.Namespace, "incident")
	if err != nil || name != bound.WarmBinding.JobName {
		t.Fatalf("PVC-backed request identity lost: %q %v", name, err)
	}
}

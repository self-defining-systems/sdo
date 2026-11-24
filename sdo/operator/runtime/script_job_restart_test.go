package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestScriptCancellationRecoversDrainedEvidence(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
		action.(ktesting.CreateAction).GetObject().(*batchv1.Job).UID = "synthetic-job-uid"
		return false, nil, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	drainPolls := 0
	publish := false
	kube.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
		get := action.(ktesting.GetAction)
		obj, err := kube.Tracker().Get(action.GetResource(), action.GetNamespace(), get.GetName())
		if err != nil {
			return false, nil, nil
		}
		if ctx.Err() == nil {
			cancel()
			return true, nil, ctx.Err()
		}
		drainPolls++
		if drainPolls < 2 {
			return false, nil, nil
		}
		job := obj.(*batchv1.Job).DeepCopy()
		if !publish {
			publish = true
			resource := corev1.SchemeGroupVersion.WithResource("configmaps")
			stored, err := kube.Tracker().Get(resource, "control", get.GetName())
			if err != nil {
				t.Fatal(err)
			}
			index := stored.(*corev1.ConfigMap).DeepCopy()
			exit := int32(0)
			raw, _ := json.Marshal(ScriptExecutionResult{ApplicationID: "app-1", JobName: get.GetName(), JobUID: string(job.UID), ScriptDigest: program.ScriptDigest("true"), ExitCode: &exit})
			index.Data["result.json"] = string(raw)
			if err := kube.Tracker().Update(resource, index, "control"); err != nil {
				t.Fatal(err)
			}
			receipt, _ := json.Marshal(ScriptAPIReceipt{Method: "patch", Status: 200, Written: []sdk.ObjectRef{{Kind: "ConfigMap", Name: "changed"}}})
			if err := kube.Tracker().Create(resource, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: get.GetName() + "-api-test", Labels: map[string]string{"sdo.dev/script-capture": get.GetName()}}, Data: map[string]string{"job_uid": string(job.UID), "receipt.json": string(receipt)}}, "control"); err != nil {
				t.Fatal(err)
			}
		}
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
		return true, job, nil
	})
	request := ScriptExecutionRequest{ApplicationID: "app-1", IncidentID: "incident-1", Namespace: "app", Script: "true", Timeout: time.Minute}
	first := TypedPlanClient{Client: kube, ScriptImage: "synthetic", ScriptNamespace: "control", ScriptRepositoryMount: t.TempDir(), ScriptRepositoryPVC: "repo"}
	running, err := first.RunScript(ctx, request)
	if err != nil || running.ExitCode == nil || len(running.Receipts) != 1 || drainPolls < 2 {
		t.Fatalf("cancelled caller lost drained evidence: %+v, %v", running, err)
	}
	creates := 0
	for _, action := range kube.Actions() {
		if action.Matches("create", "jobs") {
			creates++
		}
	}
	if creates != 1 {
		t.Fatalf("created %d Jobs for one application", creates)
	}
}

func TestInterruptedScriptDeletesJobPastShutdownBound(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "script", Namespace: "control", UID: "saved-uid"}}
	kube := fake.NewSimpleClientset(job)
	client := TypedPlanClient{Client: kube, ScriptNamespace: "control"}
	_, err := client.finishInterruptedScript(t.Context(), time.Unix(1, 0), ScriptExecutionResult{JobName: job.Name, JobUID: string(job.UID)})
	if err == nil || !strings.Contains(err.Error(), "effects unknown") {
		t.Fatalf("expired shutdown bound: %v", err)
	}
	for _, action := range kube.Actions() {
		if action.Matches("delete", "jobs") {
			options := action.(ktesting.DeleteAction).GetDeleteOptions()
			if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != job.UID ||
				options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
				t.Fatalf("shutdown must delete the original Job and its pods: %+v", options)
			}
			return
		}
	}
	t.Fatal("stuck Job was not deleted")
}

func TestScriptPrewarmLogsImagePullFailure(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		selector := action.(ktesting.ListAction).GetListRestrictions().Labels
		job, _ := selector.RequiresExactMatch("batch.kubernetes.io/job-name")
		return true, &corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"batch.kubernetes.io/job-name": job}}, Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"}}}},
		}}}}, nil
	})
	var log bytes.Buffer
	prewarmScriptImage(t.Context(), kube, "control", "test-node", "missing-image", slog.New(slog.NewTextHandler(&log, nil)))
	if !strings.Contains(log.String(), "ErrImagePull") || !strings.Contains(log.String(), "continuing") {
		t.Fatalf("prewarm did not report its failure: %s", &log)
	}
}

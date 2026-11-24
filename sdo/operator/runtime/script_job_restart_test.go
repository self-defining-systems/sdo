package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk/program"
)

func TestScriptJobRestartReusesRunningJob(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
		action.(ktesting.CreateAction).GetObject().(*batchv1.Job).UID = "synthetic-job-uid"
		return false, nil, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	kube.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
		// The first GET after creation stands for a controller losing its
		// process while the Job is still active and has published no result.
		get := action.(ktesting.GetAction)
		_, err := kube.Tracker().Get(action.GetResource(), action.GetNamespace(), get.GetName())
		if err == nil && ctx.Err() == nil {
			cancel()
			return true, nil, ctx.Err()
		}
		return false, nil, nil
	})
	request := ScriptExecutionRequest{ApplicationID: "app-1", IncidentID: "incident-1", Namespace: "app", Script: "true", Timeout: time.Minute}
	first := TypedPlanClient{Client: kube, ScriptImage: "synthetic"}
	running, err := first.RunScript(ctx, request)
	if !errors.Is(err, context.Canceled) || running.JobUID != "synthetic-job-uid" {
		t.Fatalf("expected interruption with a reserved Job: %+v, %v", running, err)
	}

	restarted := TypedPlanClient{Client: kube, ScriptImage: "synthetic"}
	maps := kube.CoreV1().ConfigMaps(request.Namespace)
	index, err := maps.Get(t.Context(), running.JobName, metav1.GetOptions{})
	if err != nil || index.Data["result.json"] != "" {
		t.Fatalf("expected unfinished application: %v", err)
	}

	// Publish the result after the restarted client has looked up the
	// still-running Job, at its next index read.
	jobLookedUp := false
	kube.PrependReactor("get", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) {
		jobLookedUp = true
		return false, nil, nil
	})
	exit := int32(0)
	finished := running
	finished.ExitCode = &exit
	finished.ScriptDigest = program.ScriptDigest(request.Script)
	raw, err := json.Marshal(finished)
	if err != nil {
		t.Fatal(err)
	}
	kube.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if !jobLookedUp {
			return false, nil, nil
		}
		index := index.DeepCopy()
		index.Data["result.json"] = string(raw)
		return true, index, nil
	})
	result, err := restarted.RunScript(t.Context(), request)
	if err != nil || result.JobUID != running.JobUID || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("expected existing Job result: %+v, %v", result, err)
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

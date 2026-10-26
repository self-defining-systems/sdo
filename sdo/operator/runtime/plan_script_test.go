package runtime

import (
	"context"
	"encoding/json"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func TestRunScriptJobBoundsRecoveryAndEnvironment(t *testing.T) {
	fake := kubefake.NewSimpleClientset()
	creates := 0
	fake.PrependReactor("create", "jobs", func(a ktesting.Action) (bool, runtime.Object, error) {
		job := a.(ktesting.CreateAction).GetObject().(*batchv1.Job)
		creates++
		job.UID = types.UID("original-job")
		c := job.Spec.Template.Spec.Containers[0]
		if job.Namespace != "app" || job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 || job.Spec.Template.Spec.RestartPolicy != "Never" || job.Spec.Template.Spec.ServiceAccountName != "sdo-script" {
			t.Fatalf("unsafe job: %+v", job.Spec)
		}
		if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds > 60 {
			t.Fatal("deadline not bounded")
		}
		if c.Image != "tools@sha256:fixture" || c.Command[0] != "/usr/local/bin/sdo-operator" {
			t.Fatal("tool image or runner changed")
		}
		found := false
		for _, e := range c.Env {
			if e.Name == "POD" {
				found = true
				if e.Value != "$(echo not-code)" {
					t.Fatal("value interpolated")
				}
			}
		}
		if !found {
			t.Fatal("resolved env missing")
		}
		return false, nil, nil
	})
	client := TypedPlanClient{Client: fake, ScriptImage: "tools@sha256:fixture", ScriptPollInterval: time.Millisecond}
	request := ScriptExecutionRequest{ApplicationID: "durable-application", IncidentID: "i", Namespace: "app", Script: `kubectl exec "$POD" -- true`, Env: map[string]string{"POD": "$(echo not-code)"}, Timeout: time.Minute}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	first, err := client.RunScript(ctx, request)
	cancel()
	if err == nil || first.JobUID != "original-job" || creates != 1 {
		t.Fatalf("first: %+v %v creates=%d", first, err, creates)
	}
	cm, err := fake.CoreV1().ConfigMaps("app").Get(context.Background(), first.JobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	code := int32(0)
	completed := first
	completed.ExitCode = &code
	completed.Stdout = strings.Repeat("x", program.MaxScriptOutputBytes+100)
	completed.Stderr = completed.Stdout
	raw, _ := json.Marshal(completed)
	cm.Data["result.json"] = string(raw)
	_, err = fake.CoreV1().ConfigMaps("app").Update(context.Background(), cm, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := client.RunScript(context.Background(), request)
	if err != nil || creates != 1 || recovered.JobUID != first.JobUID || len(recovered.Stdout) != program.MaxScriptOutputBytes || !recovered.StdoutTruncated || !recovered.StderrTruncated {
		t.Fatalf("recovery: %+v %v creates=%d", recovered, err, creates)
	}
	// A deleted/replaced Job never authorizes a second script invocation.
	cm, _ = fake.CoreV1().ConfigMaps("app").Get(context.Background(), first.JobName, metav1.GetOptions{})
	delete(cm.Data, "result.json")
	_, _ = fake.CoreV1().ConfigMaps("app").Update(context.Background(), cm, metav1.UpdateOptions{})
	_ = fake.BatchV1().Jobs("app").Delete(context.Background(), first.JobName, metav1.DeleteOptions{})
	_, err = client.RunScript(context.Background(), request)
	if err == nil || creates != 1 {
		t.Fatalf("deleted Job re-executed: %v %d", err, creates)
	}
}

func TestRunScriptExecutorResolvesValuesAndRetainsReceipt(t *testing.T) {
	c := &scriptPlanFake{planFakeClient: planFakeClient{}}
	e := PlanExecutor{Client: c, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, Values: program.ValueInputs{Spec: program.ProgramSpec{ID: "repair", Params: []program.ParamSpec{{Name: "pod", ValueKind: program.StringValue, Required: true}}}, Bindings: program.Bindings{Values: map[string]any{"pod": "$(not shell code)"}}}}
	p := program.Plan{Actions: []program.Action{program.RunScript{Script: `kubectl exec "$POD" -- true`, Env: map[string]program.Value{"POD": program.FromParam("pod")}}}}
	r, err := e.Apply(context.Background(), IncidentRequest{IncidentID: "i", Namespace: "app", RepairPolicy: "recorded-actions"}, p, false)
	if err != nil || !r.Applied || c.request.Env["POD"] != "$(not shell code)" || len(r.ActionReports) != 1 || r.ActionReports[0].Script == nil || len(r.ActionReports[0].Values) != 1 {
		t.Fatalf("script result: %+v %v", r, err)
	}
}

type scriptPlanFake struct {
	planFakeClient
	request ScriptExecutionRequest
	failed  bool
}

func (c *scriptPlanFake) RunScript(_ context.Context, r ScriptExecutionRequest) (ScriptExecutionResult, error) {
	c.request = r
	code := int32(0)
	if c.failed {
		code = 1
	}
	return ScriptExecutionResult{Stdout: "private-output-canary", ApplicationID: r.ApplicationID, JobName: "job", JobUID: "uid", ScriptDigest: program.ScriptDigest(r.Script), ExitCode: &code}, nil
}

func TestRunScriptFailureNeverAutomaticallyUndoesAndOutputStaysPrivate(t *testing.T) {
	client := &scriptPlanFake{failed: true}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	plan := program.Plan{Actions: []program.Action{program.RunScript{Script: "exit 1"}}}
	result, err := executor.Apply(context.Background(), IncidentRequest{IncidentID: "failure", Namespace: "app", ResponseDeadline: time.Now().Add(2 * time.Second)}, plan, false)
	if err == nil || !result.Applied || result.RolledBack || result.RollbackError != "" || client.request.Timeout > 2*time.Second {
		t.Fatalf("script failure/budget/undo: %+v %v", result, err)
	}
	receipt, err := result.Proto()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	if strings.Contains(string(raw), "private-output-canary") {
		t.Fatal("private script output reached the durable outcome")
	}
	if result.ActionReports[0].Script.StdoutBytes == 0 || result.ActionReports[0].Script.ResultRef == "" {
		t.Fatal("private output reference missing")
	}
}

func TestRunScriptRawMenuEnvironmentValues(t *testing.T) {
	entries := rawMenuActions()
	entry, ok := entries["RunScript"]
	if !ok {
		t.Fatal("generated RunScript menu missing")
	}
	inputs := program.ValueInputs{}
	plan, err := menuRawPlan(entry.action, map[string]string{"script": `printf '%s' "$POD"`, "env": `{"POD":{"observed":{"ref":"Pod/app/current","path":"metadata.name"}},"FLAGS":"$(not shell code)"}`}, &inputs)
	if err != nil {
		t.Fatal(err)
	}
	script := plan.Actions[0].(program.RunScript)
	if script.Env["POD"].Origin() != program.ObservedValue || script.Env["FLAGS"].Origin() != program.ParameterValue || inputs.Bindings.Values["env_FLAGS"] != "$(not shell code)" {
		t.Fatalf("raw map lost provenance: %+v %+v", script.Env, inputs)
	}
}

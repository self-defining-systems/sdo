package runtime

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk/podtext/podtexttest"
	"sdo.dev/operator/sdk/program"
)

const dollarScript = "psql <<'SQL'\nDO $$ BEGIN PERFORM 1; END $$;\nSQL\necho \"$(SDO_SCRIPT_NAMESPACE) $(date) $\""

// scriptCluster runs a created script Job as the kubelet and the runner would:
// run returns what the runner publishes in the index, given its environment.
func scriptCluster(t *testing.T, run func(env map[string]string, job *batchv1.Job) map[string]string) (TypedPlanClient, *map[string]string) {
	t.Helper()
	kube := fake.NewSimpleClientset()
	received := &map[string]string{}
	kube.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job)
		job.UID = "synthetic-job-uid"
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
		*received, _ = podtexttest.Process(job.Spec.Template.Spec.Containers[0])
		resource := corev1.SchemeGroupVersion.WithResource("configmaps")
		stored, err := kube.Tracker().Get(resource, "control", job.Name)
		if err != nil {
			t.Fatal(err)
		}
		index := stored.(*corev1.ConfigMap).DeepCopy()
		for key, value := range run(*received, job) {
			index.Data[key] = value
		}
		if err := kube.Tracker().Update(resource, index, "control"); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	return TypedPlanClient{Client: kube, ScriptImage: "synthetic", ScriptNamespace: "control", ScriptRepositoryMount: t.TempDir(), ScriptRepositoryPVC: "repo"}, received
}

func deliveryRequest() ScriptExecutionRequest {
	return ScriptExecutionRequest{ApplicationID: "app-1", IncidentID: "incident-1", Namespace: "app", Script: dollarScript, Env: map[string]string{"VALUE": "$$ $(VALUE) $(SDO_SCRIPT_INDEX) $"}, Timeout: time.Minute}
}

func refusedApplication(t *testing.T, run ScriptExecutionResult, runErr error) PlanApplicationResult {
	t.Helper()
	action := program.RunScript{Script: dollarScript, Reason: "synthetic"}
	plan := program.Plan{Actions: []program.Action{action}}
	result := PlanApplicationResult{Plan: plan, ActionReports: newPlanReports(plan)}
	err := result.attachScriptResult(IncidentRequest{Namespace: "app"}, action, 0, run, runErr)
	if err == nil || !strings.Contains(err.Error(), "refused by SDO, not run") || strings.Contains(err.Error(), "unknown") {
		t.Fatalf("refusal reads %v", err)
	}
	report := result.ActionReports[0]
	if result.Applied || len(result.ActionsApplied) > 0 || len(result.Patches) > 0 || report.Status != "refused" || report.FailureReason != ScriptRefusedReason {
		t.Fatalf("refused script recorded as %+v", result)
	}
	return result
}

func TestRefusedScriptIsReportedNotRunAndCostsNoTrust(t *testing.T) {
	const clause = "script digest differs"
	client, _ := scriptCluster(t, func(_ map[string]string, job *batchv1.Job) map[string]string {
		raw, _ := json.Marshal(program.ScriptRefusal{JobUID: string(job.UID), Reason: clause})
		return map[string]string{program.ScriptRefusalKey: string(raw)}
	})
	run, err := client.RunScript(t.Context(), deliveryRequest())
	if run.Refused != clause {
		t.Fatalf("refusal lost: %+v %v", run, err)
	}
	result := refusedApplication(t, run, err)
	app := RecordedPlanApplication{ID: "application-1", ProgramIDs: []string{"fix"}, Result: result, Error: err.Error(), StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0)}
	receipt, encodeErr := recordedPlanContract(app)
	if encodeErr != nil || wire.Validate(receipt) != nil {
		t.Fatalf("receipt cannot persist: %v %v", encodeErr, wire.Validate(receipt))
	}
	if stageReceiptWrote(receipt) {
		t.Fatal("a refused script counts as a write")
	}
	// Even when an earlier action applied and was compensated.
	receipt.Applied, receipt.RolledBack = true, true
	stats := ProgramTrustStats([]*contracts.OutcomeRecord{{Classification: "escalated", SourceCommit: "rev", PlanApplications: []*contracts.PlanApplication{receipt}}})
	if stats["fix"].Failures != 0 {
		t.Fatalf("refusal counted as a playbook failure: %+v", stats)
	}
	loop := ResponderLoop{applicationsLog: []RecordedPlanApplication{app}}
	if rows := loop.incidentWrites(); len(rows) != 0 {
		t.Fatalf("refused script shown as a write: %v", rows)
	}
	for _, repair := range ProgramRepairReceipts(loop.applicationsLog, nil) {
		if repair.Success || strings.Contains(repair.Summary, "unknown") {
			t.Fatalf("refused script repair receipt: %+v", repair)
		}
	}
	line := stageAttemptLine(&contracts.StageMitigation{PlaybookId: "fix", Result: "apply_error", Error: err.Error(), ApplyReceipt: receipt})
	if !strings.Contains(line, "refused by SDO, not run") || !strings.Contains(line, clause) {
		t.Fatalf("agent sees %q", line)
	}
}

// A refusal next to evidence of a run proves nothing.
func TestRefusalWithRunEvidenceStaysUnknown(t *testing.T) {
	client, _ := scriptCluster(t, func(_ map[string]string, job *batchv1.Job) map[string]string {
		refusal, _ := json.Marshal(program.ScriptRefusal{JobUID: string(job.UID), Reason: "late"})
		capture, _ := json.Marshal(ScriptExecutionResult{ApplicationID: "app-1", JobName: job.Name, JobUID: string(job.UID), ScriptDigest: program.ScriptDigest(dollarScript)})
		return map[string]string{program.ScriptRefusalKey: string(refusal), "capture.json": string(capture)}
	})
	run, err := client.RunScript(t.Context(), deliveryRequest())
	if run.Refused != "" || err == nil || !strings.Contains(err.Error(), "effects unknown") {
		t.Fatalf("refusal hid a run: %+v %v", run, err)
	}
}

func TestScriptRejectedByTheAPIIsNotRun(t *testing.T) {
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("create", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) {
		return true, nil, apierrors.NewForbidden(batchv1.Resource("jobs"), "script", nil)
	})
	client := TypedPlanClient{Client: kube, ScriptImage: "synthetic", ScriptNamespace: "control", ScriptRepositoryMount: t.TempDir(), ScriptRepositoryPVC: "repo"}
	run, err := client.RunScript(t.Context(), deliveryRequest())
	if run.Refused == "" {
		t.Fatalf("rejected Job reported as possibly run: %+v %v", run, err)
	}
	refusedApplication(t, run, err)
}

func TestUndeliverableScriptIsRefusedBeforeAnyJob(t *testing.T) {
	for _, mutate := range []func(*ScriptExecutionRequest){
		func(r *ScriptExecutionRequest) { r.Script = "echo \xff" },
		func(r *ScriptExecutionRequest) { r.Env["VALUE"] = "\xff" },
	} {
		client, received := scriptCluster(t, func(map[string]string, *batchv1.Job) map[string]string { return nil })
		request := deliveryRequest()
		mutate(&request)
		run, err := client.RunScript(t.Context(), request)
		if len(*received) != 0 {
			t.Fatal("an undeliverable script started a Job")
		}
		refusedApplication(t, run, err)
	}
	if (program.RunScript{Script: "echo \xff"}).Validate() == nil {
		t.Fatal("plan validation, which memory check and test run, accepts an undeliverable script")
	}
}

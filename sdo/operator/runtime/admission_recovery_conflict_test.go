package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/workloadhealth"
)

// API bookkeeping advances even when a patch leaves configuration unchanged.
type admissionReadbackClient struct{ TypedPlanClient }

func (c admissionReadbackClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if err := c.TypedPlanClient.Patch(ctx, ref, kind, data); err != nil {
		return err
	}
	if ref.Kind == "ReplicaSet" {
		set, err := c.Client.AppsV1().ReplicaSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		set.ResourceVersion += "-next"
		_, err = c.Client.AppsV1().ReplicaSets(ref.Namespace).Update(ctx, set, metav1.UpdateOptions{})
		return err
	}
	return nil
}

// Both repairs really write the retry annotation. Requeueing a controller is
// not ownership of its desired state: the prerequisite repair must be able to
// requeue it again while the template repair waits for admission.
func TestAdmissionRecoveryCanRequeuePendingTemplateRepair(t *testing.T) {
	for _, scope := range []string{"requeue", "unchanged-object"} {
		t.Run(scope, func(t *testing.T) {
			ctx := context.Background()
			loop, _ := pipelineTestLoop(t, false)
			var saved []byte
			loop.saveToolState = func(_ context.Context, data []byte) error { saved = append([]byte(nil), data...); return nil }
			rollouts := admissionRolloutsFor("api")
			kube := rollouts.Client
			d, _ := kube.AppsV1().Deployments("app").Get(ctx, "api", metav1.GetOptions{})
			d.ResourceVersion = "1"
			d.Spec.Template.Spec.Containers = []corev1.Container{{Name: "app", Image: "bad:v1"}}
			if _, err := kube.AppsV1().Deployments("app").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			rs, _ := kube.AppsV1().ReplicaSets("app").Get(ctx, "frontend-next", metav1.GetOptions{})
			rs.ResourceVersion = "1"
			rs.Annotations = map[string]string{"unchanged": "yes"}
			rs.Generation, rs.Status.ObservedGeneration = 2, 2
			rs.Spec.Template = *d.Spec.Template.DeepCopy()
			rs.Spec.Template.Spec.Containers[0].Image = "good:v1"
			rs.Status.Conditions = []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: `failed calling webhook "validate.example.test": x509: certificate signed by unknown authority`}}
			if _, err := kube.AppsV1().ReplicaSets("app").Update(ctx, rs, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			config, _ := kube.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, "app-policy", metav1.GetOptions{})
			config.UID, config.ResourceVersion = "policy", "1"
			config.Webhooks[0].ClientConfig.CABundle = []byte("broken-ca")
			if _, err := kube.CoreV1().Secrets("app").Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "serving", Namespace: "app"}, Data: map[string][]byte{"ca.crt": []byte("synthetic-ca")}}, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := kube.AdmissionregistrationV1().ValidatingWebhookConfigurations().Update(ctx, config, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			loop.Executor = PlanExecutor{Client: admissionReadbackClient{TypedPlanClient{Client: kube}}, Rollouts: rollouts, Verifier: planFakeVerifier{true}, RolloutTimeout: time.Second}
			deployment := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			retry := program.RetryAdmission{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: rs.Name}}
			var initial program.Action = retry
			if scope == "unchanged-object" {
				initial = program.SetField{Ref: retry.Ref, Path: "spec.template.spec.containers[0].image", Value: testPlanValue("good:v1"), Reason: "already restored"}
			}
			loop.plans["template"] = selectedPlan{plan: program.Plan{Actions: []program.Action{program.PatchObject{Ref: deployment, Type: "json", Patch: json.RawMessage(`[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"good:v1"}]`), Reason: "restore template"}, initial}}}
			loop.plans["prerequisite"] = selectedPlan{plan: program.Plan{Actions: []program.Action{program.SyncWebhookCA{Target: program.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: config.Name}, Webhook: "validate.example.test", SecretNamespace: "app", SecretName: "serving", SecretKey: "ca.crt"}, Reason: "restore admission"}, retry}}}
			first := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{"template"}})
			if first.RolloutPending.GetReasonCode() != "admission_blocked" {
				t.Fatalf("template did not encounter admission failure: %+v", first)
			}
			// Rejoining a durable attempt must not turn observation targets back into locks.
			if err := loop.restoreToolSnapshot(saved); err != nil {
				t.Fatal(err)
			}
			second := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{"prerequisite"}})
			if second.ApplicationID == "" || strings.Contains(second.Error, "conflicts with pending") {
				t.Fatalf("prerequisite repair refused: %+v", second)
			}
			got, _ := kube.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, config.Name, metav1.GetOptions{})
			if string(got.Webhooks[0].ClientConfig.CABundle) != "synthetic-ca" {
				t.Fatalf("prerequisite write did not land: %+v", second)
			}
			// The desired template remains reserved until independently observed healthy.
			loop.plans["overlap"] = selectedPlan{plan: program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: deployment, Replicas: testPlanValue(2)}}}}
			overlap := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{"overlap"}})
			if !strings.Contains(overlap.Error, "conflicts with pending") {
				t.Fatalf("desired-state reservation lost: %+v", overlap)
			}
		})
	}
}

// Feed the real admission detector into the stage candidate consumer. Program
// names and coverage order convey no dependency; the typed finding does.
func TestCoveredAdmissionPrerequisitePrecedesWorkloadRepairs(t *testing.T) {
	for _, id := range []string{"aaa-repair", "zzz-repair"} {
		detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "renamed-detector", Playbooks: []string{id}})
		findings, err := detector.Detect(context.Background(), sdktest.Snapshot{NamespaceName: "app", Admission: &sdk.PodAdmissionResult{Namespace: "app", Rejection: `failed calling webhook "arbitrary.example": x509: certificate signed by unknown authority`}})
		if err != nil || len(findings) != 1 {
			t.Fatalf("admission findings: %v %v", findings, err)
		}
		blocked := sdk.Finding{DetectorID: "templates", RuleID: "drift", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}, Playbooks: []string{"template"}}
		findings = append([]sdk.Finding{blocked}, findings...)
		runs := []ProgramRun{}
		matches := []*contracts.PlaybookMatch{}
		for index, name := range []string{"template", id} {
			runs = append(runs, ProgramRun{ProgramID: name, FindingFingerprint: FindingFingerprint(findings[index]), Bindings: program.Bindings{}, Verdict: program.Verdict{Result: program.Match}})
			matches = append(matches, &contracts.PlaybookMatch{PlaybookId: name})
		}
		state := &IncidentState{Request: IncidentRequest{Findings: findings, InitialProgramRuns: runs}, Findings: findings, Coverage: &contracts.PlaybookCoverage{Matches: matches}}
		got := stageCandidates(state)
		if len(got) != 2 || got[0].PlaybookId != id {
			t.Fatalf("admission prerequisite was not first: %v", got)
		}
	}
}

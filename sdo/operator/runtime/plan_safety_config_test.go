package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

func TestPlanSafetyReviewedControllerConfig(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	object := planObject(ref, nil)
	object.SetUID("fixture-uid")
	delete(object.Object, "spec")
	object.Object["data"] = map[string]any{"key": "before"}
	after := object.DeepCopy()
	after.Object["data"].(map[string]any)["key"] = "after"
	doc := ReviewedPlanSafetyDocument{Version: 1, Authority: "reviewed-fixture", ReviewID: "synthetic-review", SourceCommit: "accepted", ExpiresAt: time.Now().Add(time.Hour), Objects: []ReviewedPlanObject{{Ref: ref, UID: "fixture-uid", Digests: []string{planSafetyDigest(object), planSafetyDigest(after)}}}}
	raw, _ := json.Marshal(doc)
	digest := sha256.Sum256(raw)
	cfg := &contracts.ControllerConfig{PlanSafetyPolicyJson: string(raw), PlanSafetyPolicySha256: hex.EncodeToString(digest[:])}
	policy, err := loadReviewedPlanSafety(cfg.PlanSafetyPolicyJson, cfg.PlanSafetyPolicySha256, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.authorizeRead(ref, "accepted", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := policy.checkObject(object, ref); err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
	executor := PlanExecutor{Client: client, Safety: policy, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[key]", Value: testPlanValue("after")}}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("configured structured action unavailable: %v", err)
	}
	args := strings.Join(controllerConfigToArgs(cfg), " ")
	if !strings.Contains(args, "--plan-safety-policy-json") || !strings.Contains(args, cfg.PlanSafetyPolicySha256) {
		t.Fatal("controller config dropped authority")
	}
	for _, bad := range []string{`{"Plan":{"Actions":[]},"DryRun":true}`, strings.Replace(string(raw), "reviewed-fixture", "dry-run", 1)} {
		sum := sha256.Sum256([]byte(bad))
		if _, err := loadReviewedPlanSafety(bad, hex.EncodeToString(sum[:]), time.Now()); err == nil {
			t.Fatal("dry-run output accepted as authority")
		}
	}
	if _, err := loadReviewedPlanSafety(string(raw), strings.Repeat("0", 64), time.Now()); err == nil {
		t.Fatal("unreviewed replacement accepted")
	}
}

package install_test

import (
	"crypto/sha256"
	"encoding/hex"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli"
	"strings"
	"testing"
)

func TestOperateJobPreservesReviewedPlanAuthority(t *testing.T) {
	raw := `{"version":1}`
	sum := sha256.Sum256([]byte(raw))
	hash := hex.EncodeToString(sum[:])
	cfg := install.Config{Namespace: "app", Application: "app", ControllerImage: "controller:test", ResponderImage: "agent:test", ValidatorImage: "validator:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "model", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", PlanSafetyPolicyJSON: raw, PlanSafetyPolicySHA256: hash, PlanWebhookNames: []string{"fixture.example.invalid"}}
	job, err := install.OperateJob(cfg, "healthy", "synthetic-fixture", 1)
	if err != nil {
		t.Fatal(err)
	}
	launch, err := operatorcli.ParseOperateLaunch(job.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	if launch.Install.PlanSafetyPolicyJSON != raw || launch.Install.PlanSafetyPolicySHA256 != hash || len(launch.Install.PlanWebhookNames) != 1 {
		t.Fatal("operate job dropped reviewed authority")
	}
	// The lifecycle supplies the accepted commit after deployment.
	launch.Install.AcceptedCommit = cfg.AcceptedCommit
	plan, err := install.Resources(launch.Install)
	if err != nil {
		t.Fatal(err)
	}
	controller, _, err := operatorcli.ParseControllerLaunch(plan.Job.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	if controller.Controller.PlanSafetyPolicyJson != raw || controller.Controller.PlanSafetyPolicySha256 != hash {
		t.Fatal("controller child dropped authority")
	}
	cfg.PlanSafetyPolicySHA256 = strings.Repeat("0", 64)
	if _, err := install.Resources(cfg); err == nil {
		t.Fatal("installer accepted bytes outside reviewer pin")
	}
}

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
	sourceCfg := cfg
	sourceCfg.PlanSafetyPolicyJSON = ""
	sourceCfg.PlanSafetyPolicySHA256 = ""
	sourceCfg.PlanSourcePolicyJSON = `{"version":1,"authority":"accepted-source-and-incident","applications":{"app":"app"},"field_repairs":"accepted-source-only","create_kinds":["v1/ConfigMap"],"delete_kinds":["networking.k8s.io/v1/NetworkPolicy"],"ttl_seconds":3600}`
	sourceJob, err := install.OperateJob(sourceCfg, "healthy", "source-fixture", 1)
	if err != nil {
		t.Fatal(err)
	}
	sourceLaunch, err := operatorcli.ParseOperateLaunch(sourceJob.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	sourceLaunch.Install.AcceptedCommit = cfg.AcceptedCommit
	sourcePlan, err := install.Resources(sourceLaunch.Install)
	if err != nil {
		t.Fatal(err)
	}
	sourceController, _, err := operatorcli.ParseControllerLaunch(sourcePlan.Job.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	if sourceController.Controller.PlanSourcePolicyJson != sourceCfg.PlanSourcePolicyJSON {
		t.Fatal("controller lost source policy authority")
	}

	cfg.PlanSafetyPolicySHA256 = strings.Repeat("0", 64)
	if _, err := install.Resources(cfg); err == nil {
		t.Fatal("installer accepted bytes outside reviewer pin")
	}
}

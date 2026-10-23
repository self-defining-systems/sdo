package operatorcli

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sdo.dev/operator/install"
	"strings"
	"testing"
)

func TestPlanReviewedAuthoritySurvivesEveryProductionLaunch(t *testing.T) {
	raw := `{"version":1}`
	sum := sha256.Sum256([]byte(raw))
	hash := hex.EncodeToString(sum[:])
	common := []string{"--namespace", "app", "--application", "app", "--controller-image", "controller:test", "--responder-image", "agent:test", "--validator-image", "validator:test", "--repository-pvc", "repository", "--credentials-secret", "agent-credentials", "--model", "model"}
	for _, command := range []string{"host", "operate", "install-existing"} {
		t.Run(command, func(t *testing.T) {
			args := append([]string{command}, common...)
			switch command {
			case "host":
				args = append(args, "--repository", "/tmp/app", "--health-objective", "healthy", "--operation-id", "fixture", "--buildkit-image", "buildkit@sha256:"+strings.Repeat("a", 64), "--registry-push-secret", "registry-push")
			case "operate":
				args = append(args, "--app", "/workspace/application", "--repository-mount-path", "/workspace", "--worktree-root", "/workspace/worktrees", "--health-objective", "healthy", "--operation-id", "fixture", "--buildkit-image", "buildkit@sha256:"+strings.Repeat("a", 64), "--registry-push-secret", "registry-push")
			default:
				args = append(args, "--repository", "/tmp/app")
			}
			args = append(args, "--plan-safety-policy-json", raw, "--plan-safety-policy-sha256", hash, "--plan-webhook-name", "fixture.example.invalid")
			var c install.Config
			var err error
			switch command {
			case "host":
				var launch HostConfig
				launch, err = ParseHostLaunch(args)
				c = launch.Install
			case "operate":
				var launch OperateConfig
				launch, err = ParseOperateLaunch(args)
				c = launch.Install
			default:
				var launch InstallExistingConfig
				launch, err = ParseInstallExistingLaunch(args)
				c = launch.Install
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.PlanSafetyPolicyJSON != raw || c.PlanSafetyPolicySHA256 != hash || len(c.PlanWebhookNames) != 1 || c.PlanWebhookNames[0] != "fixture.example.invalid" {
				t.Fatal("reviewed authority or exact-name webhook grant lost")
			}
			policyPath, err := filepath.Abs("../config/plan-policies/mixed10.json")
			if err != nil {
				t.Fatal(err)
			}
			policyBytes, err := os.ReadFile(policyPath)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("SDO_PLAN_SOURCE_POLICY_FILE", policyPath)
			sourceArgs := args[:indexOf(args, "--plan-safety-policy-json")]
			switch command {
			case "host":
				launch, e := ParseHostLaunch(sourceArgs)
				c = launch.Install
				err = e
			case "operate":
				launch, e := ParseOperateLaunch(sourceArgs)
				c = launch.Install
				err = e
			default:
				launch, e := ParseInstallExistingLaunch(sourceArgs)
				c = launch.Install
				err = e
			}
			if err != nil || c.PlanSourcePolicyJSON != string(policyBytes) {
				t.Fatalf("committed source policy file did not reach launch: %v", err)
			}

		})
	}
}

package install

import (
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sdo.dev/operator/sdk/actiongrants"
)

func ruleGrants(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	has := func(values []string, want string) bool {
		for _, value := range values {
			if value == want || value == "*" {
				return true
			}
		}
		return false
	}
	for _, rule := range rules {
		if has(rule.APIGroups, group) && has(rule.Resources, resource) && has(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

// A program plan is denied at run time when the controller Role lacks a verb
// its action performs. actiongrants.ActionPermissions is the vocabulary's own
// statement of those verbs, so the Role of both install layouts must hold all
// of them and no action may be added without an entry.
func TestControllerRoleHoldsEveryProgramActionPermission(t *testing.T) {
	split := Config{Namespace: "app", ControlNamespace: "app-sdo", Application: "app", Model: "gpt-fixture", TimeoutSeconds: 60, ControllerImage: "controller:fixture", ResponderImage: "responder:fixture", ValidatorImage: "validator:fixture", RepositoryPVC: "repository", CredentialsSecret: "fixture", AcceptedCommit: strings.Repeat("a", 40), NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryPushDockerConfig: []byte("{}")}
	single := split
	single.ControlNamespace = ""
	for name, cfg := range map[string]Config{"split namespaces": split, "single namespace": single} {
		plan, err := Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		var controller []rbacv1.PolicyRule
		for _, role := range append(append([]*rbacv1.Role{}, plan.AppRoles...), plan.ControlRoles...) {
			if role.Name == "sdo-controller" && role.Namespace == cfg.Namespace {
				controller = role.Rules
			}
		}
		if len(controller) == 0 {
			t.Fatalf("%s: no controller Role in the application namespace", name)
		}
		for action, entry := range actiongrants.ActionPermissions {
			for _, need := range entry.Needs {
				for _, verb := range need.Verbs {
					if !ruleGrants(controller, need.Group, need.Resource, verb) {
						t.Errorf("%s: %s needs %s on %s/%s but the controller Role lacks it", name, action, verb, need.Group, need.Resource)
					}
				}
			}
		}
	}
}

package install

import (
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sdo.dev/operator/sdk/actiongrants"
)

func ruleGrants(rules []rbacv1.PolicyRule, group, resource, verb string) bool {
	for _, rule := range rules {
		if slices.Contains(rule.APIGroups, group) && slices.Contains(rule.Resources, resource) && slices.Contains(rule.Verbs, verb) {
			return true
		}
	}
	return false
}

// boundRules are the rules the identity holds in namespace through Roles
// bound to it by a RoleBinding there, whichever layout produced them.
func boundRules(plan Plan, identity, controlNamespace, namespace string) []rbacv1.PolicyRule {
	var rules []rbacv1.PolicyRule
	roles := append(append([]*rbacv1.Role{}, plan.AppRoles...), plan.ControlRoles...)
	for _, b := range append(append([]*rbacv1.RoleBinding{}, plan.AppBindings...), plan.ControlBindings...) {
		if b.Namespace != namespace || b.RoleRef.Kind != "Role" {
			continue
		}
		bound := false
		for _, subject := range b.Subjects {
			bound = bound || subject.Kind == "ServiceAccount" && subject.Name == identity && subject.Namespace == controlNamespace
		}
		for _, role := range roles {
			if bound && role.Namespace == namespace && role.Name == b.RoleRef.Name {
				rules = append(rules, role.Rules...)
			}
		}
	}
	return rules
}

// A program plan is denied at run time when the identity that performs an
// action lacks a verb it uses. actiongrants.ActionPermissions is the
// vocabulary's own statement of those verbs and that identity, so the Roles
// bound to the identity in both install layouts must hold all of them, hold
// no wildcard that would hide a missing one, and no action may be added
// without an entry.
func TestControllerRoleHoldsEveryProgramActionPermission(t *testing.T) {
	split := Config{Namespace: "app", ControlNamespace: "app-sdo", Application: "app", Model: "gpt-fixture", TimeoutSeconds: 60, ControllerImage: "controller:fixture", ResponderImage: "responder:fixture", ValidatorImage: "validator:fixture", RepositoryPVC: "repository", CredentialsSecret: "fixture", AcceptedCommit: strings.Repeat("a", 40), NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryPushDockerConfig: []byte("{}")}
	single := split
	single.ControlNamespace = ""
	for name, cfg := range map[string]Config{"split namespaces": split, "single namespace": single} {
		plan, err := Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for action, entry := range actiongrants.ActionPermissions {
			held := boundRules(plan, entry.Identity, cfg.controlNamespace(), cfg.Namespace)
			if len(held) == 0 {
				t.Fatalf("%s: %s is performed by %s, which holds no Role in namespace %s", name, action, entry.Identity, cfg.Namespace)
			}
			for _, need := range entry.Needs {
				for _, verb := range need.Verbs {
					if !ruleGrants(held, need.Group, need.Resource, verb) {
						t.Errorf("%s: %s needs %s on %s/%s as %s but its Roles lack it", name, action, verb, need.Group, need.Resource, entry.Identity)
					}
				}
			}
		}
		for _, rule := range boundRules(plan, actiongrants.Controller, cfg.controlNamespace(), cfg.Namespace) {
			if slices.Contains(rule.Verbs, "*") || slices.Contains(rule.Resources, "*") {
				t.Errorf("%s: the controller holds a wildcard rule %v, which would hide a missing grant from this ratchet", name, rule)
			}
		}
	}
}

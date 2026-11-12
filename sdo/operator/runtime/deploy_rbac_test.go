package runtime

import (
	"bytes"
	"os"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// The controller's one write on Secrets is the restore of a Secret from
// accepted source: a merge patch, with no create, update, delete or list-and-
// write beyond the read the informer already has. The responder has none.
func TestControllerRoleGrantsExactlyOneSecretWrite(t *testing.T) {
	raw, err := os.ReadFile("deploy/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]rbacv1.Role{}
	for _, document := range bytes.Split(raw, []byte("\n---")) {
		var role rbacv1.Role
		if err := yaml.Unmarshal(document, &role); err != nil || role.Kind != "Role" {
			continue
		}
		roles[role.Name] = role
	}
	var writes, reads []string
	for _, rule := range roles["sdo-controller"].Rules {
		if !slices.Contains(rule.Resources, "secrets") {
			continue
		}
		if len(rule.Resources) != 1 || len(rule.ResourceNames) != 0 {
			t.Fatalf("a Secret rule shares its resources or names: %+v", rule)
		}
		for _, verb := range rule.Verbs {
			if verb == "get" || verb == "list" || verb == "watch" {
				reads = append(reads, verb)
			} else {
				writes = append(writes, verb)
			}
		}
	}
	if !slices.Equal(writes, []string{"patch"}) || len(reads) != 3 {
		t.Fatalf("controller Secret verbs: writes %v, reads %v; want one patch and get/list/watch", writes, reads)
	}
	for _, rule := range roles["sdo-responder"].Rules {
		if slices.Contains(rule.Resources, "secrets") {
			t.Fatalf("the responder role grants Secrets: %+v", rule)
		}
	}
}

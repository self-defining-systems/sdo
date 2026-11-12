package runtime

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/rendered"
)

func inventoryObject(apiVersion, kind, name string) *unstructured.Unstructured {
	o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": apiVersion, "kind": kind}}
	o.SetName(name)
	o.SetNamespace("app")
	return o
}

// The objects the deployer applies in hook phases are accepted, not strangers:
// the drift inventory must not report them as absent from accepted source
// whether the hook's delete policy kept them or not, nor demand them when the
// policy removed them. A stranger is still reported.
func TestAcceptedInventoryTreatsHookObjectsAsAcceptedButOptional(t *testing.T) {
	declared := inventoryObject("apps/v1", "Deployment", "web")
	declared.Object["spec"] = map[string]any{"replicas": int64(1)}
	hookJob := inventoryObject("batch/v1", "Job", "setup-a1b2c3d")
	hooks := []*unstructured.Unstructured{
		hookJob,
		inventoryObject("v1", "ConfigMap", "setup"),
		inventoryObject("v1", "ServiceAccount", "setup"),
		inventoryObject("rbac.authorization.k8s.io/v1", "Role", "setup"),
		inventoryObject("rbac.authorization.k8s.io/v1", "RoleBinding", "setup"),
		inventoryObject("batch/v1", "Job", "gone-by-policy"),
	}
	index := NewSourceInventoryFrom("app", "rev", rendered.Inventory{Objects: []*unstructured.Unstructured{declared}, Hooks: hooks})
	pod := inventoryObject("v1", "Pod", "setup-a1b2c3d-xyz")
	pod.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "setup-a1b2c3d"}})
	stranger := inventoryObject("v1", "ConfigMap", "someone-elses")
	live := []*unstructured.Unstructured{declared, hooks[0], hooks[1], hooks[2], hooks[3], hooks[4], pod, stranger}

	findings := index.Findings(live, time.Unix(0, 0))
	var got []string
	for _, f := range findings {
		got = append(got, f.Summary)
	}
	if len(findings) != 1 || !strings.Contains(findings[0].Summary, "ConfigMap/someone-elses") {
		t.Fatalf("findings = %v, want only the stranger", got)
	}
}

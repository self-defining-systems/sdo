package sdk_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func healthyPlatform() sdk.PlatformObservation {
	return sdk.PlatformObservation{
		Namespace: metav1.ObjectMeta{Name: "app", Labels: map[string]string{"kubernetes.io/metadata.name": "app"}},
		RoleList: []rbacv1.Role{{ObjectMeta: metav1.ObjectMeta{Name: "reader", Namespace: "app"},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}}}},
		Inventory: []sdk.ObjectRef{
			{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"},
			{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role", Namespace: "app", Name: "reader"},
			{APIVersion: "example.dev/v1", Kind: "Widget", Namespace: "app", Name: "w"},
		},
	}
}

func TestPlatformIsUnavailableUnlessObserved(t *testing.T) {
	if _, err := sdk.Platform(sdktest.Snapshot{NamespaceName: "app"}); err == nil {
		t.Fatal("an unobserved platform must be an error")
	}
	observed := healthyPlatform()
	observed.Errors = map[string]string{sdk.PlatformLimitRanges: "forbidden"}
	platform, err := sdk.Platform(sdktest.Snapshot{NamespaceName: "app", Platform: &observed})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := platform.LimitRanges(); err == nil {
		t.Fatal("an unreadable part must be an error, never an empty list")
	}
	if _, err := platform.Roles(); err != nil {
		t.Fatal(err)
	}
}

func TestPlatformChangesAgainstBaseline(t *testing.T) {
	base := healthyPlatform()
	now := healthyPlatform()
	if _, ok := now.Changes(); ok {
		t.Fatal("no baseline yet must say so")
	}
	now.Namespace.Labels = map[string]string{"kubernetes.io/metadata.name": "app", "pod-security.kubernetes.io/enforce": "restricted"}
	now.RoleList[0].Rules[0].Verbs = nil
	now.LimitRangeList = []corev1.LimitRange{{ObjectMeta: metav1.ObjectMeta{Name: "container-limits", Namespace: "app"},
		Spec: corev1.LimitRangeSpec{Limits: []corev1.LimitRangeItem{{Type: corev1.LimitTypeContainer, Max: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Mi")}}}}}}
	now.Webhooks = []sdk.AdmissionWebhook{{Configuration: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "pod-defaults"}, Webhook: "pods.example", FailurePolicy: "Ignore"}}
	now.Baseline = &base
	changes, ok := now.Changes()
	if !ok {
		t.Fatal("baseline present")
	}
	want := map[string]string{
		"LimitRange/container-limits/":                                     sdk.PlatformAdded,
		"MutatingWebhookConfiguration/pod-defaults/webhooks[pods.example]": sdk.PlatformAdded,
		"Namespace/app/labels.pod-security.kubernetes.io/enforce":          sdk.PlatformAdded,
		"Role/reader/": sdk.PlatformModified,
	}
	if len(changes) != len(want) {
		t.Fatalf("changes = %+v", changes)
	}
	for _, change := range changes {
		key := change.Ref.Kind + "/" + change.Ref.Name + "/" + change.Field
		if want[key] != change.Change {
			t.Errorf("unexpected change %s = %s", key, change.Change)
		}
	}
}

func TestPlatformMissingFromLiveNamesDeletedSourceObjects(t *testing.T) {
	observed := healthyPlatform()
	observed.RoleList = nil
	snapshot := sdktest.Snapshot{NamespaceName: "app", Platform: &observed}
	missing, err := observed.MissingFromLive(snapshot, "Service")
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 2 || missing[0].Kind != "Service" || missing[1].Kind != "Role" {
		t.Fatalf("missing = %+v (kinds neither view holds are not judged)", missing)
	}
	snapshot.ServiceList = []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}
	if missing, _ := observed.MissingFromLive(snapshot, "Service"); len(missing) != 1 {
		t.Fatalf("missing = %+v", missing)
	}
}

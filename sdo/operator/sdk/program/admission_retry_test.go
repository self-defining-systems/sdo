package program_test

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestAdmissionFailedCreateClass(t *testing.T) {
	replicas := int32(1)
	for _, message := range []string{`admission webhook "policy.example" denied the request`, `failed calling webhook "policy.example": service not found`, `failed to call webhook: TLS x509 unknown authority`, `denied by policy`, `admission policy denied`, `exceeded quota`, `FailedScheduling`, `ImagePullBackOff`, `webhook backend healthy`} {
		for _, state := range []string{"blocked", "healthy", "scaled-down", "terminating", "cleared", "wrong-condition", "wrong-reason"} {
			t.Run(message+"/"+state, func(t *testing.T) {
				set := appsv1.ReplicaSet{Spec: appsv1.ReplicaSetSpec{Replicas: &replicas}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate", Message: message}}}}
				switch state {
				case "healthy":
					set.Status.Replicas = 1
				case "scaled-down":
					zero := int32(0)
					set.Spec.Replicas = &zero
				case "terminating":
					stamp := metav1.Now()
					set.DeletionTimestamp = &stamp
				case "cleared":
					set.Status.Conditions[0].Status = corev1.ConditionFalse
				case "wrong-condition":
					set.Status.Conditions[0].Type = "Progressing"
				case "wrong-reason":
					set.Status.Conditions[0].Reason = "FailedDelete"
				}
				want := state == "blocked" && program.AdmissionRejection(message)
				if got := len(program.AdmissionFailedCreate(set)) > 0; got != want {
					t.Fatalf("got %v want %v", got, want)
				}
			})
		}
	}
	for _, ref := range []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "api"}, {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}, {APIVersion: "v1", Kind: "ReplicaSet", Namespace: "app", Name: "api"}, {APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api"}, {APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app"}} {
		err := (program.Plan{Actions: []program.Action{program.RetryAdmission{Ref: ref}}}).Validate()
		want := ref.APIVersion == "apps/v1" && ref.Kind == "ReplicaSet" && ref.Namespace != "" && ref.Name != ""
		if (err == nil) != want {
			t.Fatalf("validation ref=%+v err=%v", ref, err)
		}
	}
	var absent *program.RetryAdmission
	if err := (program.Plan{Actions: []program.Action{absent}}).Validate(); err == nil {
		t.Fatal("nil retry accepted")
	}
}

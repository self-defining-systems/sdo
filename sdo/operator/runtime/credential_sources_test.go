package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk/program"
)

func TestConfigMapCredentialAssignment(t *testing.T) {
	for _, tc := range []struct{ content, want string }{
		{"ROOT_PASSWORD=sentinel", "sentinel"},
		{"export ROOT_PASSWORD='sentinel' # comment", "sentinel"},
		{" ROOT_PASSWORD=\"sentinel\"\nOTHER=unrelated", "sentinel"},
		{"OTHER=sentinel", ""},
		{"ROOT_PASSWORD=sentinel\nROOT_PASSWORD=other", ""},
		{"ROOT_PASSWORD=$(echo sentinel)", ""},
		{"ROOT_PASSWORD=\"${OTHER}\"", ""},
		{"ROOT_PASSWORD=sentinel; echo dangerous", ""},
	} {
		t.Run(tc.content, func(t *testing.T) {
			client := TypedPlanClient{Client: kubefake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "mongo-geo-script", Namespace: "app"}, Data: map[string]string{"k8s-geo-mongo.sh": tc.content}})}
			var ref program.CredentialRef
			if err := json.Unmarshal([]byte(`{"config_map":{"name":"mongo-geo-script","key":"k8s-geo-mongo.sh","assignment":"ROOT_PASSWORD"}}`), &ref); err != nil {
				t.Fatal(err)
			}
			if err := ref.Validate(); err != nil {
				t.Fatal(err)
			}
			got, err := client.resolveCredential(context.Background(), "app", ref)
			if tc.want != "" {
				if err != nil || got != tc.want {
					t.Fatalf("resolution failed: %v", err)
				}
			} else {
				var typed *CredentialReferenceError
				if !errors.As(err, &typed) || strings.Contains(err.Error(), "sentinel") || !strings.Contains(err.Error(), "mongo-geo-script") {
					t.Fatalf("unsafe or untyped failure: %v", err)
				}
			}
		})
	}
}

func TestConfigMapCredentialKeyAndUnavailableReference(t *testing.T) {
	client := TypedPlanClient{Client: kubefake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "app"}, Data: map[string]string{"password": "key-sentinel"}, BinaryData: map[string][]byte{"binary": []byte("binary-sentinel")}})}
	for _, key := range []string{"password", "binary", "missing"} {
		ref := program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "credentials", Key: key}}
		value, err := client.resolveCredential(context.Background(), "app", ref)
		if key == "missing" {
			var typed *CredentialReferenceError
			if !errors.As(err, &typed) {
				t.Fatalf("missing key: %v", err)
			}
		} else if err != nil || !strings.HasSuffix(value, "sentinel") {
			t.Fatalf("key unavailable: %v", err)
		}
	}
	ref := program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "credentials", Key: "password"}}
	_, err := client.resolveCredential(context.Background(), "other", ref)
	var typed *CredentialReferenceError
	if !errors.As(err, &typed) {
		t.Fatalf("reference escaped namespace: %v", err)
	}
	for _, a := range client.Client.(*kubefake.Clientset).Actions() {
		if a.GetResource().Resource != "configmaps" {
			t.Fatalf("unexpected credential source: %v", a)
		}
	}
}

func TestConfigMapCredentialContractRoundTrip(t *testing.T) {
	a := databaseAction()
	ref := program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "k8s-geo-mongo.sh", Assignment: "ROOT_PASSWORD"}}
	a.Password = ref
	a.Target.Auth = &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "k8s-geo-mongo.sh", Assignment: "ROOT_USER"}}, Password: ref}
	original := program.Plan{Actions: []program.Action{a}}
	encoded, err := (PlanApplicationResult{Plan: original}).Proto()
	if err != nil {
		t.Fatal(err)
	}
	restored := planApplicationState(encoded).Result.Plan
	if err := restored.Validate(); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(original)
	after, _ := json.Marshal(restored)
	if string(before) != string(after) {
		t.Fatalf("reference round trip changed: %s != %s", before, after)
	}
}

package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
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

func FuzzCredentialAssignmentLiteral(f *testing.F) {
	for _, seed := range []string{"sentinel", "spaces and $literal", "unicode-密碼", "\\literal", "punctuation#@!"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		if value == "" || !utf8.ValidString(value) || strings.ContainsAny(value, "'\r\n\x00") {
			return
		}
		content := "ROOT_PASSWORD='" + value + "'\n"
		got, ok := assignmentCredential(content, "ROOT_PASSWORD")
		if !ok || got != value {
			t.Fatal("quoted literal did not round trip")
		}
		if _, ok := assignmentCredential(content+content, "ROOT_PASSWORD"); ok {
			t.Fatal("duplicate credential assignment accepted")
		}
	})
}

func TestLegacyMongoStdinBootstrapPreservesPrivateInput(t *testing.T) {
	const sentinel = "DBC_PRIVATE_STDIN_SENTINEL"
	// The real POSIX bootstrap must pass bytes as data, including shell syntax.
	// The child stands in for mongo only at the process boundary; no database
	// behavior or authentication is simulated by this test.
	script := "print(\"" + sentinel + " $(printf injected) `printf injected` \\\"quoted\\\"\");"
	directory := t.TempDir()
	child := `#!/bin/sh
[ "$_SDO_DATABASE_SCRIPT" = "$DBC_EXPECTED_SCRIPT" ] || exit 11
for arg do
 case "$arg" in *DBC_PRIVATE_STDIN_SENTINEL*|*injected*) exit 12;; esac
done
[ "$7" = "--eval" ] || exit 13
[ "$8" = 'eval(_getEnv("_SDO_DATABASE_SCRIPT"))' ] || exit 14
`
	if err := os.WriteFile(filepath.Join(directory, "mongo"), []byte(child), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+":"+os.Getenv("PATH"))
	t.Setenv("DBC_EXPECTED_SCRIPT", script)
	args := mongoCommandArgs("mongo")
	command := exec.Command(args[0], args[1:]...)
	command.Stdin = strings.NewReader(script + "\n")
	if err := command.Run(); err != nil {
		t.Fatalf("legacy shell did not preserve private stdin/environment boundary: %v", err)
	}
}

func TestLegacyBootstrapDoesNotShadowCredentialEnv(t *testing.T) {
	args := mongoCommandArgs("mongo", program.CredentialRef{Env: "_SDO_DATABASE_SCRIPT"}, program.CredentialRef{Env: "_SDO_DATABASE_SCRIPT_"})
	if !strings.Contains(args[2], "read -r _SDO_DATABASE_SCRIPT__ ") || args[len(args)-1] != `eval(_getEnv("_SDO_DATABASE_SCRIPT__"))` {
		t.Fatal("private stdin bootstrap shadows a declared credential env")
	}
}

func TestConfigMapCredentialReadErrorNeverIncludesResponse(t *testing.T) {
	const canary = "DBC_CONFIGMAP_ERROR_VALUE_CANARY"
	client := kubefake.NewSimpleClientset()
	client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		return true, nil, fmt.Errorf("upstream response: %s", canary)
	})
	ref := program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "k8s-geo-mongo.sh", Assignment: "ROOT_PWD"}}
	_, err := (TypedPlanClient{Client: client}).resolveCredential(context.Background(), "app", ref)
	var typed *CredentialReferenceError
	if !errors.As(err, &typed) || !strings.Contains(err.Error(), "mongo-geo-script") || strings.Contains(err.Error(), canary) {
		t.Fatal("upstream credential read failure was not sanitized")
	}
}

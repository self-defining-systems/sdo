//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sdo.dev/operator/sdk/program"
)

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

func TestCredentialConfigMapNeverEntersVerboseClientLogs(t *testing.T) {
	if !inIsolatedProcess(t) {
		return
	}
	const canary = "DBC_VERBOSE_API_RESPONSE_CREDENTIAL_CANARY"
	state := klog.CaptureState()
	defer state.Restore()
	flags := flag.NewFlagSet("credential-client-logging", flag.ContinueOnError)
	klog.InitFlags(flags)
	if err := flags.Set("v", "10"); err != nil {
		t.Fatal(err)
	}
	if err := flags.Set("logtostderr", "false"); err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	klog.SetOutput(logs)
	for _, mode := range []string{"success", "denied", "malformed", "oversized", "redirect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/must-not-follow" {
				t.Error("credential API followed redirect")
				http.Error(w, "forbidden", 403)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if mode == "denied" {
				http.Error(w, canary, http.StatusForbidden)
				return
			}
			if mode == "malformed" {
				_, _ = w.Write([]byte(canary))
				return
			}
			if mode == "oversized" {
				_, _ = w.Write([]byte(canary + strings.Repeat("x", 2<<20)))
				return
			}
			if mode == "redirect" {
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(http.StatusFound)
				return
			}
			_ = json.NewEncoder(w).Encode(corev1.ConfigMap{Data: map[string]string{"password": canary}})
		}))
		client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
		if err != nil {
			t.Fatal(err)
		}
		ref := program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "password"}}
		value, err := (TypedPlanClient{Client: client}).resolveCredential(context.Background(), "app", ref)
		server.Close()
		if (err != nil) != (mode != "success") || (mode == "success" && value != canary) {
			t.Fatal("credential read changed semantics")
		}
	}
	klog.Flush()
	if strings.Contains(logs.String(), canary) {
		t.Fatal("credential API response entered verbose client logs")
	}
}

//go:build integration

package env

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestWithDefaultsKindClusterFromEnvironment(t *testing.T) {
	t.Setenv("FUZZOPS_KIND_CLUSTER", "other")
	e := Env{}.WithDefaults()
	if e.KindCluster != "other" || e.KubeContext != "kind-other" {
		t.Fatalf("got cluster %q context %q", e.KindCluster, e.KubeContext)
	}
	if e := (Env{KindCluster: "set"}).WithDefaults(); e.KindCluster != "set" {
		t.Fatalf("explicit cluster overridden: %q", e.KindCluster)
	}
}

type call struct {
	stdin string
	cmd   []string
}

func fake(out string, err error, calls *[]call) Runner {
	return func(_ context.Context, stdin []byte, name string, args ...string) ([]byte, error) {
		*calls = append(*calls, call{string(stdin), append([]string{name}, args...)})
		return []byte(out), err
	}
}

func TestDefaultsAndBaseURL(t *testing.T) {
	e := Default()
	if e.KindCluster != "fuzzops" || e.KubeContext != "kind-fuzzops" || e.Namespace != "shop" || e.NodePort != 30080 {
		t.Fatalf("bad defaults: %+v", e)
	}
	if got := e.BaseURL(); got != "http://127.0.0.1:30080" {
		t.Fatalf("BaseURL = %s", got)
	}
}

func TestKubectlArgs(t *testing.T) {
	var calls []call
	e := Env{KindCluster: "x", Namespace: "ns"}.WithRunner(fake("ok", nil, &calls))
	if _, err := e.Kubectl(context.Background(), "get", "pods"); err != nil {
		t.Fatal(err)
	}
	want := []string{"kubectl", "--context", "kind-x", "-n", "ns", "get", "pods"}
	if !reflect.DeepEqual(calls[0].cmd, want) {
		t.Fatalf("got %v want %v", calls[0].cmd, want)
	}
}

func TestKubectlStdinAndError(t *testing.T) {
	var calls []call
	e := Default().WithRunner(fake("boom", errors.New("exit 1"), &calls))
	_, err := e.KubectlStdin(context.Background(), []byte("yaml"), "apply", "-f", "-")
	if err == nil || calls[0].stdin != "yaml" {
		t.Fatalf("err=%v stdin=%q", err, calls[0].stdin)
	}
	if !contains(err.Error(), "boom") {
		t.Fatalf("error lacks output: %v", err)
	}
}

func TestNodeExec(t *testing.T) {
	var calls []call
	e := Default().WithRunner(fake("", nil, &calls))
	_, _ = e.NodeExec(context.Background(), "fuzzops-worker", "ls", "/")
	want := []string{"docker", "exec", "fuzzops-worker", "ls", "/"}
	if !reflect.DeepEqual(calls[0].cmd, want) {
		t.Fatalf("got %v", calls[0].cmd)
	}
}

func TestPodNodeAndPodsByLabel(t *testing.T) {
	var calls []call
	e := Default().WithRunner(fake("n1\n", nil, &calls))
	n, err := e.PodNode(context.Background(), "p")
	if err != nil || n != "n1" {
		t.Fatalf("%q %v", n, err)
	}
	e = Default().WithRunner(fake("a b", nil, &calls))
	pods, err := e.PodsByLabel(context.Background(), "app=api")
	if err != nil || !reflect.DeepEqual(pods, []string{"a", "b"}) {
		t.Fatalf("%v %v", pods, err)
	}
	e = Default().WithRunner(fake("", nil, &calls))
	if _, err := e.PodNode(context.Background(), "p"); err == nil {
		t.Fatal("want error for unscheduled pod")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestExecAndNodeExecStdin(t *testing.T) {
	var calls []call
	e := Default().WithRunner(fake("", nil, &calls))
	_, _ = e.Exec(context.Background(), "go", "version")
	_, _ = e.NodeExecStdin(context.Background(), "n1", []byte("bits"), "sh", "-c", "cat > /x")
	if !reflect.DeepEqual(calls[0].cmd, []string{"go", "version"}) {
		t.Fatalf("exec: %v", calls[0].cmd)
	}
	if !reflect.DeepEqual(calls[1].cmd, []string{"docker", "exec", "-i", "n1", "sh", "-c", "cat > /x"}) || calls[1].stdin != "bits" {
		t.Fatalf("stdin exec: %+v", calls[1])
	}
}

// A cancelled command returns promptly even when a child it started still
// holds its output pipe (a script that ran kubectl or helm in the background).
func TestCancelKillsChildren(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Env{}.Exec(ctx, "sh", "-c", "sleep 60 & sleep 60")
	if err == nil {
		t.Fatal("want an error from a cancelled command")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("cancelled command took %s to return", d)
	}
}

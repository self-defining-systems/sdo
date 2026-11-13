//go:build integration

// Package skillexamples runs the playbook-writing skill's examples against minimal synthetic installs.
package skillexamples

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const examplesDir = "../../skills/playbook-writing/examples"

// driver is copied beside an example (renamed to package example) and runs its Diagnose on a healthy
// install, on the other example's fault (an unrelated fault) and on its own fault.
const driver = `package example

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type view struct {
	program.ReadView
	drifted bool
}

func (v view) Logs(sdk.ObjectRef, string, time.Duration) (string, error) { return "boom", nil }

func (v view) DiffAgainstSource(sdk.ObjectRef) (sdk.SourceDiff, error) {
	side := func(image string) []byte {
		return []byte("{\"spec\":{\"template\":{\"spec\":{\"containers\":[{\"name\":\"web\",\"image\":\"" + image + "\"}]}}}}")
	}
	if !v.drifted {
		return sdk.SourceDiff{Text: sdk.SourceComparisonText(side("web:1"), side("web:1"))}, nil
	}
	return sdk.SourceDiff{Changed: true, Text: sdk.SourceComparisonText(side("web:1"), side("web:bad"))}, nil
}

// install is a one-Deployment application; crash makes its Pod crash loop, drifted gives its template
// a wrong image (so the Pod cannot pull it).
func install(crash, drifted bool) view {
	yes := true
	status := corev1.ContainerStatus{Name: "web", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
	if crash {
		status = corev1.ContainerStatus{Name: "web", RestartCount: 5, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}
	}
	if drifted {
		status = corev1.ContainerStatus{Name: "web", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app",
		DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}}},
		ReplicaSetList: []appsv1.ReplicaSet{{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", Controller: &yes}}}}},
		PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "web-1-a", Namespace: "app", OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-1", Controller: &yes}}},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{status}}}}}
	return view{ReadView: snapshot.RuntimeReadView(), drifted: drifted}
}

func TestDiagnose(t *testing.T) {
	own, other := install(true, false), install(false, true)
	if (Program{}).Spec().ID == "template-drift" {
		own, other = other, own
	}
	if params := (Program{}).Spec().Params; len(params) != 0 {
		t.Fatalf("an example declares no object role, got %+v", params)
	}
	for name, c := range map[string]struct {
		v    view
		want program.Result
	}{"healthy install": {install(false, false), program.NoMatch}, "unrelated fault": {other, program.NoMatch}, "its own fault": {own, program.Match}} {
		started := time.Now()
		verdict := program.RunDiagnose(context.Background(), Program{}, c.v, program.Bindings{}, 10*time.Second)
		duration := time.Since(started)
		t.Logf("%s: %s in %s", name, verdict.Result, duration)
		if verdict.Result != c.want {
			t.Errorf("%s: %+v, want %s", name, verdict, c.want)
		}
		if duration > time.Second {
			t.Errorf("%s: Diagnose took %s, far over its share of the 10s diagnosis deadline", name, duration)
		}
	}
}
`

func TestExamplesDiagnoseNoMatchOffTheirFault(t *testing.T) {
	entries, err := os.ReadDir(examplesDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("examples: %v", err)
	}
	pkg := regexp.MustCompile(`(?m)^package \w+`)
	for _, entry := range entries {
		t.Run(entry.Name(), func(t *testing.T) {
			dir, err := os.MkdirTemp(".", "_example-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			for _, name := range []string{"diagnose.go", "mitigate.go"} {
				data, err := os.ReadFile(filepath.Join(examplesDir, entry.Name(), name))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), pkg.ReplaceAll(data, []byte("package example")), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "driver_test.go"), []byte(driver), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("go", "test", "-v", "-count=1", "./"+filepath.Base(dir)).CombinedOutput()
			t.Log(strings.TrimSpace(string(out)))
			if err != nil {
				t.Fatalf("go test: %v", err)
			}
		})
	}
}

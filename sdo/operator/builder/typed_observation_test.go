package builder

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Exercise the actual authored-program compiler and generated memory replay,
// including rejected stale observations, rather than a test-only Diagnoser.
func TestTypedObservationProgramCompilesAndReportsStaleEvidence(t *testing.T) {
	testutil.OfflineGo(t, "..")
	t.Setenv("GOFLAGS", os.Getenv("GOFLAGS")+" -mod=mod -p=2")
	root := programFixture(t)
	path := filepath.Join(root, ".sdo", "playbooks", "service-targetport-mismatch", "diagnose.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Replace(string(data), `"sdo.dev/operator/sdk/program"`, `"sdo.dev/operator/sdk/program"`+"\n"+`"sdo.dev/operator/sdk"`+"\n"+`corev1 "k8s.io/api/core/v1"`+"\n"+`metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"`, 1)
	source = strings.Replace(source, `if !found {`, `if !found {
  podRef:=sdk.ObjectRefFrom("Pod","v1",&pod)
  logs,err:=v.LogsWithOptions(podRef,corev1.PodLogOptions{Container:pod.Spec.Containers[0].Name,Previous:true})
  if err!=nil{return program.Verdict{},err}
  var samples *metricsv1beta1.PodMetrics
  samples,err=v.PodUsage(podRef)
  if err!=nil{return program.Verdict{},err}
  if logs==""||len(samples.Containers)==0||samples.Timestamp.IsZero(){return program.Verdict{Result:program.Unknown},nil}
 `, 1)
	if err := os.WriteFile(path, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	corpus := filepath.Join(root, ".sdo", "corpus", "targetport-mismatch", "recordings.json")
	var calls []sdktest.PrimitiveCall
	for _, phase := range []string{"open", "fixed"} {
		for _, name := range []string{"front-1", "peer-1"} {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "demo", Name: name}
			add := func(method string, args, result any) {
				a, _ := json.Marshal(args)
				r, _ := json.Marshal(result)
				calls = append(calls, sdktest.PrimitiveCall{Phase: phase, Method: method, Arguments: a, Result: r})
			}
			add("LogsWithOptions", []any{ref, corev1.PodLogOptions{Container: "app", Previous: true}}, "prior container terminated")
			add("PodUsage", []any{ref}, &metricsv1beta1.PodMetrics{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace}, Containers: []metricsv1beta1.ContainerMetrics{{Name: "app", Usage: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("125m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}}, Timestamp: metav1.NewTime(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)), Window: metav1.Duration{Duration: 30 * time.Second}})
		}
	}
	raw, err := json.Marshal(calls)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(corpus, raw, 0644); err != nil {
		t.Fatal(err)
	}
	workspace, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("typed reads failed compilation/replay: %v", err)
	}
	// Reuse the built workspace so only the corpus changes: opening evidence
	// must not be filled from fixed observations or treated as healthy zeros.
	for _, method := range []string{"LogsWithOptions", "PodUsage"} {
		t.Run("missing-"+method, func(t *testing.T) {
			var stale []sdktest.PrimitiveCall
			for _, call := range calls {
				if call.Phase != "open" || call.Method != method {
					stale = append(stale, call)
				}
			}
			raw, err := json.Marshal(stale)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(workspace.Directory, "corpus", "targetport-mismatch", "recordings.json"), raw, 0644); err != nil {
				t.Fatal(err)
			}
			_, err = workspace.ValidateAndBuild(context.Background())
			if err != nil {
				t.Fatalf("stale %s blocked saving: %v", method, err)
			}
			notes, err := workspace.MitigationReplayResults()
			if err != nil {
				t.Fatal(err)
			}
			if len(notes) != 1 || !strings.Contains(strings.Join(notes[0].Effects, " "), "unrecorded "+method) {
				t.Fatalf("stale %s not reported: %v", method, notes)
			}
		})
	}
}

package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type repeatHealthDetector struct{ missingConfigMapDetector }

func (repeatHealthDetector) Spec() sdk.DetectorSpec {
	spec := (missingConfigMapDetector{}).Spec()
	spec.Class, spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	spec.OriginatingCommit = "synthetic-health"
	spec.Interval = 20 * time.Millisecond
	spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: time.Millisecond}
	return spec
}

// The production maintenance watcher, informer queue, scheduler, persistence and
// dispatcher must keep moving with the bundle loaded after a memory publication.
// The erroring learned detector is deliberately retained: its failure is visible
// while an independent health detector must still detect and dispatch the fault.
func TestRunRepeatWithLearnedDetectorErrorAndQueuedPodChanges(t *testing.T) {
	repository := gitRepository(t)
	accepted := nativeGit(t, repository, "rev-parse", "HEAD")
	objects := []kruntime.Object{&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "required-settings", Namespace: "app"}}}
	params := []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}
	for i := 0; i < 7; i++ {
		params = append(params, program.ParamSpec{Name: fmt.Sprintf("deployment%d", i), Kind: "Deployment", Required: true})
	}
	for i := 0; i < 24; i++ {
		meta := metav1.ObjectMeta{Name: fmt.Sprintf("resource-%02d", i), Namespace: "app"}
		deployment := &appsv1.Deployment{ObjectMeta: meta}
		if i == 0 {
			deployment.Spec.Template.Spec.Containers = []corev1.Container{{Name: "app", EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "required-settings"}}}}}}
		}
		objects = append(objects, deployment, &corev1.Service{ObjectMeta: meta})
	}
	learned := candidateProgram{spec: program.ProgramSpec{ID: "learned-discovery", Params: params}, run: func(context.Context, program.Bindings) program.Verdict {
		return program.Verdict{Result: program.NoMatch}
	}}
	registry, err := program.NewRegistry([]program.Diagnoser{learned}, nil)
	if err != nil {
		t.Fatal(err)
	}
	detector := repeatUnavailableDetector{accepted: accepted}
	client := fake.NewSimpleClientset(objects...)
	dispatcher := capturingDispatcher{requests: make(chan IncidentRequest, 1)}
	output, writer := newRestartClassOutput()
	diagnostics, errorWriter := newRestartClassOutput()
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan error, 1)
	go func() {
		exited <- RunWithOptions(ctx, []sdk.Detector{detector, repeatHealthDetector{}}, RuntimeOptions{
			Args: []string{"--namespace", "app", "--control-namespace", "app-sdo", "--app-root", repository, "--application", "app", "--source-commit", accepted,
				"--synthetic-traffic=false", "--state-baseline=false", "--clean-responder-helpers=false", "--lease-duration", "3s", "--maintenance-poll-interval", "5ms",
				"--diagnosis-deadline", "300ms", "--firing-telemetry-path", "off", "--lifecycle-telemetry-path", "off"},
			Client: client, Programs: registry, Dispatcher: dispatcher, Stdout: writer, Stderr: errorWriter,
		})
	}()
	defer func() {
		cancel()
		select {
		case err := <-exited:
			if err != nil {
				t.Errorf("controller exit: %v; %s", err, diagnostics)
			}
		case <-time.After(3 * time.Second):
			t.Error("controller could not stop")
		}
	}()
	eventually(t, "initial detector evaluation", func() bool { return output.has(`{"controller_iteration":0`) })
	pause := maintenanceConfigMap("app-sdo", "paused", "repeat-injection")
	pause.Data["preserve_baseline"] = "true"
	if _, err := client.CoreV1().ConfigMaps("app-sdo").Create(ctx, pause, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "maintenance pause", func() bool { return output.has(`{"controller_maintenance":"paused"`) })
	for i := 0; i < 30; i++ {
		if _, err := client.CoreV1().Pods("app").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("restarted-%02d", i), Namespace: "app"}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := client.CoreV1().ConfigMaps("app").Delete(ctx, "required-settings", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("app-sdo").Update(ctx, maintenanceConfigMap("app-sdo", "active", "repeat-injected"), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-dispatcher.requests:
		if request.SourceCommit != accepted {
			t.Fatalf("repeat used stale accepted memory: %s", request.SourceCommit)
		}
		foundFault, foundError := false, false
		for _, finding := range request.Findings {
			foundFault = foundFault || finding.DetectorID == "missing-configmap"
		}
		for _, evaluation := range request.DetectorHistory {
			foundError = foundError || evaluation.DetectorID == "learned-unavailable" && evaluation.Status == DetectorEvaluationError
		}
		if !foundFault || !foundError {
			t.Fatalf("fault or typed detector error missing: %+v", request)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("queued learned-program work starved repeat dispatch; stdout=%s stderr=%s", output, diagnostics)
	}
	store := lifecycleio.NewConfigMapStateStore(client, "app-sdo", controllerStateConfigMap)
	eventually(t, "subsequent detector iterations", func() bool {
		state, _, err := store.Load(ctx)
		return err == nil && state.GetEvaluationIteration() >= 4
	})
}

// An independent detector may fail to observe a dependency. That error must
// remain visible while the scheduler and unrelated detectors keep progressing.
type repeatUnavailableDetector struct{ accepted string }

func (d repeatUnavailableDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "learned-unavailable", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder, Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: 20 * time.Millisecond, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingIncident: "synthetic-first-encounter", OriginatingCommit: d.accepted}
}
func (repeatUnavailableDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, fmt.Errorf("synthetic deployment evidence unavailable")
}

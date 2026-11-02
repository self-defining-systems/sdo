package runtime

import (
	"context"
	"encoding/json"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/builder"
)

// Execute the accepted bundle in its own module so generated All/Programs are
// the authored, validated code, rather than an in-process detector substitute.
func runCompiledLearnAcceptRepeatController(t *testing.T, ctx context.Context, built *builder.Workspace, accepted string, request IncidentRequest) {
	t.Helper()
	acceptedCommit := nativeGit(t, accepted, "rev-parse", "HEAD")
	if acceptedCommit == request.SourceCommit {
		t.Fatal("cycle did not accept new learned memory after the first incident")
	}
	request.SourceCommit = acceptedCommit
	request.RepositoryWorktree = accepted
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "accepted-request.json")
	if err := os.WriteFile(input, raw, 0600); err != nil {
		t.Fatal(err)
	}
	source, err := format.Source([]byte(compiledLearnAcceptRepeatController))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(built.Directory, "generated/learn_accept_repeat_test.go")
	if err := os.WriteFile(path, source, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "go", "test", "-race", "-v", "-count=1", "-timeout=45s", "-run", "^TestAcceptedLearnedControllerRepeat$", "./generated")
	command.Dir = built.Directory
	command.Env = append(os.Environ(), "LE_CYCLE_REQUEST="+input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("accepted learned controller repeat: %v: %s", err, output)
	}
	t.Logf("dynamic accepted-memory controller cycle: %s", output)
}

const compiledLearnAcceptRepeatController = `package generated

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"sync"
	"testing"
	"time"
)

type cycleOutput struct {
	mu   sync.Mutex
	text strings.Builder
}

func (w *cycleOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.text.Write(p)
}
func (w *cycleOutput) String() string { w.mu.Lock(); defer w.mu.Unlock(); return w.text.String() }

type cycleDispatcher struct{ requests chan operator.IncidentRequest }

func (d cycleDispatcher) Dispatch(ctx context.Context, r operator.IncidentRequest) (operator.IncidentResult, error) {
	select {
	case d.requests <- r:
	case <-ctx.Done():
		return operator.IncidentResult{}, ctx.Err()
	}
	<-ctx.Done()
	return operator.IncidentResult{}, ctx.Err()
}
func TestAcceptedLearnedControllerRepeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw, err := os.ReadFile(os.Getenv("LE_CYCLE_REQUEST"))
	if err != nil {
		t.Fatal(err)
	}
	var accepted operator.IncidentRequest
	if err := json.Unmarshal(raw, &accepted); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(accepted.RepositoryWorktree, ".sdo/corpus/*/labels.json"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("accepted learned corpus: %v %v", paths, err)
	}
	corpus, err := sdktest.LoadCorpusCase(filepath.Dir(paths[0]))
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	for _, item := range corpus.Fixed.ServiceList {
		if _, err := client.CoreV1().Services("app").Create(ctx, item.DeepCopy(), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range corpus.Fixed.PodList {
		if _, err := client.CoreV1().Pods("app").Create(ctx, item.DeepCopy(), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := Programs()
	if err != nil {
		t.Fatal(err)
	}
	detectors := All()
	found := false
	for _, d := range detectors {
		found = found || d.Spec().ID == "routing-relations"
	}
	if !found {
		t.Fatal("accepted learned detector was not compiled into restarted controller")
	}
	output, diagnostics := new(cycleOutput), new(cycleOutput)
	dispatcher := cycleDispatcher{requests: make(chan operator.IncidentRequest, 4)}
	exited := make(chan error, 1)
	evidenceRoot := t.TempDir()
	go func() {
		exited <- operator.RunWithOptions(ctx, detectors, operator.RuntimeOptions{
			Args: []string{"--namespace", "app", "--control-namespace", "app-sdo", "--app-root", accepted.RepositoryWorktree, "--application", "app", "--source-commit", accepted.SourceCommit,
				"--repository-mount-path", evidenceRoot, "--synthetic-traffic=false", "--state-baseline=false", "--clean-responder-helpers=false", "--lease-duration", "3s", "--maintenance-poll-interval", "5ms",
				"--diagnosis-deadline", "300ms", "--firing-telemetry-path", "off", "--lifecycle-telemetry-path", "off"},
			Client: client, Programs: registry, Dispatcher: dispatcher, Stdout: output, Stderr: diagnostics})
	}()
	defer func() {
		cancel()
		select {
		case err := <-exited:
			if err != nil {
				t.Errorf("controller exit: %v; %s", err, diagnostics)
			}
		case <-time.After(3 * time.Second):
			t.Error("controller did not stop")
		}
	}()
	wait := func(what string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if condition() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("%s: stdout=%s stderr=%s", what, output, diagnostics)
	}
	state := func() *contracts.ControllerState {
		cm, err := client.CoreV1().ConfigMaps("app-sdo").Get(ctx, "sdo-controller-state", metav1.GetOptions{})
		if err != nil {
			return nil
		}
		value := new(contracts.ControllerState)
		if err := protojson.Unmarshal([]byte(cm.Data["runtime-state.json"]), value); err != nil {
			return nil
		}
		return value
	}
	wait("accepted bundle healthy before repeat", func() bool {
		s := state()
		if s == nil || s.GetEvaluationIteration() < 2 {
			return false
		}
		for _, evaluation := range s.GetHistory() {
			if evaluation.GetDetectorId() == "routing-relations" && evaluation.GetStatus() == "clear" {
				return true
			}
		}
		return false
	})
	before := state().GetEvaluationIteration()
	select {
	case request := <-dispatcher.requests:
		t.Fatalf("healthy accepted baseline dispatched: %+v", request)
	default:
	}
	paused := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: operator.MaintenanceConfigMapName, Namespace: "app-sdo"}, Data: map[string]string{"state": "paused", "generation": "learn-accept-repeat", "preserve_baseline": "true"}}
	if _, err := client.CoreV1().ConfigMaps("app-sdo").Create(ctx, paused, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	wait("maintenance pause after accepted learning", func() bool { return strings.Contains(output.String(), "\"controller_maintenance\":\"paused\"") })
	for i := 0; i < 30; i++ {
		if _, err := client.CoreV1().Pods("app").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("cycle-queued-%02d", i), Namespace: "app"}}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	service, err := client.CoreV1().Services("app").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	if _, err := client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	active := paused.DeepCopy()
	active.Data = map[string]string{"state": "active", "generation": "learn-accept-repeat-injected"}
	if _, err := client.CoreV1().ConfigMaps("app-sdo").Update(ctx, active, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case request := <-dispatcher.requests:
		if request.SourceCommit != accepted.SourceCommit {
			t.Fatalf("repeat lost accepted memory revision: %s != %s", request.SourceCommit, accepted.SourceCommit)
		}
		found := false
		for _, finding := range request.Findings {
			found = found || finding.DetectorID == "routing-relations"
		}
		if !found {
			t.Fatalf("injected fault was not found by accepted learned detector: %+v", request.Findings)
		}
	case <-ctx.Done():
		t.Fatalf("accepted learned controller missed injected repeat; stdout=%s stderr=%s", output, diagnostics)
	}
	dispatchedIteration := state().GetEvaluationIteration()
	wait("controller keeps iterating after learned repeat dispatch", func() bool { s := state(); return s != nil && s.GetEvaluationIteration() >= dispatchedIteration+2 })
	t.Logf("accepted commit %s: healthy iteration %d, queued changes, learned repeat dispatch, later iteration %d", accepted.SourceCommit, before, state().GetEvaluationIteration())
}
`

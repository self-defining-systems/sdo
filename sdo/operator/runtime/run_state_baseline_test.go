package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
)

// capturingDispatcher records the incident request the controller dispatches.
type capturingDispatcher struct{ requests chan IncidentRequest }

func (d capturingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	return completedResult(request.IncidentID), nil
}

type lockedLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *lockedLines) has(prefix string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func (l *lockedLines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func collectLines(reader io.Reader, into *lockedLines) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		into.mu.Lock()
		into.lines = append(into.lines, scanner.Text())
		into.mu.Unlock()
	}
}

func gitRepository(t *testing.T) string {
	t.Helper()
	repository := t.TempDir()
	// Production snapshots load observation policy only from accepted architecture.
	// Keep these run fixtures on that contract, even without foreign observations.
	if err := os.MkdirAll(filepath.Join(repository, ".sdo"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, ".sdo", "arch.md"), []byte("---\nschema_version: 1\n---\nSynthetic test architecture.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, command := range [][]string{
		{"git", "init", "-q", repository},
		{"git", "-C", repository, "config", "user.name", "test"},
		{"git", "-C", repository, "config", "user.email", "test@example.com"},
		{"git", "-C", repository, "add", ".sdo/arch.md"},
		{"git", "-C", repository, "commit", "-q", "-m", "initial"},
	} {
		if output, err := exec.Command(command[0], command[1:]...).CombinedOutput(); err != nil {
			t.Fatalf("run %v: %v: %s", command, err, output)
		}
	}
	return repository
}

func readyFrontend() []corev1.Pod {
	return []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{
			Name: "frontend-7d9", Namespace: hotel, Labels: map[string]string{"io.kompose.service": "frontend"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}}
}

func frontendEndpoints(ready int) *corev1.Endpoints {
	endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: hotel}}
	if ready > 0 {
		endpoints.Subsets = []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: "10.0.0.7"}},
			Ports:     []corev1.EndpointPort{{Name: "5000", Port: 5000}},
		}}
	}
	return endpoints
}

// TestRunAttachesStateChangesMadeAfterStartupToIncident drives the production
// run.go wiring, not a hand-built tracker: the controller starts, outlives its
// state-tracker start deadline, and only then does a fault change the
// application. The dispatched incident must carry that change. Before
// 13d5613, run.go's start context stopped the tracker's informers once Start
// returned, so the diff stayed frozen at the startup state and was empty.
func TestRunAttachesStateChangesMadeAfterStartupToIncident(t *testing.T) {
	testRunAttachesStateChanges(t, false, false)
}

func TestRunPreservesStateChangesAcrossObservationBarrier(t *testing.T) {
	testRunAttachesStateChanges(t, true, false)
}

func TestRunPreservesLatentFaultAcrossObservationBarrier(t *testing.T) {
	testRunAttachesStateChanges(t, true, true)
}

func testRunAttachesStateChanges(t *testing.T, observationBarrier, latent bool) {
	objects := hotelObjects()
	objects = append(objects, frontendEndpoints(1))
	for index := range readyFrontend() {
		objects = append(objects, &readyFrontend()[index])
	}
	client := fake.NewSimpleClientset(objects...)
	dispatcher := capturingDispatcher{requests: make(chan IncidentRequest, 1)}
	startTimeout := 300 * time.Millisecond

	detector := servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{
		ID: "service-endpoints", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: servicehealth.EndpointWatches(), Interval: 200 * time.Millisecond,
		Persistence:       sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:          sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 50 * time.Millisecond},
		OriginatingCommit: "lifecycle-bootstrap",
	})
	stdoutReader, stdoutWriter := io.Pipe()
	lines := &lockedLines{}
	go collectLines(stdoutReader, lines)
	stderrReader, stderrWriter := io.Pipe()
	errors := &lockedLines{}
	go collectLines(stderrReader, errors)
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	var runErr error
	repository := gitRepository(t)
	runArgs := []string{"--namespace", hotel, "--control-namespace", hotel + "-sdo", "--app-root", repository,
		"--application", "hotel", "--synthetic-traffic=false", "--lease-duration", "3s", "--drift-batch-window", "20ms"}
	if latent {
		service, err := client.CoreV1().Services(hotel).Get(ctx, "frontend", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		service.APIVersion, service.Kind = "v1", "Service"
		manifest, err := json.Marshal(service)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(repository+"/app.json", manifest, 0600); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"add", "app.json"}, {"commit", "-m", "accepted service"}} {
			if output, err := exec.Command("git", append([]string{"-C", repository}, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("%s: %v", output, err)
			}
		}
		seedRenderedLock(t, GitPlanSource{Repository: repository, ManifestPaths: []string{"app.json"}}, nativeGit(t, repository, "rev-parse", "HEAD"), hotel)
		runArgs = append(runArgs, "--deployed-manifest-path", "app.json")
	}
	go func() {
		defer close(exited)
		runErr = RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{
			Args:   runArgs,
			Stdout: stdoutWriter, Stderr: stderrWriter,
			Client: client, StateBaselineStartTimeout: startTimeout, Dispatcher: dispatcher,
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			t.Error("controller did not stop after cancellation")
		}
		_ = stdoutWriter.Close()
		_ = stderrWriter.Close()
	})

	started := func() bool { return lines.has(`{"state_baseline_startup_ms"`) }
	eventually(t, "the state tracker to start", func() bool {
		select {
		case <-exited:
			t.Fatalf("controller exited: %v\n%s", runErr, errors)
		default:
		}
		return started()
	})
	// Outlive the start deadline, so a tracker still bound to it has stopped.
	time.Sleep(4 * startTimeout)
	if observationBarrier {
		_, err := client.CoreV1().ConfigMaps(hotel+"-sdo").Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: MaintenanceConfigMapName, Namespace: hotel + "-sdo"},
			Data:       map[string]string{"state": "paused", "generation": "barrier", "preserve_baseline": "true"},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		eventually(t, "observation pause acknowledgement", func() bool {
			return lines.has(`{"controller_maintenance":"paused"`)
		})
	}

	if latent {
		policy := foreignPolicy("deny-frontend")
		policy.Namespace = hotel
		if _, err := client.NetworkingV1().NetworkPolicies(hotel).Create(ctx, policy, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		select {
		case request := <-dispatcher.requests:
			t.Fatalf("dispatched while observation paused: %s", request.IncidentID)
		default:
		}
	} else {
		service, err := client.CoreV1().Services(hotel).Get(ctx, "frontend", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		service.Spec.Selector["current_service_name"] = "frontend"
		if _, err := client.CoreV1().Services(hotel).Update(ctx, service, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		// The endpoints controller empties the Service's endpoints.
		if _, err := client.CoreV1().Endpoints(hotel).Update(ctx, frontendEndpoints(0), metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if observationBarrier {
		cm, err := client.CoreV1().ConfigMaps(hotel+"-sdo").Get(ctx, MaintenanceConfigMapName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cm.Data = map[string]string{"state": "active", "generation": "barrier-resumed"}
		if _, err := client.CoreV1().ConfigMaps(hotel+"-sdo").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	var request IncidentRequest
	select {
	case request = <-dispatcher.requests:
	case <-time.After(15 * time.Second):
		t.Fatalf("no incident was dispatched; controller output:\n%s\n%s", lines, errors)
	}
	if request.StateChanges == nil {
		t.Fatalf("the incident carries no state diff: %+v", request)
	}
	if latent {
		for _, event := range request.DriftEvents {
			if event.Ref.Kind == "NetworkPolicy" && event.Ref.Name == "deny-frontend" {
				return
			}
		}
		t.Fatalf("latent watch change lost across observation barrier: %+v", request)
	}
	change := findChange(request.StateChanges, "Service", "frontend")
	if change == nil || change.Change != StateChangeModified {
		t.Fatalf("the diff must name the Service changed after startup, got %+v", request.StateChanges.Changes)
	}
	for _, other := range request.StateChanges.Changes {
		if strings.HasPrefix(other.Name, "failure-admin") {
			t.Fatalf("decoys present at baseline must not appear: %+v", request.StateChanges.Changes)
		}
	}

	// A second fault after dispatch appears in the live view, not the request.
	if err := client.CoreV1().ConfigMaps(hotel).Delete(ctx, "mongo-geo-script", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	var view *contracts.IncidentView
	eventually(t, "the live view to name the second fault", func() bool {
		configMap, getErr := client.CoreV1().ConfigMaps(hotel+"-sdo").Get(ctx, "sdo-controller-state", metav1.GetOptions{})
		if getErr != nil {
			return false
		}
		document := &contracts.ControllerState{}
		if wire.Unmarshal([]byte(configMap.Data[stateDataKey]), document) != nil || document.GetIncidentView() == nil {
			return false
		}
		view = document.GetIncidentView()
		return view.GetStateChanges() != nil && findChange(stateChangesState(view.GetStateChanges()), "ConfigMap", "mongo-geo-script") != nil
	})
	if view.GetIncidentId() != request.IncidentID || len(view.GetBlockingDetectors()) != 1 ||
		view.GetBlockingDetectors()[0] != "service-endpoints" {
		t.Fatalf("the view must name the open incident and its blocking health detector: %+v", view)
	}
	if findChange(request.StateChanges, "ConfigMap", "mongo-geo-script") != nil {
		t.Fatalf("the dispatched request keeps its own snapshot: %+v", request.StateChanges.Changes)
	}
}

package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
)

// helperRequestEnvironment names the file the helper dispatcher writes the
// incident request it received to. The helper runs as this test binary.
const helperRequestEnvironment = "SDO_TEST_HELPER_DISPATCHER_REQUEST"

// TestRunHelperDispatcher is not a test: it is the local-mode responder the
// run.go liveness test dispatches, re-executing this test binary.
func TestRunHelperDispatcher(t *testing.T) {
	output := os.Getenv(helperRequestEnvironment)
	if output == "" {
		t.Skip("helper process for TestRunAttachesStateChangesMadeAfterStartupToIncident")
	}
	payload, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var request IncidentRequest
	if err := json.Unmarshal(payload, &request); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := os.WriteFile(output+".controller-state", []byte(os.Getenv(ControllerStateEnvironment)), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := os.WriteFile(output, payload, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	result, _ := json.Marshal(completedResult(request.IncidentID))
	fmt.Println(string(result))
	os.Exit(0)
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
	for _, command := range [][]string{
		{"git", "init", "-q", repository},
		{"git", "-C", repository, "config", "user.name", "test"},
		{"git", "-C", repository, "config", "user.email", "test@example.com"},
		{"git", "-C", repository, "commit", "-q", "--allow-empty", "-m", "initial"},
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
	objects := hotelObjects()
	objects = append(objects, frontendEndpoints(1))
	for index := range readyFrontend() {
		objects = append(objects, &readyFrontend()[index])
	}
	client := fake.NewSimpleClientset(objects...)
	requestPath := filepath.Join(t.TempDir(), "request.json")
	t.Setenv(helperRequestEnvironment, requestPath)
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
	go func() {
		defer close(exited)
		runErr = RunWithOptions(ctx, []sdk.Detector{detector}, RuntimeOptions{
			Args: []string{
				"--namespace", hotel, "--control-namespace", hotel + "-sdo", "--app-root", gitRepository(t),
				"--application", "hotel",
				"--dispatcher", os.Args[0], "--dispatcher-arg", "-test.run=^TestRunHelperDispatcher$",
				"--dispatcher-mode", "local", "--synthetic-traffic=false", "--lease-duration", "3s",
			},
			Stdout: stdoutWriter, Stderr: stderrWriter,
			Client: client, StateBaselineStartTimeout: startTimeout,
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

	var request IncidentRequest
	deadline := time.Now().Add(15 * time.Second)
	for {
		payload, readErr := os.ReadFile(requestPath)
		if readErr == nil && json.Unmarshal(payload, &request) == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no incident was dispatched; controller output:\n%s\n%s", lines, errors)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if request.StateChanges == nil {
		t.Fatalf("the incident carries no state diff: %+v", request)
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

	// The responder learns where the controller publishes its live view.
	location, err := os.ReadFile(requestPath + ".controller-state")
	if err != nil || string(location) != hotel+"-sdo/sdo-controller-state" {
		t.Fatalf("the responder must be told where the controller state lives, got %q (%v)", location, err)
	}
	// A second fault after dispatch appears in the live view, not the request.
	if err := client.CoreV1().ConfigMaps(hotel).Delete(ctx, "mongo-geo-script", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	var view *IncidentView
	eventually(t, "the live view to name the second fault", func() bool {
		configMap, getErr := client.CoreV1().ConfigMaps(hotel+"-sdo").Get(ctx, "sdo-controller-state", metav1.GetOptions{})
		if getErr != nil {
			return false
		}
		var state RuntimeState
		if json.Unmarshal([]byte(configMap.Data[stateDataKey]), &state) != nil || state.IncidentView == nil {
			return false
		}
		view = state.IncidentView
		return view.StateChanges != nil && findChange(view.StateChanges, "ConfigMap", "mongo-geo-script") != nil
	})
	if view.IncidentID != request.IncidentID || len(view.BlockingDetectors) != 1 ||
		view.BlockingDetectors[0] != "service-endpoints" {
		t.Fatalf("the view must name the open incident and its blocking health detector: %+v", view)
	}
	if findChange(request.StateChanges, "ConfigMap", "mongo-geo-script") != nil {
		t.Fatalf("the dispatched request keeps its own snapshot: %+v", request.StateChanges.Changes)
	}
}

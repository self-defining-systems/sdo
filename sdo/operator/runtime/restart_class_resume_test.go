package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// RT1 reduces social slot 2 to its actual source edit: restore one deployment
// while preserving the source-restoration annotation in accepted Git. The
// application is healthy and ACK is durable, but maintenance has held the old
// lock. Exercise RunWithOptions, its informer resume, lease, and maintenance
// watcher, rather than calling Step to stand in for the resumed process.
func TestRestartClassResumedControllerPublishesAfterACK(t *testing.T) {
	t.Setenv("RT1_SLOT2_SOURCE_REPAIR", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	root, proposals, _, _, coordinator := tracingFixture(t)
	tracingWrite(t, root, "deploy/tracing.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: jaeger\nspec:\n  replicas: 1\n  selector:\n    matchLabels: {app: jaeger}\n  template:\n    metadata:\n      labels: {app: jaeger}\n    spec:\n      containers:\n      - name: jaeger\n        image: synthetic/jaeger@sha256:"+strings.Repeat("a", 64)+"\n")
	nativeGit(t, root, "add", "deploy/tracing.yaml")
	nativeGit(t, root, "commit", "-qm", "source deployment")
	base := tracingRefresh(t, root, coordinator)
	native := restartClassNative(t, root, proposals, t.TempDir())
	replicas := int32(1)
	live := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "jaeger", Namespace: "demo", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}}
	client := fake.NewSimpleClientset(live, maintenanceConfigMap("demo-sdo", "paused", "before-publication"))
	source := GitPlanSource{Repository: root, ManifestPaths: []string{"deploy/tracing.yaml"}, DeployedCommit: base.Commit}
	if _, err := (RenderedLockReconciler{Source: source, Client: client, Broker: native.Repository}).Reconcile(ctx, base.Commit, "demo"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "controller.json")
	c := restartClassIncident(t, ctx, root, base.Commit, path, native)
	effect, ok := c.PendingClosureEffect()
	if !ok {
		t.Fatal("missing closure")
	}
	if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, c.closureResults)
	c.processBrokerCompletions()
	if err := c.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	current := tracingRefresh(t, root, coordinator)
	cfg := testControllerConfig()
	cfg.SourceCommit = current.Commit
	resumed, err := NewController(cfg, []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}, staticProvider{snapshot: restartClassSnapshot()}, &recordingDispatcher{}, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.AttachStateStore(ctx, crashStateStore{path: path}); err != nil {
		t.Fatal(err)
	}
	if err := resumed.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	rv6Guard(resumed, root, current.Commit)
	ack, ok := resumed.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("missing durable ACK")
	}
	if err := resumed.ExecuteClosureAcknowledgmentEffect(ctx, ack); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, resumed.acknowledgmentResults)
	resumed.processBrokerCompletions()
	if err := resumed.PersistState(ctx); err != nil {
		t.Fatal(err)
	}
	head := nativeGit(t, root, "rev-parse", "HEAD")
	if source.LockCurrent(ctx, head, "demo") {
		t.Fatal("fixture did not retain a stale post-repair lock")
	}
	store := lifecycleio.NewConfigMapStateStore(client, "demo-sdo", controllerStateConfigMap)
	if _, err := store.Save(ctx, exportState(t, resumed), ""); err != nil {
		t.Fatal(err)
	}
	outReader, outWriter := newRestartClassOutput()
	errReader, errWriter := newRestartClassOutput()
	runCtx, stop := context.WithCancel(ctx)
	exited := make(chan error, 1)
	detector := controllerDetector("health", time.Second, sdk.Finding{})
	go func() {
		exited <- RunWithOptions(runCtx, []sdk.Detector{detector}, RuntimeOptions{Args: []string{"--namespace", "demo", "--control-namespace", "demo-sdo", "--app-root", root, "--application", "demo", "--synthetic-traffic=false", "--state-baseline=false", "--deployed-manifest-path", "deploy/tracing.yaml", "--deployed-commit", base.Commit, "--lease-duration", "3s", "--maintenance-poll-interval", "5ms"}, Client: client, NativeBroker: &native, Dispatcher: &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, Stdout: outWriter, Stderr: errWriter, StateBaselineStartTimeout: 200 * time.Millisecond})
	}()
	defer func() {
		stop()
		select {
		case err := <-exited:
			if err != nil {
				t.Errorf("controller exit: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("resumed controller exceeded cancellation bound")
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !outReader.has(`{"controller_maintenance":"paused"`) && time.Now().Before(deadline) {
		select {
		case err := <-exited:
			t.Fatalf("startup exited: %v\n%s", err, errReader)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !outReader.has(`{"controller_maintenance":"paused"`) {
		t.Fatalf("missing paused startup: %s\n%s", outReader, errReader)
	}
	if _, err := client.CoreV1().ConfigMaps("demo-sdo").Update(ctx, maintenanceConfigMap("demo-sdo", "active", "rendered-closeout"), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		head = nativeGit(t, root, "rev-parse", "HEAD")
		if source.LockCurrent(ctx, head, "demo") {
			ledger, err := native.readLedger(ack.Receipt.IncidentID)
			if err != nil || !ledger.Acknowledged {
				t.Fatalf("publication lost durable ACK: %v", err)
			}
			restartClassWriteReceipt(t, native, path+".receipt", ack.Receipt.IncidentID)
			data, err := os.ReadFile(path + ".receipt")
			if err != nil {
				t.Fatal(err)
			}
			restartClassStrictReceipt(t, ctx, data)
			t.Logf("slot-2 source annotation resumed after ACK: lock published; broker acknowledged; diagnostics=%s", errReader)
			return
		}
		select {
		case err := <-exited:
			t.Fatalf("resumed process exited before lock publication: %v\n%s", err, errReader)
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("controller acknowledged closure but did not publish a current rendered source lock; diagnostics=%s stdout=%s", errReader, outReader)
}

// lockedLines is the existing race-safe runtime output fixture. Its string
// writer avoids exposing a concurrently mutated bytes.Buffer to the driver.
type restartClassOutput struct{ lines *lockedLines }

func (w restartClassOutput) Write(p []byte) (int, error) {
	w.lines.mu.Lock()
	defer w.lines.mu.Unlock()
	w.lines.lines = append(w.lines.lines, string(p))
	return len(p), nil
}
func newRestartClassOutput() (*lockedLines, restartClassOutput) {
	lines := &lockedLines{}
	return lines, restartClassOutput{lines: lines}
}

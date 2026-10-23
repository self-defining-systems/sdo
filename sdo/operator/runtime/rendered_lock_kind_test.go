package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type lockKindResponder struct {
	source GitPlanSource
	client kubernetes.Interface
	calls  atomic.Int32
}

func (r *lockKindResponder) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "lk1", Name: "api"}
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{Source: r.source}, 1)
	response := loop.Handle(ctx, ToolRequest{Command: "source", Ref: &ref})
	if response.Error != "" {
		return IncidentResult{}, fmt.Errorf("scripted sdo source: %s", response.Error)
	}
	data, _ := json.Marshal(response.Source)
	var accepted corev1.Service
	if err := json.Unmarshal(data, &accepted); err != nil {
		return IncidentResult{}, err
	}
	live, err := r.client.CoreV1().Services("lk1").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		return IncidentResult{}, err
	}
	live.Spec.Ports = accepted.Spec.Ports
	if _, err := r.client.CoreV1().Services("lk1").Update(ctx, live, metav1.UpdateOptions{FieldManager: "sdo-responder"}); err != nil {
		return IncidentResult{}, err
	}
	r.calls.Add(1)
	return fastloopGateResult(request, []sdk.ObjectRef{ref}), nil
}

// Level 6 is opt-in and confined to the task's disposable lane. It uses the
// production controller/cache, real SSA ownership, broker and source tool with
// a scripted responder; it spends no model tokens.
func TestRenderedLockKindTwoIncidents(t *testing.T) {
	kubeconfig := os.Getenv("SDO_LK1_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("run scripts/test_rendered_lock_kind.sh on luna-pilot-53")
	}
	raw, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	if raw.CurrentContext != "kind-luna-pilot-53" {
		t.Fatal("LK1 e2e may use only kind-luna-pilot-53")
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "lk1"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.CoreV1().Namespaces().Delete(context.Background(), "lk1", metav1.DeleteOptions{}) })
	repo := t.TempDir()
	manifest := `apiVersion: v1
kind: Service
metadata:
  name: api
spec:
  selector: {app: api}
  ports: [{port: 80, targetPort: 8080}]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 1
  selector:
    matchLabels: {app: api}
  template:
    metadata:
      labels: {app: api}
    spec:
      initContainers:
      - name: setup
        image: busybox:1.36
        imagePullPolicy: IfNotPresent
        command: [sh, -c, "true"]
      containers:
      - name: web
        image: busybox:1.36
        imagePullPolicy: IfNotPresent
        command: [sh, -c, "mkdir -p /www; echo lk1 > /www/index.html; httpd -f -p 8080 -h /www"]
        ports: [{containerPort: 8080}]
        readinessProbe:
          tcpSocket: {port: 8080}
          periodSeconds: 1
`
	if err := os.WriteFile(filepath.Join(repo, "app.yaml"), []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init"}, {"config", "user.name", "Test"}, {"config", "user.email", "test@example.com"}, {"add", "."}, {"commit", "-qm", "synthetic source"}} {
		nativeGit(t, repo, args...)
	}
	revision := nativeGit(t, repo, "rev-parse", "HEAD")
	tree := nativeGit(t, repo, "rev-parse", "HEAD^{tree}")
	source := GitPlanSource{Repository: repo, ManifestPaths: []string{"app.yaml"}, DeployedCommit: revision}
	objects, err := source.RawManifests(ctx, revision, "lk1")
	if err != nil {
		t.Fatal(err)
	}
	var input bytes.Buffer
	for _, o := range objects {
		if _, _, err := rendered.RenderManifest(o.Object, "lk1", tree, nil); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(o.Object)
		input.Write(data)
		input.WriteByte('\n')
	}
	command := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "apply", "--server-side", "--field-manager=sdo-deployer", "-f", "-")
	command.Stdin = &input
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("apply: %s %v", output, err)
	}
	if output, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "-n", "lk1", "rollout", "status", "deployment/api", "--timeout=90s").CombinedOutput(); err != nil {
		t.Fatalf("rollout: %s %v", output, err)
	}
	authority, err := broker.New(repo, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	responder := &lockKindResponder{source: source, client: client}
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Client: client, Namespace: "lk1", TrackDrift: true}, []sdk.Detector{fastloopDetector{}})
	if err != nil {
		t.Fatal(err)
	}
	cache.Start(ctx)
	defer cache.Stop()
	if err := cache.WaitForSync(ctx); err != nil {
		t.Fatal(err)
	}
	newController := func(state *Controller) (*Controller, error) {
		c, err := NewController(ControllerConfig{Application: "lk1", Namespace: "lk1", SourceCommit: revision, DeployedCommit: revision, RepositoryWorktree: repo, FiringThreshold: 1, ClearThreshold: 1, RepairPolicy: "recorded-actions", ResponseTimeout: 30 * time.Second, VerificationTimeout: 30 * time.Second}, []sdk.Detector{fastloopDetector{}}, cache, responder, time.Now())
		if err != nil {
			return nil, err
		}
		if state != nil {
			saved, err := state.ExportState()
			if err != nil {
				return nil, err
			}
			if err := c.RestoreState(saved); err != nil {
				return nil, err
			}
		}
		c.ReconcileRendered = func(ctx context.Context, revision string) (string, error) {
			return (RenderedLockReconciler{Source: source, Client: client, Broker: authority}).Reconcile(ctx, revision, "lk1")
		}
		c.ConfigureDrift(DriftControllerConfig{Events: cache, Source: source, Client: client, BatchWindow: time.Millisecond, View: func(snapshot sdk.DetectionContext) program.ReadView {
			return LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: c.CurrentAcceptedCommit()}
		}})
		c.OnError = func(err error) { t.Log(err) }
		return c, nil
	}
	controller, err := newController(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.EvaluateAll(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	if controller.CurrentAcceptedCommit() == revision {
		t.Fatal("controller did not publish post-rollout lock")
	}
	data, err := os.ReadFile(filepath.Join(repo, rendered.LockPath))
	if err != nil {
		t.Fatal(err)
	}
	var lock rendered.Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		t.Fatal(err)
	}
	foundInit := false
	for _, entry := range lock.Objects {
		if entry.Error != "" {
			t.Fatalf("unexpected real-cluster lock gap: %s", entry.Error)
		}
		for _, image := range entry.Images {
			if image.Init && strings.Contains(image.Digest, "@sha256:") {
				foundInit = true
			}
		}
	}
	if !foundInit {
		t.Fatal("real init-container digest missing")
	}
	lockTree := nativeGit(t, repo, "rev-parse", "HEAD:.sdo/rendered")
	closed := 0
	for incident := 0; incident < 2; incident++ {
		if incident == 1 {
			proposal, err := authority.CreateProposalForPhase(ctx, "between-incidents", broker.InitialPhase)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(proposal.Path, ".sdo/goal.md"), []byte("---\nschema_version: 1\nowner: human\napplication: lk1\n---\nKeep serving\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := authority.Publish(ctx, "between-incidents", broker.Human, broker.InitialPhase); err != nil {
				t.Fatal(err)
			}
			// A process restart discards volatile image evidence between incidents.
			controller, err = newController(controller)
			if err != nil {
				t.Fatal(err)
			}
		}
		controller.OnIncidentClosed = func(IncidentClosure) { closed++ }
		live, err := client.CoreV1().Services("lk1").Get(ctx, "api", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		live.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
		if _, err := client.CoreV1().Services("lk1").Update(ctx, live, metav1.UpdateOptions{FieldManager: "lk1-fault"}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(35 * time.Second)
		for closed <= incident && time.Now().Before(deadline) {
			if err := controller.StepEvents(ctx, time.Now(), cache.TakeEvents()); err != nil {
				t.Fatal(err)
			}
			if err := executePendingEffects(ctx, controller); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if closed != incident+1 || responder.calls.Load() != int32(incident+1) {
			t.Fatalf("incident %d failed: closures=%d calls=%d state=%+v", incident, closed, responder.calls.Load(), exportState(t, controller))
		}
		if got := nativeGit(t, repo, "rev-parse", "HEAD:.sdo/rendered"); got != lockTree {
			t.Fatal("incident or memory commit rewrote lock")
		}
		// Service API/informer convergence precedes kube-proxy propagation.
		// Independently require actual route recovery within a bounded window.
		var output []byte
		var routeErr error
		routeDeadline := time.Now().Add(10 * time.Second)
		for {
			output, routeErr = exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "-n", "lk1", "exec", "deployment/api", "-c", "web", "--", "wget", "-T", "2", "-qO-", "http://api").CombinedOutput()
			if routeErr == nil && strings.TrimSpace(string(output)) == "lk1" {
				break
			}
			if time.Now().After(routeDeadline) {
				t.Fatalf("real Service route not restored: %s %v", output, routeErr)
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Logf("incident %d: source repair, route recovery, closure, unchanged lock %s", incident+1, lockTree)
	}
	foundDrift := false
	for _, event := range exportState(t, controller).DriftEvents {
		if event.Classification == string(sdk.DriftUnexplained) {
			foundDrift = true
		}
	}
	if !foundDrift {
		t.Fatal("real informer Service drift never reached controller")
	}
}

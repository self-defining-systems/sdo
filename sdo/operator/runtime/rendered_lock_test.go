package runtime

import (
	"context"
	"encoding/json"
	"errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"os"
	"path/filepath"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

func TestRenderedLockAfterRolloutAndRestart(t *testing.T) {
	source, _, commit, deployed, service := acceptedSourceDeployment(t, true)
	commit = withoutRenderedLock(t, source)
	deployed.Spec.Template.Spec.Containers[0].Image = "consul@sha256:" + strings.Repeat("b", 64)
	deployed.Generation = 1
	deployed.Status.ObservedGeneration = 0
	deployed.Status.ReadyReplicas = 1
	deployed.Status.UpdatedReplicas = 1
	deployed.Status.AvailableReplicas = 1
	replicas := int32(1)
	deployed.Spec.Replicas = &replicas
	client := fake.NewSimpleClientset(deployed, service)
	authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	commit = publishTestDeploymentEvidence(t, authority, source, deployed, nil)
	reconciler := RenderedLockReconciler{Source: source, Client: client, Broker: authority}
	if _, err := reconciler.Reconcile(context.Background(), commit, "app"); err == nil {
		t.Fatal("lock written before rollout observation")
	}
	if _, err := os.Stat(filepath.Join(source.Repository, rendered.LockPath)); !os.IsNotExist(err) {
		t.Fatal("lock exists before rollout")
	}
	// Crash between deploy and lock write: rebuild the reconciler with no FX1 baseline.
	deployed.Status.ObservedGeneration = 1
	if _, err := client.AppsV1().Deployments("app").UpdateStatus(context.Background(), deployed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	source.Images = nil
	reconciler = RenderedLockReconciler{Source: source, Client: client, Broker: authority}
	locked, err := reconciler.Reconcile(context.Background(), commit, "app")
	if err != nil {
		t.Fatal(err)
	}
	object, err := source.AcceptedManifest(context.Background(), locked, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := object.MarshalJSON()
	if !strings.Contains(string(data), "example/setup@sha256:"+strings.Repeat("a", 64)) || !strings.Contains(string(data), "consul@sha256:"+strings.Repeat("b", 64)) {
		t.Fatalf("trusted regular/init apply images missing: %s", data)
	}
	// Lost reply after Git publication: retrying the pre-lock revision recovers
	// the accepted lock instead of generating another commit or stalling.
	recovered, err := reconciler.Reconcile(context.Background(), commit, "app")
	if err != nil || recovered != locked {
		t.Fatalf("lost lock reply failed recovery: %s %v", recovered, err)
	}
	before := nativeGit(t, source.Repository, "rev-parse", "HEAD:.sdo/rendered")
	if err := os.MkdirAll(filepath.Join(source.Repository, ".sdo"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source.Repository, ".sdo/goal.md"), []byte("memory only\n"), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".")
	nativeGit(t, source.Repository, "commit", "-qm", "memory")
	memory := nativeGit(t, source.Repository, "rev-parse", "HEAD")
	next, err := reconciler.Reconcile(context.Background(), memory, "app")
	if err != nil || next != memory {
		t.Fatalf("memory commit regenerated lock: %s %v", next, err)
	}
	if after := nativeGit(t, source.Repository, "rev-parse", "HEAD:.sdo/rendered"); after != before {
		t.Fatal("memory changed lock bytes")
	}
	// A changed manifest must never serve the previous lock.
	path := filepath.Join(source.Repository, "app.yaml")
	raw, _ := os.ReadFile(path)
	if err := os.WriteFile(path, []byte(strings.Replace(string(raw), "8080", "8081", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".")
	nativeGit(t, source.Repository, "commit", "-qm", "manifest")
	changed := nativeGit(t, source.Repository, "rev-parse", "HEAD")
	if _, err := source.AcceptedManifest(context.Background(), changed, sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}); err == nil {
		t.Fatal("stale lock served after source change")
	}
	if _, err := reconciler.Reconcile(context.Background(), changed, "app"); err == nil {
		t.Fatal("lock regenerated before changed source rolled out")
	}
	// Workload annotation must identify the new deployment before regeneration.
	deployed.Annotations["sdo.dev/source-tree"] = nativeGit(t, source.Repository, "rev-parse", "HEAD^{tree}")
	deployed.Spec.Template.Annotations["sdo.dev/source-tree"] = deployed.Annotations["sdo.dev/source-tree"]
	if _, err := client.AppsV1().Deployments("app").Update(context.Background(), deployed, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	next, err = reconciler.Reconcile(context.Background(), changed, "app")
	if err != nil || next == changed {
		t.Fatalf("manifest lock not regenerated: %s %v", next, err)
	}
}

func TestRenderedLockMissingIsEvidenceGap(t *testing.T) {
	source, commit := foreignSource(t)
	// Delete the lock from a new accepted revision, preserving source inventory.
	_ = os.RemoveAll(filepath.Join(source.Repository, ".sdo/rendered"))
	nativeGit(t, source.Repository, "add", "-A")
	nativeGit(t, source.Repository, "commit", "--allow-empty", "-qm", "missing lock")
	commit = nativeGit(t, source.Repository, "rev-parse", "HEAD")
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	if _, err := source.AcceptedManifest(context.Background(), commit, ref); err == nil {
		t.Fatal("missing lock silently rendered source")
	}
}

// Test fixtures publish a synthetic controller lock separately from source.
func seedRenderedLock(t *testing.T, source GitPlanSource, revision, namespace string) string {
	t.Helper()
	snapshot, err := source.BuildLock(context.Background(), revision, namespace)
	if err != nil {
		t.Fatal(err)
	}
	proof := rendered.DeploymentEvidence{SourceTree: revision}
	for _, entry := range snapshot.Lock.Objects {
		if entry.Error != "" {
			continue
		}
		for _, image := range entry.Images {
			applied := image.Digest
			if source.Images != nil {
				if value, ok := source.Images.BaselineImage(entry.Ref, image.Name, image.Init); ok {
					applied = value
				}
			}
			proof.Images = append(proof.Images, rendered.DeployedImage{Ref: entry.Ref, Name: image.Name, Init: image.Init, Applied: applied, Digest: image.Digest})
		}
	}
	if err := snapshot.ValidateWithEvidence(context.Background(), source.Repository, revision, []rendered.DeploymentEvidence{proof}); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Write(source.Repository); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".sdo/rendered")
	nativeGit(t, source.Repository, "commit", "-qm", "synthetic controller lock")
	return nativeGit(t, source.Repository, "rev-parse", "HEAD")
}

func removeWorkloadLock(t *testing.T, source GitPlanSource, revision string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(source.Repository, rendered.LockPath))
	if err != nil {
		t.Fatal(err)
	}
	var lock rendered.Lock
	if err := json.Unmarshal(data, &lock); err != nil {
		t.Fatal(err)
	}
	for _, entry := range lock.Objects {
		if entry.Ref.Kind == "Deployment" {
			if err := os.Remove(filepath.Join(source.Repository, rendered.Directory, entry.File)); err != nil {
				t.Fatal(err)
			}
		}
	}
	nativeGit(t, source.Repository, "add", "-A")
	nativeGit(t, source.Repository, "commit", "-qm", "synthetic missing workload evidence")
	return nativeGit(t, source.Repository, "rev-parse", "HEAD")
}

func withoutRenderedLock(t *testing.T, source GitPlanSource) string {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(source.Repository, rendered.Directory)); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", "-A")
	nativeGit(t, source.Repository, "commit", "-qm", "synthetic deployment before lock write")
	return nativeGit(t, source.Repository, "rev-parse", "HEAD")
}

func TestRenderedLockPerObjectGaps(t *testing.T) {
	for _, mode := range []string{"missing-file", "missing-index", "render-failure", "corrupt-file"} {
		t.Run(mode, func(t *testing.T) {
			source, _, revision, _, _ := acceptedSourceDeployment(t, true)
			source.Images = nil
			path := filepath.Join(source.Repository, rendered.LockPath)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var lock rendered.Lock
			if err := json.Unmarshal(data, &lock); err != nil {
				t.Fatal(err)
			}
			for i, entry := range lock.Objects {
				if entry.Ref.Kind != "Deployment" {
					continue
				}
				switch mode {
				case "missing-file":
					err = os.Remove(filepath.Join(source.Repository, rendered.Directory, entry.File))
				case "missing-index":
					lock.Objects = append(lock.Objects[:i], lock.Objects[i+1:]...)
				case "corrupt-file":
					err = os.WriteFile(filepath.Join(source.Repository, rendered.Directory, entry.File), []byte("bad object"), 0600)
				case "render-failure":
					snapshot, e := source.BuildLock(context.Background(), revision, "app")
					if e != nil {
						t.Fatal(e)
					}
					if err := snapshot.Write(source.Repository); err != nil {
						t.Fatal(err)
					}
				}
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing-index" {
				data, _ = json.Marshal(lock)
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			nativeGit(t, source.Repository, "add", "-A")
			nativeGit(t, source.Repository, "commit", "-qm", "synthetic object evidence gap")
			revision = nativeGit(t, source.Repository, "rev-parse", "HEAD")
			objects, err := source.AcceptedManifests(context.Background(), revision, "app")
			if err == nil || len(objects) != 1 || objects[0].GetKind() != "Service" {
				t.Fatalf("gap hid good Service: %v %v", objects, err)
			}
			workload := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
			if _, err := source.AcceptedManifest(context.Background(), revision, workload); !errors.Is(err, rendered.ErrEvidenceGap) {
				t.Fatalf("workload gap is not explicit: %v", err)
			}
			service := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
			if _, err := source.AcceptedManifest(context.Background(), revision, service); err != nil {
				t.Fatalf("workload gap poisoned Service source: %v", err)
			}
		})
	}
}

// Reduced L3 sequence, with the affected Service and mutable workload named as
// in the incident: memory publication and lost consul evidence cannot silence
// reservation's targetPort drift.
func TestRenderedLockConsulGapAfterMemoryCommitKeepsReservationDrift(t *testing.T) {
	source, _, _, _, service := acceptedSourceDeployment(t, true)
	path := filepath.Join(source.Repository, "app.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(string(data), "name: api", "name: reservation", 1)), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", "app.yaml")
	nativeGit(t, source.Repository, "commit", "-qm", "synthetic reservation deployment")
	revision := nativeGit(t, source.Repository, "rev-parse", "HEAD")
	source.Images = nil
	revision = seedRenderedLock(t, source, revision, "app") // Explicit mutable consul render gap.
	before := nativeGit(t, source.Repository, "rev-parse", "HEAD:.sdo/rendered")
	if err := os.WriteFile(filepath.Join(source.Repository, ".sdo/goal.md"), []byte("memory between incidents\n"), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", ".sdo/goal.md")
	nativeGit(t, source.Repository, "commit", "-qm", "memory only")
	revision = nativeGit(t, source.Repository, "rev-parse", "HEAD")
	if nativeGit(t, source.Repository, "rev-parse", "HEAD:.sdo/rendered") != before {
		t.Fatal("memory publication changed lock")
	}
	service.Name = "reservation"
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "reservation"}
	accepted, err := source.AcceptedManifest(context.Background(), revision, ref)
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{*service}}, Source: source, SourceCommit: revision}
	event, _, err := (DriftClassifier{}).Classify(context.Background(), ObjectChange{Old: accepted, New: driftObject(service, sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}), Adoption: true}, view, time.Unix(20, 0))
	if err != nil || event == nil || event.Classification != sdk.DriftUnexplained || !hasTargetPortChange(event) {
		t.Fatalf("consul gap hid reservation drift after memory: %+v %v", event, err)
	}
}

func TestRenderedLockPublicationWaitsForClosureAcknowledgment(t *testing.T) {
	now := time.Unix(1000, 0)
	controller := newClosureRetryController(t, ClosureRetryPolicy{}, &rejectingIncidentBroker{}, &now)
	calls := 0
	controller.ReconcileRendered = func(_ context.Context, revision string) (string, error) { calls++; return revision, nil }
	if err := controller.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("lock publication can race the pending repair/outcome/reflection transaction")
	}
	attemptClosure(t, controller)
	if err := controller.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("lock publication ran before closure acknowledgment")
	}
	effect, ok := controller.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("closure acknowledgment missing")
	}
	if err := controller.ExecuteClosureAcknowledgmentEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, controller.acknowledgmentResults)
	if err := controller.Step(context.Background(), now, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("lock publication did not resume after closure acknowledgment: %d", calls)
	}
}

func TestRenderedLockMutableImageReferencesDoNotCreateDrift(t *testing.T) {
	source, _, _, _, service := acceptedSourceDeployment(t, true)
	path := filepath.Join(source.Repository, "consul.yaml")
	manifest := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: consul
spec:
  replicas: 1
  selector:
    matchLabels: {app: consul}
  template:
    metadata:
      labels: {app: consul}
    spec:
      initContainers:
      - name: setup
        image: example/setup:latest
      containers:
      - name: consul
        image: consul:1.15
`
	if err := os.WriteFile(path, []byte(manifest), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, source.Repository, "add", "consul.yaml")
	nativeGit(t, source.Repository, "commit", "-qm", "synthetic mutable deployment")
	revision := nativeGit(t, source.Repository, "rev-parse", "HEAD")
	tree := nativeGit(t, source.Repository, "rev-parse", "HEAD^{tree}")
	source.Images = nil
	source.DeployedCommit = revision
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
	raw, err := source.Revision(context.Background(), ref, revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := deployment.RenderManifest(raw.Object, "app", tree, nil); err != nil {
		t.Fatal(err)
	}
	live := &appsv1.Deployment{}
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, live); err != nil {
		t.Fatal(err)
	}
	live.Generation = 1
	live.Status.ObservedGeneration = 1
	live.Status.ReadyReplicas = 1
	live.Status.UpdatedReplicas = 1
	live.Status.AvailableReplicas = 1
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "consul-pod", Namespace: "app", Labels: map[string]string{"app": "consul"}, Annotations: map[string]string{"sdo.dev/source-tree": tree}}, Spec: live.Spec.Template.Spec, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "consul", ImageID: "docker-pullable://consul@sha256:" + strings.Repeat("b", 64)}}, InitContainerStatuses: []corev1.ContainerStatus{{Name: "setup", ImageID: "docker-pullable://example/setup@sha256:" + strings.Repeat("a", 64)}}}}
	authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	revision = publishTestDeploymentEvidence(t, authority, source, live, pod)
	revision, err = (RenderedLockReconciler{Source: source, Client: fake.NewSimpleClientset(live, pod, service), Broker: authority}).Reconcile(context.Background(), revision, "app")
	if err != nil {
		t.Fatal(err)
	}
	view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{*live}}, Source: source, SourceCommit: revision}
	if diff, err := view.DiffAgainstSource(ref); err != nil || diff.Changed {
		t.Fatalf("deployed mutable references created false drift: %+v %v", diff, err)
	}
	for _, init := range []bool{false, true} {
		changed := live.DeepCopy()
		if init {
			changed.Spec.Template.Spec.InitContainers[0].Image = "example/setup:changed"
		} else {
			changed.Spec.Template.Spec.Containers[0].Image = "consul:changed"
		}
		view.DetectionContext = sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{*changed}}
		if diff, err := view.DiffAgainstSource(ref); err != nil || !diff.Changed {
			t.Fatalf("changed image reference disappeared (init=%t): %+v %v", init, diff, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(source.Repository, rendered.LockPath))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "consul@sha256:"+strings.Repeat("b", 64)) || !strings.Contains(string(data), "example/setup@sha256:"+strings.Repeat("a", 64)) {
		t.Fatalf("per-container digests were not recorded: %s", data)
	}
}

// publishTestDeploymentEvidence represents the trusted deployer boundary: the
// fixture's supplied apply/rollout images are persisted via real source acceptance.
// It must be called before any injected mutation, never from the reconciler.
func publishTestDeploymentEvidence(t *testing.T, authority *broker.Broker, source GitPlanSource, live *appsv1.Deployment, pod *corev1.Pod) string {
	t.Helper()
	ctx := context.Background()
	proposal, err := authority.CreateProposalForPhase(ctx, "fixture-deployment", broker.InitialPhase)
	if err != nil {
		t.Fatal(err)
	}
	// Add an input so the real broker performs a source publication.
	if err := os.WriteFile(filepath.Join(proposal.Path, "deployment-input.txt"), []byte("trusted synthetic deployment\n"), 0600); err != nil {
		t.Fatal(err)
	}
	tree, err := broker.ProposalTreeDigest(ctx, proposal.Path)
	if err != nil {
		t.Fatal(err)
	}
	live.Annotations["sdo.dev/source-tree"] = tree
	live.Spec.Template.Annotations["sdo.dev/source-tree"] = tree
	if pod != nil {
		pod.Annotations["sdo.dev/source-tree"] = tree
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: live.Namespace, Name: live.Name}
	proof := broker.SourceEvidence{ProposalTreeDigest: tree, ObservedRevision: tree, StrategyID: "raw-manifests", ResourceIDs: []string{"Deployment/" + live.Namespace + "/" + live.Name}, ManifestPaths: source.ManifestPaths, Ready: true}
	for _, init := range []bool{true, false} {
		containers := live.Spec.Template.Spec.Containers
		if init {
			containers = live.Spec.Template.Spec.InitContainers
		}
		for _, c := range containers {
			digest := c.Image
			if pod != nil {
				statuses := pod.Status.ContainerStatuses
				if init {
					statuses = pod.Status.InitContainerStatuses
				}
				for _, status := range statuses {
					if status.Name == c.Name {
						digest = strings.TrimPrefix(status.ImageID, "docker-pullable://")
					}
				}
			}
			proof.Images = append(proof.Images, digest)
			proof.ContainerImages = append(proof.ContainerImages, rendered.DeployedImage{Ref: ref, Name: c.Name, Init: init, Applied: c.Image, Digest: digest})
		}
	}
	receipt, err := authority.PublishVerifiedSource(ctx, "fixture-deployment", proof)
	if err != nil {
		t.Fatal(err)
	}
	return receipt.Commit
}

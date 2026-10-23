package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Properties, before the scheduled transitions:
// P1: memory commits leave the deployment lock byte-for-byte unchanged.
// P2: an unavailable workload object cannot hide an unrelated Service's drift.
// P3: restart after rollout and before publication recovers a lock without an
// in-memory healthy baseline; before publication, source remains a gap.
// P4: pre-publication regular/init mutations never enter accepted image evidence.
func TestSimRenderedLockSchedule(t *testing.T) {
	if seed := os.Getenv("SDO_CHAOS_SEED"); seed != "" {
		parsed, err := strconv.ParseUint(seed, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		runSimRenderedLock(t, parsed)
		return
	}
	sweepSimSeedsN(t, 6, runSimRenderedLock)
}

func runSimRenderedLock(t *testing.T, seed uint64) {
	for _, pinned := range []bool{false, true} {
		for _, mutation := range []string{"none", "regular", "init"} {
			t.Run(fmt.Sprintf("pinned=%t/%s", pinned, mutation), func(t *testing.T) { runSimRenderedLockMutation(t, seed, mutation, pinned) })
		}
	}
}

func runSimRenderedLockMutation(t *testing.T, seed uint64, mutation string, pinned bool) {
	rng := newSimRNG(seed)
	clock := newSimClock()
	source, _, _, deployed, service := acceptedSourceDeployment(t, true)
	revision := withoutRenderedLock(t, source)
	deployed.Spec.Template.Spec.Containers[0].Image = "consul@sha256:" + strings.Repeat("b", 64)
	replicas := int32(1)
	deployed.Spec.Replicas = &replicas
	deployed.Generation = 1
	deployed.Status.ObservedGeneration = 0
	deployed.Status.UpdatedReplicas = 1
	deployed.Status.ReadyReplicas = 1
	deployed.Status.AvailableReplicas = 1
	authority, err := broker.New(source.Repository, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if pinned {
		path := filepath.Join(source.Repository, "consul.yaml")
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		body = []byte(strings.ReplaceAll(strings.ReplaceAll(string(body), "consul:1.15", deployed.Spec.Template.Spec.Containers[0].Image), "example/setup:latest", deployed.Spec.Template.Spec.InitContainers[0].Image))
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		nativeGit(t, source.Repository, "add", "consul.yaml")
		nativeGit(t, source.Repository, "commit", "-qm", "source-pinned deployment")
		revision = nativeGit(t, source.Repository, "rev-parse", "HEAD")
		tree := nativeGit(t, source.Repository, "rev-parse", "HEAD^{tree}")
		deployed.Annotations["sdo.dev/source-tree"] = tree
		deployed.Spec.Template.Annotations["sdo.dev/source-tree"] = tree
		// No named ledger: source pins themselves are authoritative.
	} else {
		revision = publishTestDeploymentEvidence(t, authority, source, deployed, nil)
	}
	client := fake.NewSimpleClientset(deployed, service)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	original, err := source.Revision(context.Background(), ref, revision)
	if err != nil {
		t.Fatal(err)
	}
	faults := drawSimFaults(rng)
	kube := newSimKube(faults, original.DeepCopy())
	cache := newSimCache(kube, faults)
	events := &fixedObjectChanges{}
	makeController := func(previous *Controller) *Controller {
		c, err := NewController(ControllerConfig{Application: "app", Namespace: "app", SourceCommit: revision, DeployedCommit: source.DeployedCommit, FiringThreshold: 1, ClearThreshold: 1, ResponseTimeout: time.Minute, RepairPolicy: "recorded-actions"}, nil, cache, &recordingDispatcher{}, clock.Now())
		if err != nil {
			t.Fatal(err)
		}
		if previous != nil {
			state, err := previous.ExportState()
			if err != nil {
				t.Fatal(err)
			}
			if err := c.RestoreState(state); err != nil {
				t.Fatal(err)
			}
		}
		c.ReconcileRendered = func(ctx context.Context, revision string) (string, error) {
			return (RenderedLockReconciler{Source: source, Client: client, Broker: authority}).Reconcile(ctx, revision, "app")
		}
		c.ConfigureDrift(DriftControllerConfig{Source: source, Events: events, View: func(snapshot sdk.DetectionContext) program.ReadView {
			return LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: c.CurrentAcceptedCommit()}
		}})
		c.OnError = func(err error) { t.Logf("seed=%d t=%s: %v", seed, clock.Now(), err) }
		return c
	}
	controller := makeController(nil)
	// Each restart reconstructs the actual controller from exported state and
	// drops FX1's volatile image evidence, including before the lock is written.
	writeTick := 2 + rng.IntN(4)
	var lockTree string
	for tick := 0; tick < writeTick+5; tick++ {
		clock.now = clock.now.Add(time.Second)
		if tick <= writeTick || rng.IntN(2) == 0 {
			source.Images = nil
			controller = makeController(controller)
		}
		if tick == writeTick {
			if mutation != "none" {
				image := "unrelated/incident@sha256:" + strings.Repeat("c", 64)
				if mutation == "init" {
					deployed.Spec.Template.Spec.InitContainers[0].Image = image
				} else {
					deployed.Spec.Template.Spec.Containers[0].Image = image
				}
				deployed.Generation++
			}
			deployed.Status.ObservedGeneration = deployed.Generation
			if _, err := client.AppsV1().Deployments("app").UpdateStatus(context.Background(), deployed, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		if tick <= writeTick {
			if err := controller.Step(context.Background(), clock.Now(), nil); err != nil {
				t.Fatal(err)
			}
		}
		if tick < writeTick {
			if _, err := source.AcceptedManifest(context.Background(), revision, ref); err == nil {
				t.Fatal("P3: pre-publication source claimed evidence")
			}
			continue
		}
		if tick == writeTick {
			revision = controller.CurrentAcceptedCommit()
			lockTree = nativeGit(t, source.Repository, "rev-parse", "HEAD:.sdo/rendered")
			workload := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "consul"}
			view := LiveReadView{DetectionContext: sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{*deployed}}, Source: source, SourceCommit: revision}
			if diff, err := view.DiffAgainstSource(workload); err != nil || diff.Changed != (mutation != "none") {
				t.Fatalf("P4 seed=%d mutation=%s: accepted image drift=%+v error=%v", seed, mutation, diff, err)
			}
		} else {
			if err := os.WriteFile(filepath.Join(source.Repository, ".sdo/goal.md"), []byte(fmt.Sprintf("memory tick %d", tick)), 0600); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, source.Repository, "add", ".sdo/goal.md")
			nativeGit(t, source.Repository, "commit", "-qm", "memory between incidents")
			revision = nativeGit(t, source.Repository, "rev-parse", "HEAD")
			if err := controller.Step(context.Background(), clock.Now(), nil); err != nil {
				t.Fatal(err)
			}
			next := controller.CurrentAcceptedCommit()
			if err != nil || next != revision || nativeGit(t, source.Repository, "rev-parse", "HEAD:.sdo/rendered") != lockTree {
				t.Fatalf("P1 seed=%d: memory rewrote lock: %v", seed, err)
			}
		}
	}
	// One object's missing or corrupt YAML is an evidence gap after restart.
	revision = removeWorkloadLock(t, source, revision)
	objects, gap := source.AcceptedManifests(context.Background(), revision, "app")
	if gap == nil || len(objects) != 1 {
		t.Fatalf("P2: partial coverage lost: %v %v", objects, gap)
	}
	current := kube.objects[ref]
	ports, _, _ := unstructuredPorts(current.Object)
	ports[0].(map[string]any)["targetPort"] = int64(9999)
	kube.bump(current)
	events.changes = []ObjectChange{{Old: objects[0], New: current, Adoption: true}}
	if err := controller.Step(context.Background(), clock.Now(), nil); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range exportState(t, controller).DriftEvents {
		if event.Classification == string(sdk.DriftUnexplained) {
			found = true
		}
	}
	if !found {
		t.Fatalf("P2 seed=%d: workload evidence gap hid Service drift", seed)
	}
}

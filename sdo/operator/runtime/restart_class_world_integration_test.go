//go:build integration

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/worker_registry/registrytest"
)

func restartClassTracingWorld() restartClassWorld {
	return restartClassWorld{
		Application: "tracing", Namespace: "demo", ManifestPaths: []string{"deploy/tracing.yaml"}, EffectPath: "README.md",
		Live: func(*testing.T, string, string) []kruntime.Object {
			replicas := int32(1)
			return []kruntime.Object{&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "jaeger", Namespace: "demo", Generation: 1}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}, Status: appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}}}
		},
		Coordinator: func(t *testing.T, root, proposals, state string) lifecycle.Coordinator {
			coordinator, _ := crashCoordinator(t, root, proposals, state, "")
			return coordinator
		},
	}
}

// restartClassSlot2World is the social slot-2 source repair reduced to one
// Deployment: the accepted manifest keeps the source-restoration annotation.
func restartClassSlot2World() restartClassWorld {
	world := restartClassTracingWorld()
	world.Edit = func(t *testing.T, worktree string) error {
		path := filepath.Join(worktree, "deploy/tracing.yaml")
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		body := strings.Replace(string(raw), "metadata:\n", "metadata:\n  annotations:\n    sdo.dev/source-restoration: accepted-source\n", 1)
		return os.WriteFile(path, []byte(body), 0600)
	}
	return world
}

func readRealSeedLock(t *testing.T, root, revision string) realSeedLock {
	t.Helper()
	var lock realSeedLock
	raw, err := exec.Command("git", "-C", root, "show", revision+":.sdo/rendered/lock.json").Output()
	if err != nil {
		t.Fatalf("seed has no accepted rendered lock at %s: %v", revision, err)
	}
	if err := json.Unmarshal(raw, &lock); err != nil {
		t.Fatal(err)
	}
	return lock
}

// restartClassWorldFromEnv selects the world a restarted controller process
// observes. The parent exports the real-seed slot; otherwise the synthetic one.
func restartClassWorldFromEnv(t *testing.T, root string) restartClassWorld {
	t.Helper()
	raw := os.Getenv(realSeedSlotEnv)
	if raw == "" {
		return restartClassTracingWorld()
	}
	slot, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatal(err)
	}
	return restartClassSeedWorld(t, slot, root)
}

func restartClassSeedWorld(t *testing.T, slot int, root string) restartClassWorld {
	t.Helper()
	lock := readRealSeedLock(t, root, "HEAD")
	arch, err := exec.Command("git", "-C", root, "show", "HEAD:.sdo/arch.md").Output()
	if err != nil {
		t.Fatal(err)
	}
	application := "seed"
	if match := realSeedApplication.FindSubmatch(arch); match != nil {
		application = string(match[1])
	}
	return restartClassWorld{
		Application: application, Namespace: lock.Namespace, ManifestPaths: lock.ManifestPaths, EffectPath: "kubernetes",
		Edit: func(t *testing.T, worktree string) error {
			// Reuse the production replay's own edit routine rather than a Go copy.
			command := exec.Command("uv", "run", "python", "-m", "tests.unit.benchmarks.sregym.adapter.replay_source_edit", "--slot", strconv.Itoa(slot), "--root", worktree)
			command.Dir = "../../.."
			if output, err := command.CombinedOutput(); err != nil {
				return fmt.Errorf("replay slot %d source edit: %v: %s", slot, err, output)
			}
			return nil
		},
		Live: func(t *testing.T, root, commit string) []kruntime.Object {
			// The cluster still runs what the base lock says was deployed, even
			// after a later process has published a newer lock.
			deployed := readRealSeedLock(t, root, commit)
			return realSeedLive(t, root, commit, deployed.SourceCommit, deployed.Namespace, deployed.ManifestPaths)
		},
		Coordinator: func(t *testing.T, root, proposals, state string) lifecycle.Coordinator {
			repository, err := broker.New(root, proposals, func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			registry := registrytest.BuiltIn(t)
			return lifecycle.Coordinator{Broker: repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registry, Workers: seedJudge{registry: registry}}
		},
	}
}

// realSeedLive describes a fully rolled-out application as the deployer left
// it: every workload of the accepted source, ready, stamped with the tree it
// was deployed from.
func realSeedLive(t *testing.T, root, commit, deployed, namespace string, paths []string) []kruntime.Object {
	t.Helper()
	tree := nativeGit(t, root, "rev-parse", deployed+"^{tree}")
	objects, err := (GitPlanSource{Repository: root, ManifestPaths: paths}).RawManifests(context.Background(), commit, namespace)
	if err != nil {
		t.Fatalf("read accepted seed manifests: %v", err)
	}
	live := []kruntime.Object{}
	for _, object := range objects {
		stamp := func(template *metav1.ObjectMeta) {
			if template.Annotations == nil {
				template.Annotations = map[string]string{}
			}
			template.Annotations["sdo.dev/source-tree"] = tree
		}
		switch object.GetKind() {
		case "Deployment":
			item := &appsv1.Deployment{}
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, item); err != nil {
				t.Fatal(err)
			}
			replicas := int32(1)
			if item.Spec.Replicas != nil {
				replicas = *item.Spec.Replicas
			}
			item.Generation, item.Namespace = 1, namespace
			stamp(&item.Spec.Template.ObjectMeta)
			item.Status = appsv1.DeploymentStatus{ObservedGeneration: 1, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas}
			live = append(live, item)
		case "StatefulSet":
			item := &appsv1.StatefulSet{}
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, item); err != nil {
				t.Fatal(err)
			}
			replicas := int32(1)
			if item.Spec.Replicas != nil {
				replicas = *item.Spec.Replicas
			}
			item.Generation, item.Namespace = 1, namespace
			stamp(&item.Spec.Template.ObjectMeta)
			item.Status = appsv1.StatefulSetStatus{ObservedGeneration: 1, Replicas: replicas, ReadyReplicas: replicas, UpdatedReplicas: replicas, CurrentRevision: "r1", UpdateRevision: "r1"}
			live = append(live, item)
		case "DaemonSet":
			item := &appsv1.DaemonSet{}
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, item); err != nil {
				t.Fatal(err)
			}
			item.Generation, item.Namespace = 1, namespace
			stamp(&item.Spec.Template.ObjectMeta)
			item.Status = appsv1.DaemonSetStatus{ObservedGeneration: 1, DesiredNumberScheduled: 1, NumberReady: 1, UpdatedNumberScheduled: 1}
			live = append(live, item)
		case "Job":
			item := &batchv1.Job{}
			if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, item); err != nil {
				t.Fatal(err)
			}
			item.Namespace = namespace
			stamp(&item.Spec.Template.ObjectMeta)
			item.Status.Succeeded = 1
			live = append(live, item)
		}
	}
	return live
}

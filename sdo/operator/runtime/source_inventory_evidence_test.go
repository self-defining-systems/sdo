package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"sdo.dev/operator/core"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk/sdktest"
)

// Every workload has a durable image-evidence gap, as can occur when the
// accepted deployment predates trusted named-container deployment receipts.
// These are synthetic manifests and locks, never captured run artifacts.
func gappedInventorySource(t *testing.T) (GitPlanSource, string, []*unstructured.Unstructured, []k8sruntime.Object) {
	t.Helper()
	repo := t.TempDir()
	testutil.InitGit(t, repo)
	nativeGit(t, repo, "config", "user.email", "fixture@example.test")
	nativeGit(t, repo, "config", "user.name", "Fixture")
	declared, live := hotelInventory()
	var manifest bytes.Buffer
	objects := []k8sruntime.Object{}
	for _, object := range declared {
		data, err := json.Marshal(object.Object)
		if err != nil {
			t.Fatal(err)
		}
		manifest.Write(data)
		manifest.WriteString("\n---\n")
	}
	for _, object := range live {
		objects = append(objects, typedInventoryObject(t, object))
	}
	if err := os.WriteFile(filepath.Join(repo, "app.yaml"), manifest.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	nativeGit(t, repo, "add", "app.yaml")
	nativeGit(t, repo, "commit", "-qm", "synthetic source")
	revision := nativeGit(t, repo, "rev-parse", "HEAD")
	source := GitPlanSource{Repository: repo, ManifestPaths: []string{"app.yaml"}}
	// BuildLock records the missing image evidence, with no fabricated digest.
	revision = seedRenderedLock(t, source, revision, "app")
	for i := 0; i < 20; i++ {
		ref := inventoryRef(declared[i*3])
		if _, err := source.AcceptedManifest(context.Background(), revision, ref); !errors.Is(err, rendered.ErrEvidenceGap) {
			t.Fatalf("fixture lacks workload image gap for %s: %v", ref.Name, err)
		}
	}
	return source, revision, live, objects
}

func typedInventoryObject(t *testing.T, object *unstructured.Unstructured) k8sruntime.Object {
	t.Helper()
	typed, err := scheme.Scheme.New(object.GroupVersionKind())
	if err != nil {
		t.Fatal(err)
	}
	if err := k8sruntime.DefaultUnstructuredConverter.FromUnstructured(object.Object, typed); err != nil {
		t.Fatal(err)
	}
	return typed
}

func TestSourceInventoryEvaluateOnceWithImageEvidenceGaps(t *testing.T) {
	for _, latent := range []bool{false, true} {
		t.Run(fmt.Sprint(latent), func(t *testing.T) {
			source, _, _, objects := gappedInventorySource(t)
			want := 0
			if latent {
				for _, object := range latentInventoryObjects() {
					objects = append(objects, typedInventoryObject(t, object))
				}
				want = 5
			}
			client := fake.NewSimpleClientset(objects...)
			provider := &core.KubernetesSnapshotProvider{Client: client, Namespace: "app"}
			path := filepath.Join(t.TempDir(), "full.json")
			var output, stderr bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := evaluateOnce(ctx, nil, []string{"--namespace", "app", "--app-root", source.Repository, "--deployed-manifest-path", "app.yaml", "--source-drift-report", path}, &output, &stderr, provider); err != nil {
				t.Fatalf("image evidence prevented object inventory: %v", err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var report struct {
				Healthy      bool                            `json:"healthy"`
				Findings     []json.RawMessage               `json:"findings"`
				EvidenceGaps []rendered.InventoryEvidenceGap `json:"evidence_gaps"`
			}
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			if len(report.Findings) != want || report.Healthy != !latent || len(report.EvidenceGaps) != 20 {
				t.Fatalf("inventory conflated image gaps and object drift: %s", data)
			}
			for _, gap := range report.EvidenceGaps {
				if gap.Ref.Name == "" || gap.Kind != "rendered_object" || gap.Reason == "" {
					t.Fatalf("untyped gap: %+v", gap)
				}
			}
			if !strings.Contains(output.String(), "20 evidence gaps") {
				t.Fatal(output.String())
			}
			for _, action := range client.Actions() {
				if action.GetResource().Resource == "secrets" {
					t.Fatal("read Secret data")
				}
				if action.GetVerb() != "get" && action.GetVerb() != "list" && action.GetVerb() != "watch" {
					t.Fatalf("inventory mutated Kubernetes: %+v", action)
				}
			}
		})
	}
}

func TestSourceInventoryControllerClosureWithImageEvidenceGaps(t *testing.T) {
	source, revision, live, _ := gappedInventorySource(t)
	start := time.Unix(100, 0).UTC()
	config := testControllerConfig()
	config.Namespace, config.SourceCommit = "app", revision
	c, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, start)
	if err != nil {
		t.Fatal(err)
	}
	store := &inventoryStore{objects: live}
	c.ConfigureDrift(DriftControllerConfig{Inventory: true, Source: source, Events: store})
	var visibleGaps []error
	c.OnError = func(err error) { visibleGaps = append(visibleGaps, err) }
	if err := c.prepareSourceInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(visibleGaps) != 20 || c.sourceInventory == nil || len(c.sourceInventory.EvidenceGaps) != 20 {
		t.Fatalf("controller hid evidence gaps: %v", visibleGaps)
	}
	for _, err := range visibleGaps {
		var gap rendered.InventoryEvidenceGap
		if !errors.As(err, &gap) || !errors.Is(err, rendered.ErrEvidenceGap) {
			t.Fatalf("untyped controller evidence gap: %v", err)
		}
	}
	if err := c.prepareSourceInventory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(visibleGaps) != 20 {
		t.Fatal("cached inventory reloaded evidence gaps")
	}
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "fixture", Namespace: "app"}
	check := func(want int) {
		t.Helper()
		clear, findings := c.verifyClosureDrift(context.Background(), start, sdktest.Snapshot{NamespaceName: "app"})
		if clear != (want == 0) || len(findings) != want {
			t.Fatalf("closure clear=%v findings=%+v; want %d", clear, findings, want)
		}
		if want > 0 && !inventoryFinding(findings[0]) {
			t.Fatalf("blocked by evidence gap instead of latent drift: %+v", findings)
		}
	}
	check(0)
	store.objects = append(live, latentInventoryObjects()...)
	c.observeSourceInventory(start)
	if len(c.sourceInventoryFindings) != 5 {
		t.Fatalf("image gaps hid proactive latent detection: %+v", c.sourceInventoryFindings)
	}
	check(5)
	store.objects = live
	check(0)
}

func TestSourceInventoryEvidenceGapsNeverAuthorizeImageComparison(t *testing.T) {
	source, revision, _, _ := gappedInventorySource(t)
	inventory, err := source.AcceptedInventory(context.Background(), revision, "app")
	if err != nil || len(inventory.Objects) != 60 || len(inventory.EvidenceGaps) != 20 {
		t.Fatalf("identity inventory=%+v error=%v", inventory, err)
	}
	if _, err := source.AcceptedManifests(context.Background(), revision, "app"); !errors.Is(err, rendered.ErrEvidenceGap) {
		t.Fatalf("inventory authorized incomplete rendered fields: %v", err)
	}
	for _, mode := range []string{"stale", "missing", "selection"} {
		t.Run(mode, func(t *testing.T) {
			candidate := source
			commit := revision
			switch mode {
			case "stale":
				if err := os.WriteFile(filepath.Join(source.Repository, "build-input.txt"), []byte("new application inputs"), 0600); err != nil {
					t.Fatal(err)
				}
				nativeGit(t, source.Repository, "add", "build-input.txt")
				nativeGit(t, source.Repository, "commit", "-qm", "application changes")
				commit = nativeGit(t, source.Repository, "rev-parse", "HEAD")
			case "missing":
				var lock rendered.Lock
				data, err := os.ReadFile(filepath.Join(source.Repository, rendered.LockPath))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &lock); err != nil {
					t.Fatal(err)
				}
				commit = lock.SourceCommit
			case "selection":
				candidate.ManifestPaths = []string{"other.yaml"}
			}
			inventory, err := candidate.AcceptedInventory(context.Background(), commit, "app")
			if !errors.Is(err, rendered.ErrEvidenceGap) || len(inventory.Objects) != 0 {
				t.Fatalf("invalid source authority became usable: %+v %v", inventory, err)
			}
		})
	}
}

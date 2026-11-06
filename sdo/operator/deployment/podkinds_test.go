package deployment

import (
	"encoding/json"
	"strings"
	"testing"

	"sdo.dev/operator/rendered"
)

const podDigest = "r/app@sha256:0000000000000000000000000000000000000000000000000000000000000000"

// podKindObject builds a document of kind with one init and one regular
// container at the location the pod-template table names.
func podKindObject(t *testing.T, kind string) map[string]any {
	t.Helper()
	path, ok := rendered.PodSpecPath(kind)
	if !ok {
		t.Fatalf("%s is not in the pod template table", kind)
	}
	podSpec := map[string]any{
		"initContainers": []any{map[string]any{"name": "i", "image": "init:1"}},
		"containers":     []any{map[string]any{"name": "c", "image": "app:1"}},
	}
	var nested any = podSpec
	for i := len(path) - 1; i >= 0; i-- {
		nested = map[string]any{path[i]: nested}
	}
	doc := nested.(map[string]any)
	doc["kind"] = kind
	doc["metadata"] = map[string]any{"name": "x", "namespace": "app"}
	return doc
}

// Stamping reaches every pod-bearing kind: the tree lands on the object and on
// the pod it creates, and every container image is substituted.
func TestStampCoversEveryPodKind(t *testing.T) {
	for _, kind := range rendered.PodKinds() {
		t.Run(kind, func(t *testing.T) {
			doc := podKindObject(t, kind)
			images, err := Stamp(doc, "tree1", func(s ImageSite) string { return s.Image + "@pinned" })
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(images, " ") != "init:1@pinned app:1@pinned" {
				t.Fatalf("images = %v", images)
			}
			podMetadata, err := rendered.PodTemplateMetadata(doc, false)
			if err != nil {
				t.Fatal(err)
			}
			for name, metadata := range map[string]map[string]any{"object": doc["metadata"].(map[string]any), "pod": podMetadata} {
				annotations, _ := metadata["annotations"].(map[string]any)
				if annotations["sdo.dev/source-tree"] != "tree1" {
					t.Errorf("%s metadata not stamped: %v", name, metadata)
				}
			}
		})
	}
}

// Observation can attest every pod-bearing kind: a kind in the table that the
// backend cannot read back would fail every deployment containing it.
func TestEveryPodKindIsAttested(t *testing.T) {
	for _, kind := range rendered.PodKinds() {
		t.Run(kind, func(t *testing.T) {
			doc := podKindObject(t, kind)
			if _, err := Stamp(doc, "tree1", func(ImageSite) string { return podDigest }); err != nil {
				t.Fatal(err)
			}
			doc["metadata"].(map[string]any)["uid"] = "u"
			doc["metadata"].(map[string]any)["generation"] = 1
			doc["spec"].(map[string]any)["replicas"] = 1
			doc["status"] = map[string]any{
				"observedGeneration": 1, "readyReplicas": 1, "availableReplicas": 1, "updatedReplicas": 1, "replicas": 1,
				"phase": "Running", "conditions": []any{map[string]any{"type": "Ready", "status": "True"}},
			}
			data, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			got, err := parseObservedWorkload(kind+"/app/x", data)
			if err != nil {
				t.Fatal(err)
			}
			if got.Revision != "tree1" || len(got.ContainerImages) != 2 || strings.Join(got.Images, " ") != podDigest+" "+podDigest {
				t.Fatalf("observation = %+v", got)
			}
		})
	}
}

func TestBarePodIsReadyOnlyWhenRunningReady(t *testing.T) {
	for phase, want := range map[string]bool{"Pending": false, "Running": false, "Succeeded": true} {
		doc := podKindObject(t, "Pod")
		if _, err := Stamp(doc, "tree1", func(ImageSite) string { return podDigest }); err != nil {
			t.Fatal(err)
		}
		doc["metadata"].(map[string]any)["uid"] = "u"
		doc["status"] = map[string]any{"phase": phase}
		data, _ := json.Marshal(doc)
		got, err := parseObservedWorkload("Pod/app/x", data)
		if err != nil || got.Ready != want {
			t.Errorf("%s: ready = %v, %v; want %v", phase, got.Ready, err, want)
		}
	}
}

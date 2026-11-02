package sdktest

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func formFixture(t *testing.T, edits ...func(*Snapshot)) Snapshot {
	t.Helper()
	meta := func(name string) metav1.ObjectMeta { return metav1.ObjectMeta{Namespace: "app", Name: name} }
	view := Snapshot{
		NamespaceName: "app",
		PodList:       []corev1.Pod{{ObjectMeta: meta("web-2")}, {ObjectMeta: meta("web-1"), Status: corev1.PodStatus{Phase: corev1.PodRunning}}},
		ServiceList:   []corev1.Service{{ObjectMeta: meta("web"), Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}}}},
		ConfigMapList: []corev1.ConfigMap{{ObjectMeta: meta("settings"), Data: map[string]string{"mode": "x"}}},
	}
	for _, edit := range edits {
		edit(&view)
	}
	captured, err := CaptureSnapshot(view, "app")
	if err != nil {
		t.Fatal(err)
	}
	return captured
}

func topLevel(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func typedView(t *testing.T, s Snapshot) string {
	t.Helper()
	raw, err := json.Marshal([]any{s.Pods(), s.Services(), s.ConfigMaps(), s.Objects})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// legacyForm writes the version 1 layout older controllers stored: every
// namespace-local object twice, once in objects and once in its typed list.
func legacyForm(t *testing.T, s Snapshot) []byte {
	t.Helper()
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	fields := topLevel(t, raw)
	for key, value := range map[string]any{"object_snapshot_version": 1, "pods": s.PodList, "services": s.ServiceList, "configMaps": s.ConfigMapList, "events": []corev1.Event{}} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		fields[key] = encoded
	}
	legacy, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return legacy
}

func TestCapturedSnapshotStoresEachObjectOnce(t *testing.T) {
	captured := formFixture(t)
	raw, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	fields := topLevel(t, raw)
	for _, typed := range []string{"pods", "services", "configMaps", "events", "replicaSets"} {
		if _, ok := fields[typed]; ok {
			t.Fatalf("compact snapshot repeats typed list %q: %s", typed, raw)
		}
	}
	if string(fields["object_snapshot_version"]) != fmt.Sprint(ObjectsOnlySnapshotVersion) {
		t.Fatalf("version = %s", fields["object_snapshot_version"])
	}
	var loaded Snapshot
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	if got, want := typedView(t, loaded), typedView(t, captured); got != want {
		t.Fatalf("loader lost typed objects:\n got %s\nwant %s", got, want)
	}
	if len(loaded.Pods()) != 2 || loaded.ReadyEndpointCountForService("app", "web") != 0 {
		t.Fatalf("typed reads after load: %+v", loaded.Pods())
	}

	// The old layout of the same snapshot stays readable and is the same snapshot.
	legacy := legacyForm(t, captured)
	var old Snapshot
	if err := json.Unmarshal(legacy, &old); err != nil {
		t.Fatal(err)
	}
	if got, want := typedView(t, old), typedView(t, captured); got != want {
		t.Fatalf("legacy load differs:\n got %s\nwant %s", got, want)
	}
	canonical, err := CanonicalSnapshot(legacy)
	if err != nil || string(canonical) != string(raw) {
		t.Fatalf("canonical legacy form = %s, %v; want %s", canonical, err, raw)
	}
	again, err := CanonicalSnapshot(raw)
	if err != nil || string(again) != string(raw) {
		t.Fatalf("canonical form is not stable: %s %v", again, err)
	}
	newDigest, err := SnapshotDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	oldDigest, err := SnapshotDigest(legacy)
	if err != nil || oldDigest != newDigest || newDigest != fmt.Sprintf("%x", sha256.Sum256(raw)) {
		t.Fatalf("digests old=%s new=%s err=%v", oldDigest, newDigest, err)
	}
}

func TestSnapshotKeepsTypedItemsObjectsCannotRebuild(t *testing.T) {
	captured := formFixture(t)
	captured.PodList = append(captured.PodList, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "typed-only"}})
	raw, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := topLevel(t, raw)["pods"]; !ok {
		t.Fatalf("typed-only item dropped: %s", raw)
	}
	var loaded Snapshot
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	if len(loaded.Pods()) != 3 {
		t.Fatalf("pods after load = %d", len(loaded.Pods()))
	}
}

func TestObjectsOnlySnapshotRejectsTypedLists(t *testing.T) {
	raw := []byte(fmt.Sprintf(`{"object_snapshot_version":%d,"namespace":"app","pods":[{"metadata":{"namespace":"app","name":"p"}}]}`, ObjectsOnlySnapshotVersion))
	var loaded Snapshot
	if err := json.Unmarshal(raw, &loaded); err == nil || !strings.Contains(err.Error(), "typed") {
		t.Fatalf("objects-only snapshot with typed lists accepted: %v", err)
	}
}

func writeCase(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCorpusCaseReferencesFixedSnapshotInsteadOfCopying(t *testing.T) {
	fixed := formFixture(t)
	open := formFixture(t, func(view *Snapshot) { view.PodList = view.PodList[:1] })
	fixedRaw, _ := json.Marshal(fixed)
	openRaw, _ := json.Marshal(open)
	labels := func(openDigest, fixedDigest, openAt string, ids string) []byte {
		return []byte(fmt.Sprintf(`{%s,"provenance":{"incident_id":"incident","open_at":%q,"open_sha256":"%s","fixed_sha256":"%s"}}`, ids, openAt, openDigest, fixedDigest))
	}
	digest := func(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }
	root := t.TempDir()

	compact := filepath.Join(root, "compact")
	writeCase(t, compact, map[string][]byte{"open.json": openRaw, "fixed.json": fixedRaw, "recordings.json": []byte("[]"),
		"labels.json": labels(digest(openRaw), digest(fixedRaw), "detect", `"root_cause_program_ids":["fault"]`)})
	legacy := filepath.Join(root, "legacy")
	openLegacy, fixedLegacy := legacyForm(t, open), legacyForm(t, fixed)
	writeCase(t, legacy, map[string][]byte{"open.json": openLegacy, "fixed.json": fixedLegacy, "healthy.json": fixedLegacy, "recordings.json": []byte("[]"),
		"labels.json": labels(digest(openLegacy), digest(fixedLegacy), "detect", `"root_cause_program_ids":["fault"]`)})
	negative := filepath.Join(root, "negative")
	writeCase(t, negative, map[string][]byte{"fixed.json": fixedRaw, "recordings.json": []byte("[]"),
		"labels.json": labels(digest(fixedRaw), digest(fixedRaw), CapturedFixedNoMatchPhase, `"no_match_program_ids":["fault"]`)})

	var cases []CorpusCase
	for _, dir := range []string{compact, legacy} {
		c, err := LoadCorpusCase(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := ValidateCorpusProvenance(dir); err != nil {
			t.Fatalf("%s: %v", filepath.Base(dir), err)
		}
		cases = append(cases, c)
	}
	for index, c := range cases {
		if typedView(t, c.Healthy) != typedView(t, fixed) || typedView(t, c.Fixed) != typedView(t, fixed) || typedView(t, c.Open) != typedView(t, open) {
			t.Fatalf("case %d snapshots differ from captured ones", index)
		}
	}
	c, err := LoadCorpusCase(negative)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateCorpusProvenance(negative); err != nil {
		t.Fatal(err)
	}
	if typedView(t, c.Open) != typedView(t, fixed) || typedView(t, c.Healthy) != typedView(t, fixed) {
		t.Fatal("negative case open/healthy do not reference fixed")
	}

	// The reference is to the case's own fixed snapshot, never an implied default.
	forged := filepath.Join(root, "forged")
	writeCase(t, forged, map[string][]byte{"fixed.json": fixedRaw, "recordings.json": []byte("[]"),
		"labels.json": labels(digest(openRaw), digest(fixedRaw), "detect", `"root_cause_program_ids":["fault"]`)})
	if err := ValidateCorpusProvenance(forged); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("missing open snapshot accepted as fixed: %v", err)
	}
	empty := filepath.Join(root, "empty")
	writeCase(t, empty, map[string][]byte{"recordings.json": []byte("[]"), "labels.json": []byte(`{}`)})
	if _, err := LoadCorpusCase(empty); err == nil || !strings.Contains(err.Error(), "open.json") {
		t.Fatalf("case without snapshots loaded: %v", err)
	}
}

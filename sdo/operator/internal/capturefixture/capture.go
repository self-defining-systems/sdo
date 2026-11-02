// Package capturefixture builds capture boundary fixtures through the
// production capture path. Tests must not hand-build boundary JSON: a hand-built
// snapshot can carry fields the projection drops (or lack fields it keeps), so a
// consumer test passes while the production path is dead.
package capturefixture

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Boundary returns the bytes the controller writes for one capture boundary:
// CaptureSnapshot (and so ProjectCapturedObject) applied to a live-shaped view.
func Boundary(t testing.TB, view sdk.DetectionContext, namespace string, refs ...sdk.ObjectRef) []byte {
	t.Helper()
	snapshot, err := sdktest.CaptureSnapshot(view, namespace, refs...)
	if err != nil {
		t.Fatalf("capture boundary: %v", err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode capture boundary: %v", err)
	}
	return raw
}

// WriteBoundary writes Boundary's bytes to path, creating its directory.
func WriteBoundary(t testing.TB, path string, view sdk.DetectionContext, namespace string, refs ...sdk.ObjectRef) []byte {
	t.Helper()
	raw := Boundary(t, view, namespace, refs...)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return raw
}

// Raw is the explicit opt-out for a test that needs a shape the capture path
// never writes (malformed, forged, empty or legacy input). The reason names why.
func Raw(t testing.TB, reason string, data []byte) []byte {
	t.Helper()
	if strings.TrimSpace(reason) == "" {
		t.Fatal("capturefixture.Raw requires a reason")
	}
	return data
}

// Recapture decodes a captured boundary, applies edit to its typed view and
// captures it again. Generic objects that duplicate a typed list are dropped
// first, so the edited typed lists stay authoritative.
func Recapture(t testing.TB, raw []byte, edit func(*sdktest.Snapshot)) []byte {
	t.Helper()
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatalf("decode captured boundary: %v", err)
	}
	var typed sdktest.Snapshot
	if err := sdk.DecodeTypedObjects(&typed, snapshot.Objects); err != nil {
		t.Fatalf("decode captured objects: %v", err)
	}
	backed := map[sdk.ObjectRef]bool{}
	for _, object := range sdk.TypedObjects(typed) {
		backed[sdk.ObjectIdentity(object)] = true
	}
	generic := snapshot.Objects[:0]
	for _, object := range snapshot.Objects {
		if !backed[sdk.ObjectIdentity(object)] {
			generic = append(generic, object)
		}
	}
	snapshot.Objects = generic
	edit(&snapshot)
	return Boundary(t, snapshot, snapshot.NamespaceName)
}

// FromView decodes view (snapshot JSON text, or a value that encodes to it) as
// a live-shaped sdktest.Snapshot and captures it like Boundary, in the view's
// own namespace. Use it for compact fixtures; the projection still applies.
func FromView(t testing.TB, view any) []byte {
	t.Helper()
	text, ok := view.(string)
	if !ok {
		raw, err := json.Marshal(view)
		if err != nil {
			t.Fatalf("encode capture view: %v", err)
		}
		text = string(raw)
	}
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal([]byte(text), &snapshot); err != nil {
		t.Fatalf("decode capture view: %v", err)
	}
	return Boundary(t, snapshot, snapshot.NamespaceName)
}

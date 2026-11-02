package capturefixture

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// committedCaptureRoots hold capture fixtures checked into the repository.
var committedCaptureRoots = []string{"../../../../tests/fixtures", "../../../../examples", "../.."}

// TestCommittedCapturesKeepOnlyProjectedFields fails when a committed capture
// carries an object field the capture projection drops: a test reading it would
// rely on a field no production capture has.
func TestCommittedCapturesKeepOnlyProjectedFields(t *testing.T) {
	checked := 0
	for _, root := range committedCaptureRoots {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if path != root && (strings.HasPrefix(entry.Name(), ".") && entry.Name() != ".sdo" || entry.Name() == "node_modules") {
					return filepath.SkipDir
				}
				return nil
			}
			if !boundaryName.MatchString(filepath.ToSlash(path)) || !strings.Contains(filepath.ToSlash(path), "/corpus/") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			var snapshot sdktest.Snapshot
			if err := json.Unmarshal(raw, &snapshot); err != nil {
				t.Errorf("%s: not a capture: %v", path, err)
				return nil
			}
			checked++
			for _, object := range append(sdk.TypedObjects(snapshot), snapshot.Objects...) {
				if !projectionFixedPoint(t, object) {
					t.Errorf("%s: %s %s/%s carries fields the capture projection drops; regenerate it with capturefixture", path, object.GetKind(), object.GetNamespace(), object.GetName())
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if checked == 0 {
		t.Fatal("no committed captures found; the roots moved")
	}
}

func projectionFixedPoint(t *testing.T, object unstructured.Unstructured) bool {
	t.Helper()
	raw, err := json.Marshal(&object)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := sdktest.ProjectCapturedObject(raw)
	if err != nil {
		return false
	}
	var before, after any
	if json.Unmarshal(raw, &before) != nil || json.Unmarshal(projected, &after) != nil {
		return false
	}
	return reflect.DeepEqual(before, after)
}

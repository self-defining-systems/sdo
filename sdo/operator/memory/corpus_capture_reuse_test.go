package memory

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type unavailableCaptureView struct{ sdktest.Snapshot }

func (unavailableCaptureView) CaptureObjects([]sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	return nil, errors.New("the original observation is gone")
}

func TestSealedOpenDetectDoesNotReadAgain(t *testing.T) {
	root, incident, phase, namespace := t.TempDir(), "synthetic", "open@detect", "app"
	snapshot := sdktest.Snapshot{NamespaceName: namespace}
	gone := unavailableCaptureView{snapshot}
	if err := CaptureIncidentSnapshot(root, incident, phase, gone, namespace); err == nil {
		t.Fatal("an unsealed capture must still report an observation failure")
	}
	if err := CaptureIncidentSnapshot(root, incident, phase, snapshot, namespace); err != nil {
		t.Fatal(err)
	}
	dir, err := IncidentCorpusDir(root, incident)
	if err != nil {
		t.Fatal(err)
	}
	recording := sdktest.PrimitiveCall{Method: "DiffAgainstSource", Arguments: []byte("[]"), Result: []byte("{}")}
	if err := AppendCapturedRecording(root, incident, phase, recording); err != nil {
		t.Fatal(err)
	}
	read := func(name string) string {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	before, reads := read(phase+".json"), read(phase+"-recordings.json")
	if reads == "[]" {
		t.Fatal("test requires a nonempty primitive recording")
	}
	if err := CaptureIncidentSnapshot(root, incident, phase, gone, namespace); err != nil {
		t.Fatalf("sealed opening evidence must be reused: %v", err)
	}
	if read(phase+".json") != before || read(phase+"-recordings.json") != reads {
		t.Fatal("reusing the capture changed the original snapshot or primitive evidence")
	}
}

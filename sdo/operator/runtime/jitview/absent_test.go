package jitview

import (
	"encoding/json"
	"errors"
	"io"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The wire carries error text only, so an observed absence must keep its
// identity across it: a program tests errors.Is(err, sdk.ErrObjectAbsent).
func TestObjectAbsenceSurvivesTheWire(t *testing.T) {
	toProgram, fromController := io.Pipe()
	toController, fromProgram := io.Pipe()
	go func() {
		decoder, encoder := json.NewDecoder(toController), json.NewEncoder(fromController)
		var view program.ReadView = program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"})
		for {
			var request Message
			if decoder.Decode(&request) != nil {
				return
			}
			_ = encoder.Encode(read(absentReader{view}, request))
		}
	}()
	view := &View{Input: json.NewDecoder(toProgram), Output: json.NewEncoder(fromProgram)}
	_, err := view.Object(sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "cm"})
	if !errors.Is(err, sdk.ErrObjectAbsent) {
		t.Fatalf("Object = %v, want ErrObjectAbsent across the wire", err)
	}
}

type absentReader struct{ program.ReadView }

func (absentReader) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return nil, errors.Join(sdk.ErrObjectAbsent, errors.New(ref.Name))
}

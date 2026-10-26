package sdktest_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Compile the reflection gap probe against the public detector contract.
var _ = sdk.DetectionContext.DiffAgainstSource

func TestDetectionContextReplaysAcceptedSourceDiff(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "unexpected-binding"}
	args, _ := json.Marshal([]any{ref})
	for _, result := range []string{`{"changed":true,"source_absent":true}`, `{"changed":true,"text":"accepted field differs"}`, `{"changed":false}`} {
		snapshot := sdktest.Snapshot{Readings: []sdktest.PrimitiveCall{{Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(result)}}}
		var view sdk.DetectionContext = snapshot
		first, err := view.DiffAgainstSource(ref)
		if err != nil {
			t.Fatal(err)
		}
		second, err := view.DiffAgainstSource(ref)
		if err != nil || !reflect.DeepEqual(first, second) {
			t.Fatalf("non-deterministic diff: %+v %+v %v", first, second, err)
		}
		if first.Changed != (result != `{"changed":false}`) {
			t.Fatalf("lost source diff: %+v", first)
		}
	}
	if _, err := (sdktest.Snapshot{}).DiffAgainstSource(ref); err == nil {
		t.Fatal("unobserved source diff became clearance")
	}
}

type ignoringSourceDiffDetector struct{}

func (ignoringSourceDiffDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "source-diff"} }
func (ignoringSourceDiffDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	_, _ = view.DiffAgainstSource(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"})
	return nil, nil
}

func TestDetectionContextCannotIgnoreUnavailableSourceDiff(t *testing.T) {
	if _, err := sdk.DetectObserved(context.Background(), ignoringSourceDiffDetector{}, sdktest.Snapshot{}); err == nil {
		t.Fatal("ignored failed source diff cleared detector")
	}
}

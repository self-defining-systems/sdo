package sdktest_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Compile the reflection gap probe against the public detector contract.
var _ = sdk.DetectionContext.DiffAgainstSource

func TestDetectionContextReplaysAcceptedSourceDiff(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "unexpected-binding"}
	args, _ := json.Marshal([]any{ref})

	for _, tc := range []struct {
		name   string
		result string
		want   sdk.SourceDiff
	}{
		{"source absent", `{"changed":true,"source_absent":true}`, sdk.SourceDiff{Changed: true, SourceAbsent: true}},
		{"field changed", `{"changed":true,"text":"accepted field differs"}`, sdk.SourceDiff{Changed: true, Text: "accepted field differs"}},
		{"source restored", `{"changed":false}`, sdk.SourceDiff{}},
		{"absent object removed", `{"changed":false,"source_absent":true}`, sdk.SourceDiff{SourceAbsent: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := sdktest.Snapshot{Readings: []sdktest.PrimitiveCall{{Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(tc.result)}}}
			var view sdk.DetectionContext = snapshot
			first, err := view.DiffAgainstSource(ref)
			if err != nil || first != tc.want {
				t.Fatalf("lost source diff: %+v want %+v: %v", first, tc.want, err)
			}
			second, err := view.DiffAgainstSource(ref)
			if err != nil || !reflect.DeepEqual(first, second) {
				t.Fatalf("non-deterministic diff: %+v %+v %v", first, second, err)
			}
			// Wrappers carrying only the public detector interface retain the read.
			wrapped := struct{ sdk.DetectionContext }{snapshot}
			got, err := program.ReadFrom(wrapped).DiffAgainstSource(ref)
			if err != nil || got != tc.want {
				t.Fatalf("detector-to-program adapter lost source diff: %+v %v", got, err)
			}
		})
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

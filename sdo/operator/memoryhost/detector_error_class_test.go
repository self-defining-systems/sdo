package memoryhost

import (
	"context"
	"fmt"
	"testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

type erroringDetector struct {
	id  string
	err error
}

func (d erroringDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: d.id} }
func (d erroringDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, d.err
}

type panickingDetector struct{ id string }

func (d panickingDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: d.id} }
func (d panickingDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	panic("synthetic")
}

func TestEvaluateReportsTheDetectorErrorClass(t *testing.T) {
	s, err := NewServer("c", "d", []sdk.Detector{
		erroringDetector{"unavailable", fmt.Errorf("no record: %w", sdk.ErrUnavailable)},
		erroringDetector{"invalid", fmt.Errorf("secret: %w", sdk.ErrInvalidRead)},
		erroringDetector{"plain", fmt.Errorf("boom")},
		panickingDetector{"panics"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(&pb.MirrorRequest{Sequence: 1, ResetMirror: true, Complete: true, Namespace: "app"}); err != nil {
		t.Fatal(err)
	}
	response, err := s.Evaluate(context.Background(), &pb.EvaluateRequest{MirrorSequence: 1, DetectorIds: []string{"unavailable", "invalid", "plain", "panics", "missing"}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"unavailable": sdk.ReadClassUnavailable,
		"invalid":     sdk.ReadClassInvalid,
		"plain":       sdk.ReadClassUnavailable,
		"panics":      sdk.ReadClassInvalid,
		"missing":     sdk.ReadClassUnavailable,
	}
	for _, result := range response.Results {
		if result.Error == "" {
			t.Fatalf("%s reported no error", result.DetectorId)
		}
		if result.ErrorClass != want[result.DetectorId] {
			t.Errorf("%s class = %q, want %q (%s)", result.DetectorId, result.ErrorClass, want[result.DetectorId], result.Error)
		}
	}
}

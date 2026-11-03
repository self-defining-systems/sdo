package memoryhost

import (
	"context"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"testing"
)

type mirrorTestHost struct {
	*Server
	closed bool
}

func (h *mirrorTestHost) Ready(ctx context.Context) (*pb.ReadyResponse, error) {
	return h.Server.Ready(ctx, nil)
}
func (h *mirrorTestHost) MirrorFrame(_ context.Context, frame *pb.MirrorRequest) error {
	return h.Apply(frame)
}
func (h *mirrorTestHost) Close() error { h.closed = true; return nil }
func newMirrorTestHost(t *testing.T, commit string) *mirrorTestHost {
	t.Helper()
	s, e := NewServer(commit, "digest", nil)
	if e != nil {
		t.Fatal(e)
	}
	return &mirrorTestHost{Server: s}
}

// testDetector is the one detector the isolation tests register.
type testDetector struct{}

func (testDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "synthetic"} }
func (testDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{{Summary: view.Namespace()}}, nil
}

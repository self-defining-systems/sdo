package runtime

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/rendered"
)

func TestAdoptionErrorKindsKeepPermanentVersusTransient(t *testing.T) {
	now := time.Unix(1000, 0)
	cases := []struct {
		name       string
		err        error
		wantNeeded bool
		wantSkip   bool
		wantError  bool
		wantRetry  bool
	}{
		{"unconfigured skips permanently", &rendered.SourceError{Kind: rendered.SourceUnconfigured, Err: errors.New("no paths")}, false, true, false, false},
		{"invalid is recorded without retry", &rendered.SourceError{Kind: rendered.SourceInvalid, Err: errors.New("dup")}, false, false, true, false},
		{"unavailable backs off", &rendered.SourceError{Kind: rendered.SourceUnavailable, Err: errors.New("git")}, true, false, true, true},
		{"unclassified backs off", errors.New("other"), true, false, true, true},
		{"wrapped kind is honoured", fmt.Errorf("render: %w", &rendered.SourceError{Kind: rendered.SourceUnconfigured, Err: errors.New("x")}), false, true, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Controller{config: ControllerConfig{SourceCommit: "abc"}, adoptionNeeded: true}
			c.recordAdoptionError(now, tc.err)
			s := c.ownershipAdoption
			if c.adoptionNeeded != tc.wantNeeded || (s.SkippedReason != "") != tc.wantSkip || (s.LastError != "") != tc.wantError || (!s.RetryAt.IsZero()) != tc.wantRetry {
				t.Fatalf("needed=%v status=%+v", c.adoptionNeeded, s)
			}
		})
	}
}

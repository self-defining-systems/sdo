package program

import (
	"fmt"
	"sdo.dev/operator/sdk"
	"time"
)

// LogOptions selects one container instance; zero LimitBytes uses the 64 KiB cap.
type LogOptions struct {
	Previous      bool
	SinceTime     time.Time
	SinceDuration time.Duration
	TailLines     *int64
	Timestamps    bool
	LimitBytes    int64
}

func (o LogOptions) Validate() error {
	if o.SinceDuration < 0 || (!o.SinceTime.IsZero() && o.SinceDuration != 0) {
		return fmt.Errorf("logs require nonnegative, mutually exclusive since time or duration")
	}
	if o.TailLines != nil && *o.TailLines < 0 {
		return fmt.Errorf("log tail lines must be nonnegative")
	}
	if o.LimitBytes < 0 || o.LimitBytes > 64*1024 {
		return fmt.Errorf("log byte limit must be between 1 and 65536, or zero for default")
	}
	return nil
}

func (snapshotReadView) LogsWithOptions(sdk.ObjectRef, string, LogOptions) (string, error) {
	return "", fmt.Errorf("logs unavailable in detector context")
}
func (v *checkedReadView) LogsWithOptions(ref sdk.ObjectRef, container string, options LogOptions) (string, error) {
	r, err := v.ReadView.LogsWithOptions(ref, container, options)
	v.check(err)
	return r, err
}

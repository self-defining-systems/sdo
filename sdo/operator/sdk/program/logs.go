package program

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
)

// MaxLogBytes is the most log text one program read returns.
const MaxLogBytes = 64 * 1024

// DefaultLogTailLines is how many of the newest lines a read returns when the
// program names no tail.
const DefaultLogTailLines = 1000

// BoundedLogOptions copies upstream options into a finite read of the newest
// lines. A byte limit sent to Kubernetes would return the OLDEST bytes of the
// window, so the request carries a line tail instead, and the byte bound is
// returned for the reader to apply with NewestLogBytes. The caller's
// LimitBytes, when set, lowers that bound.
func BoundedLogOptions(options corev1.PodLogOptions) (corev1.PodLogOptions, int64, error) {
	o := options.DeepCopy()
	if o.Follow {
		return corev1.PodLogOptions{}, 0, fmt.Errorf("bounded log reads cannot follow")
	}
	if o.SinceSeconds != nil && (*o.SinceSeconds <= 0 || o.SinceTime != nil) {
		return corev1.PodLogOptions{}, 0, fmt.Errorf("logs require positive since seconds, exclusive with since time")
	}
	if o.TailLines != nil && *o.TailLines < 0 {
		return corev1.PodLogOptions{}, 0, fmt.Errorf("log tail lines must be nonnegative")
	}
	limit := int64(MaxLogBytes)
	if o.LimitBytes != nil {
		if *o.LimitBytes <= 0 || *o.LimitBytes > MaxLogBytes {
			return corev1.PodLogOptions{}, 0, fmt.Errorf("log byte limit must be between 1 and %d", MaxLogBytes)
		}
		limit = *o.LimitBytes
	}
	o.LimitBytes = nil
	if o.TailLines == nil {
		tail := int64(DefaultLogTailLines)
		o.TailLines = &tail
	}
	return *o, limit, nil
}

// NewestLogBytes keeps the newest whole lines of data that fit in limit bytes.
func NewestLogBytes(data string, limit int64) string {
	if int64(len(data)) <= limit {
		return data
	}
	data = data[int64(len(data))-limit:]
	if i := strings.IndexByte(data, '\n'); i >= 0 {
		return data[i+1:]
	}
	return ""
}

func (snapshotReadView) LogsWithOptions(sdk.ObjectRef, corev1.PodLogOptions) (string, error) {
	return "", fmt.Errorf("logs unavailable in detector context")
}
func (v *checkedReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	r, err := v.ReadView.LogsWithOptions(ref, options)
	v.check(err)
	return r, err
}

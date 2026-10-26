package program

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
)

// BoundedLogOptions copies upstream options and enforces finite reads of at most 64 KiB.
func BoundedLogOptions(options corev1.PodLogOptions) (corev1.PodLogOptions, error) {
	o := options.DeepCopy()
	if o.Follow {
		return corev1.PodLogOptions{}, fmt.Errorf("bounded log reads cannot follow")
	}
	if o.SinceSeconds != nil && (*o.SinceSeconds <= 0 || o.SinceTime != nil) {
		return corev1.PodLogOptions{}, fmt.Errorf("logs require positive since seconds, exclusive with since time")
	}
	if o.TailLines != nil && *o.TailLines < 0 {
		return corev1.PodLogOptions{}, fmt.Errorf("log tail lines must be nonnegative")
	}
	if o.LimitBytes == nil {
		limit := int64(64 * 1024)
		o.LimitBytes = &limit
	}
	if *o.LimitBytes <= 0 || *o.LimitBytes > 64*1024 {
		return corev1.PodLogOptions{}, fmt.Errorf("log byte limit must be between 1 and 65536")
	}
	return *o, nil
}

func (snapshotReadView) LogsWithOptions(sdk.ObjectRef, corev1.PodLogOptions) (string, error) {
	return "", fmt.Errorf("logs unavailable in detector context")
}
func (v *checkedReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	r, err := v.ReadView.LogsWithOptions(ref, options)
	v.check(err)
	return r, err
}

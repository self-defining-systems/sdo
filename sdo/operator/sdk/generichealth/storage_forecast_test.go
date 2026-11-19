package generichealth

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/usage"
)

const (
	gb = 1e9
	mb = 1e6
	// volumeEvery is the usage observer's sampling cadence.
	volumeEvery = 15 * time.Second
)

// volumeSeries is usage.MaxSamples volume samples at the observer's cadence,
// starting at used bytes and growing at rate(i) bytes per second over interval i.
func volumeSeries(capacity, used float64, rate func(i int) float64) []usage.VolumeSample {
	start := memNow.Add(-time.Duration(usage.MaxSamples-1) * volumeEvery)
	samples := make([]usage.VolumeSample, 0, usage.MaxSamples)
	for i := 0; i < usage.MaxSamples; i++ {
		if i > 0 {
			used += rate(i) * volumeEvery.Seconds()
		}
		samples = append(samples, usage.VolumeSample{At: start.Add(time.Duration(i) * volumeEvery), PVC: "data", CapacityBytes: fp(capacity), UsedBytes: fp(used)})
	}
	return samples
}

func claim(request string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "demo"},
		Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(request)}}},
	}
}

func volumeFindings(t *testing.T, pvc corev1.PersistentVolumeClaim, samples []usage.VolumeSample) []sdk.Finding {
	t.Helper()
	d := NewSaturation(sdk.DetectorSpec{ID: "resource-saturation"}, false)
	s := sdktest.Snapshot{NamespaceName: "demo", PVCList: []corev1.PersistentVolumeClaim{pvc}, Usage: &usage.Result{ObservedAt: memNow, Volumes: []usage.VolumeWindow{{PodUID: "u", PVC: "data", Samples: samples}}}}
	f, err := d.Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// A short burst of writes on a slowly growing volume is not a trend: the
// forecast rate is robust to it, so about 20 GB free at 0.1 MB/s (over two
// days) stays silent even when the last minute wrote at 100 MB/s. A
// least-squares slope over the same samples forecasts the volume full in
// under two hours.
func TestStorageForecastIgnoresAShortBurst(t *testing.T) {
	burst := func(i int) float64 {
		if i >= usage.MaxSamples-4 {
			return 100 * mb
		}
		return 0.1 * mb
	}
	if got := volumeFindings(t, claim("100G"), volumeSeries(100*gb, 74*gb, burst)); len(got) != 0 {
		t.Fatalf("a one-minute burst was extrapolated into a fill forecast: %+v", got)
	}
}

// Sustained growth that fills the volume within the horizon is still
// forecast, as a predicted finding: a trend, not a condition that holds now.
func TestStorageForecastFiresOnSustainedGrowthAsAPrediction(t *testing.T) {
	steady := func(int) float64 { return 4 * mb }
	got := volumeFindings(t, claim("100G"), volumeSeries(100*gb, 70*gb, steady))
	if len(got) != 1 || got[0].RuleID != "pvc-bytes-saturation" {
		t.Fatalf("sustained growth to full within the horizon was not forecast: %+v", got)
	}
	if !sdk.Predicted(got[0]) {
		t.Fatalf("a forecast-only finding is not marked predicted: %+v", got[0])
	}
}

// A forecast needs a window long enough to tell a trend from a burst; a few
// minutes of samples, however steep, are not extrapolated hours ahead.
func TestStorageForecastNeedsALongEnoughWindow(t *testing.T) {
	steady := func(int) float64 { return 4 * mb }
	samples := volumeSeries(100*gb, 70*gb, steady)
	short := samples[len(samples)-int(minForecastWindow/volumeEvery):]
	if got := volumeFindings(t, claim("100G"), short); len(got) != 0 {
		t.Fatalf("a window shorter than %s was extrapolated: %+v", minForecastWindow, got)
	}
}

// A volume whose filesystem is far larger than its claim shares that
// filesystem (a hostPath or local-path volume on the node's disk): its used
// bytes include every other tenant's, so their growth is no forecast about the
// application. A full filesystem still is reported.
func TestStorageForecastIgnoresGrowthOnASharedFilesystem(t *testing.T) {
	steady := func(int) float64 { return 100 * mb }
	if got := volumeFindings(t, claim("1Gi"), volumeSeries(3500*gb, 2800*gb, steady)); len(got) != 0 {
		t.Fatalf("growth on a shared host filesystem became a finding: %+v", got)
	}
	full := volumeFindings(t, claim("1Gi"), volumeSeries(3500*gb, 3400*gb, func(int) float64 { return 0 }))
	if len(full) != 1 || sdk.Predicted(full[0]) {
		t.Fatalf("a filesystem above 90%% must still be reported as observed: %+v", full)
	}
}

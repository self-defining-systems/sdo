package generichealth

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/usage"
)

var memNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func fp(v float64) *float64 { return &v }

const memLimit = 1000.0

// memSample is a memory observation as fractions of a 1000-byte limit. A
// negative rss means the source did not report it.
func memSample(at time.Time, workingSet, rss float64, oom time.Time) usage.Sample {
	s := usage.Sample{At: at, WorkingSetBytes: fp(workingSet * memLimit), MemoryLimit: fp(memLimit), LastOOMKilledAt: oom}
	if rss >= 0 {
		s.RSSBytes = fp(rss * memLimit)
	}
	return s
}

func memFindings(t *testing.T, samples ...usage.Sample) []sdk.Finding {
	t.Helper()
	d := NewSaturation(sdk.DetectorSpec{ID: "resource-saturation"}, false)
	s := sdktest.Snapshot{Usage: &usage.Result{ObservedAt: memNow, Containers: []usage.ContainerWindow{{
		Identity: usage.Identity{PodUID: "u", Pod: "pod-1", Container: "main"}, Samples: samples}}}}
	f, err := d.Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestSteadyHighWorkingSetWithLowRSSIsSilent(t *testing.T) {
	// Page cache fills the limit; anonymous memory is half of it.
	got := memFindings(t, memSample(memNow.Add(-4*time.Minute), 0.94, 0.5, time.Time{}), memSample(memNow, 0.94, 0.5, time.Time{}))
	if len(got) != 0 {
		t.Fatalf("steady cache-heavy working set fired: %+v", got)
	}
}

func TestWorkingSetAloneWithoutRSSOrOOMIsSilent(t *testing.T) {
	got := memFindings(t, memSample(memNow.Add(-4*time.Minute), 0.80, -1, time.Time{}), memSample(memNow.Add(-2*time.Minute), 0.88, -1, time.Time{}), memSample(memNow, 0.95, -1, time.Time{}))
	if len(got) != 0 {
		t.Fatalf("working-set slope without RSS fired: %+v", got)
	}
}

func TestHighRSSFires(t *testing.T) {
	got := memFindings(t, memSample(memNow, 0.97, 0.92, time.Time{}))
	if len(got) != 1 || !strings.Contains(got[0].Evidence, "signal=rss") {
		t.Fatalf("want one rss finding, got %+v", got)
	}
}

func TestRecentOOMKillWithHighWorkingSetFires(t *testing.T) {
	got := memFindings(t, memSample(memNow, 0.94, 0.5, memNow.Add(-5*time.Minute)))
	if len(got) != 1 || !strings.Contains(got[0].Evidence, "signal=oom-killed") {
		t.Fatalf("want one oom finding, got %+v", got)
	}
}

func TestOOMKillOutsideWindowIsSilent(t *testing.T) {
	got := memFindings(t, memSample(memNow, 0.94, 0.5, memNow.Add(-30*time.Minute)))
	if len(got) != 0 {
		t.Fatalf("stale OOM fired: %+v", got)
	}
}

func TestOOMKillBelowThresholdIsSilent(t *testing.T) {
	got := memFindings(t, memSample(memNow, 0.5, 0.4, memNow.Add(-time.Minute)))
	if len(got) != 0 {
		t.Fatalf("low ratio fired: %+v", got)
	}
}

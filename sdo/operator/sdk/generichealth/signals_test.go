package generichealth_test

import (
	"context"
	"regexp"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/generichealth"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/usage"
	"testing"
	"time"
)

func TestSaturation(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, rule := range []string{"memory-saturation", "cpu-throttling", "pvc-bytes-saturation", "pvc-inodes-saturation"} {
		for _, state := range []string{"matching", "healthy", "no-limit", "forecast"} {
			t.Run(rule+"/"+state, func(t *testing.T) {
				s := healthy()
				u := &usage.Result{ObservedAt: now}
				s.Usage = u
				var samples []usage.Sample
				var vols []usage.VolumeSample
				for i := 0; i < 3; i++ {
					value := 95.0
					if state == "healthy" || state == "no-limit" {
						value = 20
					}
					if state == "forecast" {
						value = 60 + float64(i)*10
					}
					at := now.Add(time.Duration(i-2) * time.Minute)
					limit := ptr(100.0)
					if state == "no-limit" {
						limit = nil
					}
					throttled := float64(i) * 40
					if state != "matching" {
						throttled = float64(i) * 10
					}
					samples = append(samples, usage.Sample{At: at, WorkingSetBytes: ptr(value), MemoryLimit: limit, CPULimit: limit, Periods: float64(i) * 100, ThrottledPeriods: throttled, ThrottlingAvailable: true})
					vols = append(vols, usage.VolumeSample{At: at, UsedBytes: ptr(value), CapacityBytes: limit, InodesUsed: ptr(value), Inodes: limit})
				}
				u.Containers = []usage.ContainerWindow{{Identity: usage.Identity{Pod: "instance-x", Container: "worker"}, Samples: samples}}
				u.Volumes = []usage.VolumeWindow{{PVC: "claim-x", Samples: vols}}
				fs, err := generichealth.NewSaturation(sdk.DetectorSpec{}, true).Detect(context.Background(), s)
				if err != nil {
					t.Fatal(err)
				}
				got := false
				for _, f := range fs {
					got = got || f.RuleID == rule
				}
				want := state == "matching" || state == "forecast" && rule != "cpu-throttling"
				if got != want {
					t.Fatalf("want %v: %+v", want, fs)
				}
			})
		}
	}
}
func TestCrashes(t *testing.T) {
	d := generichealth.NewCrashes(sdk.DetectorSpec{})
	q := d.(logs.Consumer).LogQueries()[0]
	positives := []string{"panic: exploded", "fatal error: concurrent map writes", "Traceback (most recent call last):", "java.lang.OutOfMemoryError: heap", "Exception in thread \"main\" java.lang.RuntimeException", "Segmentation fault (core dumped)"}
	for i, p := range q.Patterns {
		if p.ID == "previous-tail" {
			continue
		}
		re := regexp.MustCompile(p.RE2)
		if !re.MatchString(positives[i]) || re.MatchString("request error: temporarily unavailable") {
			t.Fatal(p)
		}
		for _, state := range []string{"matching", "healthy", "already-present", "truncated"} {
			t.Run(p.ID+"/"+state, func(t *testing.T) {
				s := healthy()
				a := logs.Cycle{Matches: map[string]logs.Match{}}
				b := logs.Cycle{Running: true, Matches: map[string]logs.Match{p.ID: {Count: 1, Excerpts: []string{positives[i]}}}, Previous: &logs.Cycle{Matches: map[string]logs.Match{p.ID: {Count: 1, Excerpts: []string{"previous crash"}}}}}
				if state == "healthy" {
					b.Matches = nil
				}
				if state == "already-present" {
					a.Matches = b.Matches
				}
				if state == "truncated" {
					a.Truncated = true
				}
				s.Logs = map[string]logs.Result{q.ID: {Windows: []logs.Window{{Namespace: s.NamespaceName, Pod: "instance-x", Container: "worker", Cycles: []logs.Cycle{a, b}}}}}
				fs, err := d.Detect(context.Background(), s)
				if err != nil || (len(fs) > 0) != (state == "matching") {
					t.Fatalf("%+v %v", fs, err)
				}
			})
		}
	}
}

func TestPreviousTailDoesNotFire(t *testing.T) {
	s := healthy()
	s.Logs = map[string]logs.Result{"generic-crashes": {Windows: []logs.Window{{Namespace: s.NamespaceName, Pod: "instance-x", Container: "worker", Cycles: []logs.Cycle{{Matches: map[string]logs.Match{}}, {Matches: map[string]logs.Match{"previous-tail": {Count: 3, Excerpts: []string{"healthy application message"}}}}}}}}}
	fs, err := generichealth.NewCrashes(sdk.DetectorSpec{}).Detect(context.Background(), s)
	if err != nil || len(fs) != 0 {
		t.Fatalf("%+v %v", fs, err)
	}
}

func TestTerminationCrashDecisions(t *testing.T) {
	for _, exit := range []int32{0, 2} {
		for _, matched := range []bool{false, true} {
			s := healthy()
			tail := logs.Cycle{Matches: map[string]logs.Match{}}
			if matched {
				tail.Matches["go-panic"] = logs.Match{Count: 1}
				tail.Matches["go-fatal"] = logs.Match{Count: 1}
			}
			s.Logs = map[string]logs.Result{"generic-crashes": {Windows: []logs.Window{{Namespace: s.NamespaceName, Pod: "p", UID: "uid", Container: "c", Terminations: []logs.Termination{{RestartCount: 3, ExitCode: exit, Tail: tail}}}}}}
			fs, err := generichealth.NewCrashes(sdk.DetectorSpec{}).Detect(context.Background(), s)
			want := 0
			if exit != 0 && matched {
				want = 1
			}
			if err != nil || len(fs) != want {
				t.Fatalf("exit=%d matched=%v: %+v %v", exit, matched, fs, err)
			}
		}
	}
}

func TestSurvivingCrashAndTerminationShareIdentity(t *testing.T) {
	s := healthy()
	tail := logs.Cycle{Running: true, Matches: map[string]logs.Match{"go-panic": {Count: 1}}}
	w := logs.Window{Namespace: s.NamespaceName, Pod: "p", UID: "uid", Container: "c", RestartCount: 3, Cycles: []logs.Cycle{{}, tail}}
	d := generichealth.NewCrashes(sdk.DetectorSpec{})
	s.Logs = map[string]logs.Result{"generic-crashes": {Windows: []logs.Window{w}}}
	before, err := d.Detect(context.Background(), s)
	if err != nil || len(before) != 1 {
		t.Fatal(before, err)
	}
	w.Cycles = nil
	w.Terminations = []logs.Termination{{RestartCount: 3, ExitCode: 2, Tail: tail}}
	s.Logs = map[string]logs.Result{"generic-crashes": {Windows: []logs.Window{w}}}
	after, err := d.Detect(context.Background(), s)
	if err != nil || len(after) != 1 || before[0].Fingerprint != after[0].Fingerprint {
		t.Fatalf("crash re-keyed on exit: %+v %+v %v", before, after, err)
	}
}

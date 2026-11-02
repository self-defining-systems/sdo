package workloadhealth_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/workloadhealth"
)

func TestPodAdmissionVerdictsAndReplay(t *testing.T) {
	detector := workloadhealth.NewPodAdmissionDetector(sdk.DetectorSpec{ID: "pod-admission", Playbooks: []string{"health-objective"}})
	for _, tc := range []struct {
		name             string
		result           *sdk.PodAdmissionResult
		finding, failure bool
	}{
		{name: "unobserved", failure: true},
		{name: "admitted", result: &sdk.PodAdmissionResult{Namespace: "app", Admitted: true}},
		{name: "rejected", result: &sdk.PodAdmissionResult{Namespace: "app", Rejection: "admission blocked"}, finding: true},
		{name: "transport", result: &sdk.PodAdmissionResult{Namespace: "app", Error: "connection refused"}, failure: true},
		{name: "wrong namespace", result: &sdk.PodAdmissionResult{Namespace: "foreign", Admitted: true}, failure: true},
		{name: "empty", result: &sdk.PodAdmissionResult{Namespace: "app"}, failure: true},
		{name: "inconsistent", result: &sdk.PodAdmissionResult{Namespace: "app", Admitted: true, Rejection: "rejected"}, failure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "app", Admission: tc.result}
			captured, err := sdktest.CaptureSnapshot(snapshot, "app")
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(captured)
			if err != nil {
				t.Fatal(err)
			}
			var replay sdktest.Snapshot
			if err := json.Unmarshal(data, &replay); err != nil {
				t.Fatal(err)
			}
			findings, err := sdk.DetectObserved(context.Background(), detector, replay)
			if (err != nil) != tc.failure || (len(findings) > 0) != tc.finding {
				t.Fatalf("findings=%+v err=%v", findings, err)
			}
			if tc.finding && (findings[0].RuleID != workloadhealth.RulePodAdmissionRejected || !strings.Contains(findings[0].Evidence, tc.result.Rejection)) {
				t.Fatal(findings)
			}
		})
	}
}

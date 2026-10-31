package generichealth_test

import (
	"context"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/generichealth"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestNodeHealth(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status core.ConditionStatus
		used   bool
		want   int
	}{
		{"matching", core.ConditionFalse, true, 1}, {"healthy", core.ConditionTrue, true, 0}, {"unrelated", core.ConditionFalse, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sdktest.Snapshot{NamespaceName: "tenant-z", NodeList: []core.Node{{ObjectMeta: meta.ObjectMeta{Name: "machine-q"}, Status: core.NodeStatus{Conditions: []core.NodeCondition{{Type: core.NodeReady, Status: tc.status}}}}}}
			if tc.used {
				s.PodList = []core.Pod{{ObjectMeta: meta.ObjectMeta{Name: "unit-x", Namespace: s.NamespaceName}, Spec: core.PodSpec{NodeName: "machine-q"}}}
			}
			got, err := generichealth.NewNodes(sdk.DetectorSpec{ID: "nodes"}).Detect(context.Background(), s)
			if err != nil || len(got) != tc.want {
				t.Fatalf("got %v, %v", got, err)
			}
		})
	}
}

func TestWarnings(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		count  int32
		age    time.Duration
		reason string
		want   int
	}{
		{"matching", 5, 0, "FailedMount", 1}, {"single", 1, 0, "FailedMount", 0}, {"old", 5, time.Hour, "FailedMount", 0}, {"normal", 5, 0, "Pulled", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sdktest.Snapshot{NamespaceName: "tenant-z", ObservedAtTime: &now, PodList: []core.Pod{{ObjectMeta: meta.ObjectMeta{Name: "unit-x", Namespace: "tenant-z"}}}, EventList: []core.Event{{ObjectMeta: meta.ObjectMeta{Name: "evt-x", Namespace: "tenant-z"}, Type: "Warning", Reason: tc.reason, Count: tc.count, FirstTimestamp: meta.NewTime(now.Add(-5 * time.Minute)), LastTimestamp: meta.NewTime(now.Add(-tc.age)), InvolvedObject: core.ObjectReference{Kind: "Pod", Namespace: "tenant-z", Name: "unit-x"}}}}
			got, err := generichealth.NewWarnings(sdk.DetectorSpec{}).Detect(context.Background(), s)
			if err != nil || len(got) != tc.want {
				t.Fatalf("got %v %v", got, err)
			}
		})
	}
}

func TestWiringContracts(t *testing.T) {
	for _, tc := range wiringCases() {
		t.Run(tc.name, func(t *testing.T) {
			for _, state := range []string{"matching", "healthy", "near-miss"} {
				t.Run(state, func(t *testing.T) {
					s := healthy()
					if state != "healthy" {
						tc.change(&s)
					}
					if state == "near-miss" {
						tc.excuse(&s)
					}
					fs, err := generichealth.NewWiring(sdk.DetectorSpec{ID: "wiring"}).Detect(context.Background(), s)
					if err != nil {
						t.Fatal(err)
					}
					count := 0
					for _, f := range fs {
						if f.RuleID == tc.rule {
							count++
							if f.Fingerprint == "" || len(f.ParameterBindings) == 0 || f.Severity != sdk.SeverityWarning {
								t.Fatalf("invalid finding: %+v", f)
							}
						}
					}
					want := state == "matching"
					if (count > 0) != want {
						t.Fatalf("want match=%v: %+v", want, fs)
					}
				})
			}
		})
	}
}

func TestNodePressure(t *testing.T) {
	for _, kind := range []core.NodeConditionType{core.NodeMemoryPressure, core.NodeDiskPressure, core.NodePIDPressure} {
		for _, status := range []core.ConditionStatus{core.ConditionTrue, core.ConditionFalse, core.ConditionUnknown} {
			t.Run(string(kind)+"/"+string(status), func(t *testing.T) {
				s := healthy()
				s.PodList[0].Spec.NodeName = "machine-x"
				s.NodeList = []core.Node{{ObjectMeta: meta.ObjectMeta{Name: "machine-x"}, Status: core.NodeStatus{Conditions: []core.NodeCondition{{Type: kind, Status: status}}}}}
				fs, err := generichealth.NewNodes(sdk.DetectorSpec{}).Detect(context.Background(), s)
				if err != nil || (len(fs) > 0) != (status == core.ConditionTrue) {
					t.Fatalf("%+v %v", fs, err)
				}
			})
		}
	}
}
func TestSecretKeys(t *testing.T) {
	for _, state := range []string{"missing", "healthy", "optional"} {
		t.Run(state, func(t *testing.T) {
			s := healthy()
			s.SecretList = []sdk.SecretMetadata{{Name: "credentials-x", Namespace: s.NamespaceName, DataKeys: []string{"present"}}}
			s.DeploymentList[0].Spec.Template.Spec.Containers[0].Env = []core.EnvVar{{Name: "X", ValueFrom: &core.EnvVarSource{SecretKeyRef: &core.SecretKeySelector{LocalObjectReference: core.LocalObjectReference{Name: "credentials-x"}, Key: "absent", Optional: ptr(state == "optional")}}}}
			if state == "healthy" {
				s.SecretList[0].DataKeys = append(s.SecretList[0].DataKeys, "absent")
			}
			fs, err := generichealth.NewWiring(sdk.DetectorSpec{}).Detect(context.Background(), s)
			if err != nil || (len(fs) > 0) != (state == "missing") {
				t.Fatalf("%+v %v", fs, err)
			}
		})
	}
}
func TestWarningRecovery(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := healthy()
	s.PodList[0].Status.Conditions = []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue, LastTransitionTime: meta.NewTime(now.Add(-time.Second))}}
	s.EventList = []core.Event{{Type: "Warning", Reason: "BackOff", Count: 10, FirstTimestamp: meta.NewTime(now.Add(-time.Minute * 5)), LastTimestamp: meta.NewTime(now.Add(-time.Minute)), InvolvedObject: core.ObjectReference{Kind: "Pod", Name: s.PodList[0].Name, Namespace: s.NamespaceName}}}
	fs, err := generichealth.NewWarnings(sdk.DetectorSpec{}).Detect(context.Background(), s)
	if err != nil || len(fs) > 0 {
		t.Fatalf("%+v %v", fs, err)
	}
}

func TestWarningReasons(t *testing.T) {
	for _, reason := range []string{"FailedMount", "FailedAttachVolume", "FailedScheduling", "BackOff", "Unhealthy", "Evicted", "OOMKilling", "FailedCreate", "FailedCreatePodSandBox"} {
		for _, persistent := range []bool{false, true} {
			t.Run(reason, func(t *testing.T) {
				s := healthy()
				now := *s.ObservedAtTime
				count := int32(1)
				if persistent {
					count = 5
				}
				s.EventList = []core.Event{{Type: "Warning", Reason: reason, Count: count, FirstTimestamp: meta.NewTime(now.Add(-5 * time.Minute)), LastTimestamp: meta.NewTime(now), InvolvedObject: core.ObjectReference{Kind: "Pod", Name: s.PodList[0].Name, Namespace: s.NamespaceName}}}
				fs, err := generichealth.NewWarnings(sdk.DetectorSpec{}).Detect(context.Background(), s)
				if err != nil || (len(fs) > 0) != persistent {
					t.Fatalf("%+v %v", fs, err)
				}
			})
		}
	}
}

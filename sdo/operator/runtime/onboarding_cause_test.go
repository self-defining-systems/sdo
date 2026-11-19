package runtime

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// waitCauseOf runs one onboarding health check with an unobserved finding and returns the recorded wait cause.
func waitCauseOf(t *testing.T, finding sdk.Finding) string {
	t.Helper()
	d := &warmingDetector{sequenceDetector: controllerDetector("request-metrics", time.Second), warm: true}
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.always = finding
	c := &Controller{onboardingReachability: &onboardingReachabilityState{}, detectors: map[string]sdk.Detector{d.spec.ID: d}}
	c.evaluationIteration++
	c.checkOnboardingHealth(context.Background(), sdktest.Snapshot{})
	return c.onboardingReachability.waitCause
}

// A rejection's cause is its class and the detector, rule and object kind that held it: equal for the same
// failure whatever its timings, counts or generated object names, and different for a different check.
func TestOnboardingCauseIgnoresTimingsAndGeneratedNames(t *testing.T) {
	first := sdk.Finding{DetectorID: "request-metrics", RuleID: "request-metrics.not-instrumented", Status: sdk.FindingActive, Severity: sdk.SeverityInfo,
		Summary: "http_requests_total has no series after 283.786s", PrimaryResource: sdk.ObjectRef{Kind: "Pod", Name: "compose-post-6d5985bd55-bwmvv"}}
	again := first
	again.Summary = "http_requests_total has no series after 274.988s"
	again.PrimaryResource.Name = "compose-post-7f9c8b6d44-x2k9p"
	other := first
	other.RuleID = "request-metrics.error-rate"
	cause := waitCauseOf(t, first)
	if cause != "incomplete_observation:request-metrics/request-metrics.not-instrumented/Pod" {
		t.Fatalf("cause = %q", cause)
	}
	if waitCauseOf(t, again) != cause {
		t.Fatal("the same failure with other timings and pod names has another cause")
	}
	if waitCauseOf(t, other) == cause {
		t.Fatal("a different rule shares the cause")
	}
	named := first
	named.PrimaryResource = sdk.ObjectRef{Kind: "Role", Name: "grafana"}
	if waitCauseOf(t, named) != "incomplete_observation:request-metrics/request-metrics.not-instrumented/Role/grafana" {
		t.Fatalf("an authored object's name is part of its cause: %q", waitCauseOf(t, named))
	}
}

// The durable rejection record carries the cause beside the human reason, so a harness compares causes, never text.
func TestOnboardingRejectionRecordsItsCause(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	c := &Controller{onboardingClient: client, onboardingControl: "control",
		onboardingReachability: &onboardingReachabilityState{waitReason: "incomplete observation: has no series after 3.2s", waitCause: "incomplete_observation:request-metrics/r/Pod"}}
	reason, cause := c.onboardingRejection(true)
	if cause != "window_expired/incomplete_observation:request-metrics/r/Pod" {
		t.Fatalf("cause = %q", cause)
	}
	if err := c.rejectPendingOnboarding(ctx, reason, cause); err != nil {
		t.Fatal(err)
	}
	record, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.RejectedConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if record.Data["cause"] != cause || record.Data["reason"] != reason {
		t.Fatalf("record = %v", record.Data)
	}
	c.onboardingReachability.invalid, c.onboardingReachability.invalidReason, c.onboardingReachability.invalidCause = true, "accepted install lineage changed", "lineage_changed"
	if _, cause := c.onboardingRejection(false); cause != "window_contradicted/lineage_changed" {
		t.Fatalf("contradiction cause = %q", cause)
	}
}

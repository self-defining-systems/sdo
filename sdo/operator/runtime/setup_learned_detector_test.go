package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// install steps a fresh install once: a controller whose install window holds
// the given detectors, and reports the record it accepted (nil when rejected).
type installRun struct {
	c      *Controller
	record *contracts.NamespaceOnboarding
	start  time.Time
}

func freshInstall(t *testing.T, detectors ...sdk.Detector) installRun {
	t.Helper()
	start := time.Unix(1000, 0).UTC()
	cfg := testControllerConfig()
	cfg.SourceCommit = strings.Repeat("a", 40)
	c, err := NewController(cfg, detectors, staticProvider{}, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, start)
	if err != nil {
		t.Fatal(err)
	}
	c.onboardingClient, c.onboardingControl = fake.NewSimpleClientset(), "control"
	c.onboardingReachability = &onboardingReachabilityState{}
	c.pendingOnboarding = &contracts.NamespaceOnboarding{Namespace: cfg.Namespace, AcceptedCommit: cfg.SourceCommit, EstablishedAt: timestamppb.New(start)}
	if err := c.Step(context.Background(), start, nil); err != nil {
		t.Fatal(err)
	}
	return installRun{c, c.namespaceOnboarding, start}
}

func quietHealth() *sequenceDetector {
	d := controllerDetector("health", time.Second, sdk.Finding{})
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	d.spec.OriginatingCommit = "seed"
	return d
}

// clue is a learned detector, learned from an incident or not, erroring
// with err or, when err is nil, firing.
func clue(id string, learned bool, err error) *sequenceDetector {
	sample := sdk.Finding{}
	if err == nil {
		sample = sdk.Finding{RuleID: "noisy", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "noisy", Evidence: "noisy", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"}}
	}
	d := controllerDetector(id, time.Second, sample)
	d.spec.Class = sdk.DetectorClassLearned
	d.spec.Owner = sdk.DetectorOwnerResponder
	d.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	d.spec.OriginatingCommit = "seed"
	if learned {
		d.spec.OriginatingIncident = "synthetic-incident"
	}
	if err != nil {
		d.errors[0] = err
	}
	return d
}

// Only health-class detectors (and user tests) decide whether an install is
// accepted. A learned detector, whoever authored it, that errors or
// fires on the healthy app never rejects the install, is never disabled, and
// leaves noisy evidence on the install record.
func TestLearnedDetectorOnAHealthyInstallNeverGatesOrDisables(t *testing.T) {
	for name, c := range map[string]struct {
		learned bool
		err     error
		outcome string
	}{
		"learned detector errors": {true, fmt.Errorf("acceptance record unavailable"), "errored"},
		"learned detector fires":  {true, nil, "fired"},
	} {
		t.Run(name, func(t *testing.T) {
			noisy := clue("clue", c.learned, c.err)
			got := freshInstall(t, quietHealth(), noisy)
			if got.record == nil {
				t.Fatal("a learned detector rejected a healthy install")
			}
			if err := got.c.Step(context.Background(), got.start.Add(5*time.Second), nil); err != nil {
				t.Fatal(err)
			}
			if noisy.calls < 2 {
				t.Fatalf("the detector was disabled after setup: %d evaluations", noisy.calls)
			}
			if len(got.record.SetupNoisy) != 1 || got.record.SetupNoisy[0].DetectorId != "clue" || got.record.SetupNoisy[0].Outcome != c.outcome {
				t.Fatalf("setup_noisy = %+v, want one %q entry for the detector", got.record.SetupNoisy, c.outcome)
			}
		})
	}
}

// A health detector's forecast (a volume that will fill within hours, from a
// trend) is not a condition that holds now: it neither contradicts the install
// window nor fails the install evaluation, which is accepted.
func TestPredictedHealthFindingDoesNotDecideTheInstall(t *testing.T) {
	forecast := controllerDetector("health", time.Second, sdk.Finding{RuleID: "pvc-bytes-saturation", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "fills in 2h", Evidence: "fills in 2h", PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "demo", Name: "data"}, Metadata: map[string]any{sdk.MetadataPredicted: true}})
	forecast.spec = quietHealth().spec
	got := freshInstall(t, forecast)
	if got.record == nil {
		t.Fatal("a predicted finding of a health detector rejected the install")
	}
	if got.c.onboardingReachability.invalid {
		t.Fatalf("a predicted finding contradicted the install window: %s", got.c.onboardingReachability.invalidReason)
	}
	// Detectors run behind the memory host: the mark must survive the wire.
	wire, err := findingContract(forecast.samples[0][0])
	if err != nil {
		t.Fatal(err)
	}
	if !sdk.Predicted(findingState(wire)) {
		t.Fatal("the predicted mark was lost on the memory-host wire")
	}
}

// Protection kept: a health-class detector still decides admission, erroring or firing.
func TestHealthDetectorStillDecidesTheInstall(t *testing.T) {
	erroring := quietHealth()
	erroring.errors[0] = fmt.Errorf("observation unavailable")
	firing := controllerDetector("health", time.Second, sdk.Finding{RuleID: "unhealthy", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "down", Evidence: "down", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"}})
	firing.spec = quietHealth().spec
	for name, health := range map[string]sdk.Detector{"errors": erroring, "fires": firing} {
		t.Run(name, func(t *testing.T) {
			if got := freshInstall(t, health, clue("clue", true, nil)); got.record != nil {
				t.Fatalf("a failing health detector admitted the install: %+v", got.record)
			}
		})
	}
}

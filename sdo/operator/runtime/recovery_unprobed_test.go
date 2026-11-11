package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

func selectingService(name string) corev1.Service {
	return corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": name}}}
}

func labeledPod(name, app string) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Labels: map[string]string{"app": app}}}
}

// probeWindow is a health-probe workload whose qualified scenario enters at
// target and crosses dependsOn.
func probeWindow(qualified bool, target string, dependsOn ...string) traffic.Window {
	return traffic.Window{
		Workload: traffic.Workload{Name: "probe-call", Purpose: traffic.PurposeHealthProbe},
		Scenarios: []traffic.ScenarioObservations{{
			Scenario:  traffic.Descriptor{ID: "call", Target: traffic.Target{Service: target, Port: 80}, DependsOn: dependsOn},
			Qualified: qualified,
		}},
	}
}

func unprobedView(traffic map[string]traffic.Window) sdktest.Snapshot {
	store := appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "store", Namespace: "app"}}
	store.Spec.Template.Labels = map[string]string{"app": "store"}
	return sdktest.Snapshot{
		NamespaceName:   "app",
		ServiceList:     []corev1.Service{selectingService("web"), selectingService("store")},
		PodList:         []corev1.Pod{labeledPod("web-1", "web"), labeledPod("store-0", "store")},
		StatefulSetList: []appsv1.StatefulSet{store},
		Traffic:         traffic,
	}
}

func findingOn(kind, name string) sdk.Finding {
	return sdk.Finding{DetectorID: "service-endpoints", RuleID: "no-ready-endpoints", Status: sdk.FindingActive,
		PrimaryResource: sdk.ObjectRef{Kind: kind, Namespace: "app", Name: name}}
}

// A faulted component is reached through its Services. A Service the
// incident's findings name, directly or through its Pods or workload, that no
// qualified probe scenario crosses has only readiness to show it recovered.
func TestRecoveryUnprobedNamesServicesNoProbeCrosses(t *testing.T) {
	webOnly := map[string]traffic.Window{"probe-call": probeWindow(true, "web")}
	for name, test := range map[string]struct {
		view     sdktest.Snapshot
		findings []sdk.Finding
		want     []string
	}{
		"service no probe crosses":   {unprobedView(webOnly), []sdk.Finding{findingOn("Service", "store")}, []string{"store"}},
		"pod of that service":        {unprobedView(webOnly), []sdk.Finding{findingOn("Pod", "store-0")}, []string{"store"}},
		"workload of that service":   {unprobedView(webOnly), []sdk.Finding{findingOn("StatefulSet", "store")}, []string{"store"}},
		"probe crosses it":           {unprobedView(map[string]traffic.Window{"probe-call": probeWindow(true, "web", "store")}), []sdk.Finding{findingOn("Service", "store")}, nil},
		"probe enters at it":         {unprobedView(webOnly), []sdk.Finding{findingOn("Pod", "web-1")}, nil},
		"unqualified probe is blind": {unprobedView(map[string]traffic.Window{"probe-call": probeWindow(false, "web", "store")}), []sdk.Finding{findingOn("Service", "store")}, []string{"store"}},
		"no probes at all":           {unprobedView(nil), []sdk.Finding{findingOn("Service", "store")}, nil},
		"link probes only": {unprobedView(map[string]traffic.Window{"links": {Workload: traffic.Workload{Name: "links", Purpose: traffic.PurposeLinkProbe}}}),
			[]sdk.Finding{findingOn("Service", "store")}, nil},
		"drift is not a fault of users": {unprobedView(webOnly), []sdk.Finding{{DetectorID: "drift.source", PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "store"}}}, nil},
		"no service behind the object":  {unprobedView(webOnly), []sdk.Finding{findingOn("ConfigMap", "settings")}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			got := recoveryUnprobed(test.view, test.findings, "app")
			if len(got) != len(test.want) {
				t.Fatalf("unprobed = %v, want services %v", got, test.want)
			}
			for i, service := range test.want {
				if !strings.Contains(got[i], "Service "+service) {
					t.Fatalf("unprobed = %v, want services %v", got, test.want)
				}
			}
		})
	}
}

// A component restarts and is Ready again, every detector is clear, and no
// probe crosses it: SDO cannot see whether users recovered. The closure commits
// unconfirmed and says why, and reflection may not learn a repair from it.
func TestClosureOfAnUnprobedComponentIsNotVerifiedRecovery(t *testing.T) {
	ctx := context.Background()
	for name, test := range map[string]struct {
		traffic map[string]traffic.Window
		want    contracts.ClosureUnconfirmedReason
	}{
		"no probe crosses the faulted service": {map[string]traffic.Window{"probe-call": probeWindow(true, "web")}, contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE},
		"a probe crosses it and passes":        {map[string]traffic.Window{"probe-call": probeWindow(true, "web", "store")}, contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED},
	} {
		t.Run(name, func(t *testing.T) {
			after := dataRiskOpened.Add(time.Minute)
			cfg := testControllerConfig()
			cfg.Namespace = "app"
			snapshots := riskSnapshots{view: unprobedView(test.traffic)}
			world := &sameWriteWorld{}
			world.healthy.Store(true)
			controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, snapshots, &recordingDispatcher{}, after)
			if err != nil {
				t.Fatal(err)
			}
			controller.now = func() time.Time { return after.Add(time.Minute) }
			controller.gitRead = fakeArchitecture("")
			broker := &riskBroker{}
			controller.broker = broker
			result := completedResult("synthetic-incident")
			result.Timing = TimingMetrics{StartedAt: dataRiskOpened, CompletedAt: after}
			request := IncidentRequest{SchemaVersion: ProtocolSchemaVersion, Application: "demo", Namespace: "app", IncidentID: "synthetic-incident", SourceCommit: "synthetic-accepted", RepositoryWorktree: "synthetic-worktree",
				Findings: []sdk.Finding{findingOn("Service", "store")}}
			controller.pendingClosure = &IncidentClosure{Request: request, Result: &result, DetectedAt: dataRiskOpened, DispatchedAt: dataRiskOpened, ResponderCompletedAt: after, VerifiedAt: after,
				FinalDetectorStates: []DetectorEvaluation{{DetectorID: "traffic", EvaluatedAt: after, Status: DetectorEvaluationClear}}}
			controller.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
			effect, ok := controller.PendingClosureEffect()
			if !ok || !effect.Freeze {
				t.Fatalf("want a freeze effect, got %+v ok=%v", effect, ok)
			}
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			pending := controller.pendingClosure
			if pending == nil {
				t.Fatal("closure lost")
			}
			if pending.UnconfirmedReason != test.want {
				t.Fatalf("reason = %s, want %s (detail %q)", pending.UnconfirmedReason, test.want, pending.UnconfirmedDetail)
			}
			if test.want == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
				return
			}
			if !strings.Contains(pending.UnconfirmedDetail, "Service store") || len(pending.FinalDetectorStates) != 0 {
				t.Fatalf("the closure must name the service and make no health claim: %+v", pending)
			}
			derived, err := ProtoOutcomeDeriver{}.Derive(ctx, *pending, "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if derived.Record.GetClassification() != "unconfirmed" || derived.RequiresReflection || !unverifiedRecoveryOutcome(derived.Record) {
				t.Fatalf("the outcome must be unconfirmed and carry the reason: %+v", derived.Record)
			}
		})
	}
}

func TestUnprobedRecoveryGivesReflectionNoVerifiedRepair(t *testing.T) {
	at := timestamppb.New(dataRiskOpened)
	ref := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "store"}
	outcome := &contracts.OutcomeRecord{
		Classification:      "unconfirmed",
		DetectorHistory:     []*contracts.DetectorEvaluation{{DetectorId: recoveryUnprobedEvaluation, EvaluatedAt: at, Status: string(DetectorEvaluationPending), Reason: proto.String("Service store: no probe crosses it")}},
		ConfirmedRootCauses: []*contracts.ConfirmedRootCause{{Summary: "store restarted", Resources: []*contracts.ObjectRef{ref}}},
		RepairActions:       []*contracts.RepairActionReceipt{{Success: true, Resources: []*contracts.ObjectRef{ref}, StartedAt: at, CompletedAt: at}},
	}
	if got := partialRepairCauses(outcome); len(got) != 0 {
		t.Fatalf("a repair no probe verified is no verified cause: %v", got)
	}
	if reflectionRepairVerified(outcome) {
		t.Fatal("reflection must not publish learning from a recovery no probe verified")
	}
}

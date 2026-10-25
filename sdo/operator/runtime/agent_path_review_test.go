package runtime

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type reviewScopeDetector struct{ finding sdk.Finding }

func (d reviewScopeDetector) Spec() sdk.DetectorSpec { return fastloopDetector{}.Spec() }
func (d reviewScopeDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return []sdk.Finding{d.finding}, nil
}

func TestMitigationScopeFollowsTargetFindingRatherThanWrittenObject(t *testing.T) {
	for _, scenario := range []string{"configmap-deployment", "peer-on-same-service"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
			original := sdk.Finding{DetectorID: fastloopDetector{}.Spec().ID, RuleID: "target", Severity: sdk.SeverityCritical, Summary: "target unhealthy", Evidence: "synthetic observation", Playbooks: []string{"base"}, Fingerprint: "target", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}}
			current := original
			if scenario == "peer-on-same-service" {
				original.PrimaryResource = ref
				current = original
				current.Fingerprint = "peer"
				current.RuleID = "peer"
			}
			client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "app", UID: "synthetic-settings", ResourceVersion: "1"}, Data: map[string]string{"value": "bad"}})
			provider := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}
			detector := reviewScopeDetector{current}
			executor := PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: KubernetesPlanRollouts{Client: client}, Verifier: PlanHealthVerifier{Provider: provider, Detectors: []sdk.Detector{detector}}}
			request := IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{original}}
			result, err := executor.Apply(ctx, request, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[value]", Value: testPlanValue("fixed")}}}, false)
			want := scenario == "peer-on-same-service"
			if err != nil || !result.Applied || result.VerificationPassed != want {
				t.Fatalf("scope should follow target fingerprint: result=%+v err=%v want=%v", result, err, want)
			}
		})
	}
}

func TestResponderVerifyConcurrentControllerObservations(t *testing.T) {
	now := time.Now().UTC()
	c, err := NewController(testControllerConfig(), []sdk.Detector{fastloopDetector{}}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &verificationRetryDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "concurrent", Namespace: "app"}
	c.currentIncidentRequest = &request
	c.incidentOpen = true
	loop := NewResponderLoop(request, nil, c.provider, nil, PlanExecutor{}, 3)
	loop.VerifyClosure = func(ctx context.Context) ToolResponse { return c.responderVerification(ctx, request.IncidentID) }
	server := SessionToolServer{Directory: filepath.Join(t.TempDir(), "tools"), Handle: loop.Handle}
	if err = server.Prepare(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := c.StepEvents(context.Background(), now.Add(time.Duration(i+1)*time.Hour), []sdk.WatchKind{sdk.WatchKind{APIVersion: "v1", Kind: "Service"}}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		exchangeSelfCheck(t, &server, `{"command":"verify"}`)
	}
	wg.Wait()
}

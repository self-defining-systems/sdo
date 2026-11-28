package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

type closureScopeDetector struct {
	id          string
	findings    []sdk.Finding
	unavailable bool
}

func (d *closureScopeDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: d.id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: "synthetic"}
}

func (d *closureScopeDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.unavailable {
		return nil, fmt.Errorf("still running a previous evaluation: %w", sdk.ErrUnavailable)
	}
	return d.findings, nil
}

func TestControllerRecoveryIgnoresHealthOutsideTheAlertSet(t *testing.T) {
	for _, scenario := range []string{"unavailable detector on a page", "firing sibling", "late Ready transition", "unready unrelated replica"} {
		t.Run(scenario, func(t *testing.T) {
			now := verifyT0
			owner := &closureScopeDetector{id: "service-health"}
			alert := verifyFinding(owner.id, "Service", "front", sdk.SeverityCritical)
			request := IncidentRequest{IncidentID: "scope", Namespace: "app", Findings: []sdk.Finding{alert}, OpeningKeys: findingKeys([]sdk.Finding{alert})}
			view := verifyWorld{now: now}.view()
			result := completedResult(request.IncidentID)
			switch scenario {
			case "unavailable detector on a page":
				owner.unavailable = true
				request.Page, request.OpeningKeys = "inspect external symptoms", nil
			case "firing sibling":
				sibling := verifyFinding(owner.id, "Deployment", "sidecar", sdk.SeverityWarning)
				sibling.PrimaryResource.APIVersion = "apps/v1"
				sibling.Summary, sibling.Evidence = "sidecar stalled", "synthetic rollout"
				owner.findings = []sdk.Finding{sibling}
				view.DeploymentList = []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "sidecar", Generation: 2}}}
			default:
				result.PlanApplications = restartOf("Deployment", "api", now).PlanApplications
				if scenario == "unready unrelated replica" {
					view.PodList[1].Status.Conditions[0].Status = corev1.ConditionFalse
					replicas := int32(2)
					view.DeploymentList = []appsv1.Deployment{{
						ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app", Generation: 2},
						Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
						Status:     appsv1.DeploymentStatus{ObservedGeneration: 2, UpdatedReplicas: 1, ReadyReplicas: 1},
					}}
				}
			}

			cfg := testControllerConfig()
			cfg.Namespace, cfg.ClosureStabilityWindow = "app", 5*time.Second
			provider := &riskSnapshots{view: view}
			c, err := NewController(cfg, []sdk.Detector{owner}, provider, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return now }
			c.beginIncident(now, request, request.Findings)
			c.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, result: result}, now)
			var closed IncidentClosure
			c.OnIncidentClosed = func(closure IncidentClosure) { closed = closure }
			step := func() {
				t.Helper()
				if err := c.Step(t.Context(), now, nil); err != nil {
					t.Fatal(err)
				}
				c.Wait()
			}
			step()
			if closed.Result != nil {
				t.Fatal("closure skipped the stability window")
			}
			now = now.Add(cfg.ClosureStabilityWindow)
			if scenario == "late Ready transition" {
				provider.view.PodList[1].Status.Conditions[0].LastTransitionTime = metav1.NewTime(now)
			}
			step()
			if !closed.VerifiedAt.Equal(now) {
				t.Fatalf("verified at %s, want verified at %s", closed.VerifiedAt, now)
			}
		})
	}
}

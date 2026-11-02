package runtime

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"time"
)

type stage3DependentHealth struct {
	now   func() time.Time
	ready time.Time
}

func (*stage3DependentHealth) TrafficWorkloads() []string { return []string{"synthetic-journey"} }

func (*stage3DependentHealth) Spec() sdk.DetectorSpec {
	s := incidentSpec(podWatch)
	s.ID = "synthetic-dependent-health"
	s.Class = sdk.DetectorClassHealth
	return s
}

func (d *stage3DependentHealth) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.now().Before(d.ready) {
		return nil, nil
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: v.Namespace(), Name: "public-route"}
	return []sdk.Finding{
		{DetectorID: d.Spec().ID, RuleID: servicehealth.RuleNoReadyEndpoints, Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: ref, Summary: "Synthetic endpoints settling", Evidence: "synthetic"},
		{DetectorID: d.Spec().ID, RuleID: "traffic", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: v.Namespace(), Name: "journey-entry"}, RelatedResources: []sdk.ObjectRef{ref}, Summary: "Synthetic journey settling", Evidence: "synthetic"},
	}, nil
}

type stage3AuthoredObjective struct {
	sdk.Detector
	ref sdk.ObjectRef
}

func (d stage3AuthoredObjective) Detect(ctx context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.Detector.Detect(ctx, v)
	var scoped []sdk.Finding
	for _, f := range findings {
		if f.PrimaryResource == d.ref {
			f.RuleID = "synthetic-arbitrary-authored-rule"
			scoped = append(scoped, f)
		}
	}
	return scoped, err
}

type stage3EndpointProvider struct {
	SnapshotProvider
	client kubernetes.Interface
}

func (p stage3EndpointProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	view, err := p.SnapshotProvider.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	endpoints, err := p.client.CoreV1().Endpoints("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	snapshot := view.(sdktest.Snapshot)
	snapshot.EndpointList = endpoints.Items
	return snapshot, nil
}


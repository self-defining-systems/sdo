package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type recoveryEffectProvider struct {
	snapshot       sdktest.Snapshot
	samples        int
	disappearAfter int
}

func (p *recoveryEffectProvider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	p.samples++
	s := p.snapshot
	after := p.disappearAfter
	if after == 0 {
		after = 2
	}
	if p.samples > after {
		s.PodList = s.PodList[:len(s.PodList)-1]
	}
	return s, nil
}

// Real plan receipts feed the controller's closure consumer. A draining old
// child holds observation only; its disappearance reuses the health window.
// A replay snapshot's uncaptured error cannot prove live absence. These tests
// read identity/absence through the real typed fake-API boundary instead.
type recoveryAPISnapshot struct {
	sdk.DetectionContext
	client TypedPlanClient
}

func (s recoveryAPISnapshot) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return s.client.Get(context.Background(), ref)
}

type recoveryAcceptedSource struct{ PlanSource }

func (s recoveryAcceptedSource) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Pod" || ref.Kind == "ReplicaSet" {
		return nil, ErrAcceptedManifestAbsent
	}
	return s.PlanSource.AcceptedManifest(ctx, revision, ref)
}


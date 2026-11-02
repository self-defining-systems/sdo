package runtime

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// The health objective covers one component; a second owned, declared Service
// drift joins the same incident. The responder repairs only the health-visible
// component. Read lag and durable restart timing vary across seeded schedules.
type simPartialHealth struct{ fastloopDetector }

func (simPartialHealth) Detect(ctx context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	snapshot := sdktest.Snapshot{NamespaceName: v.Namespace()}
	for _, service := range v.Services() {
		if service.Name == simServiceName(0) {
			snapshot.ServiceList = append(snapshot.ServiceList, service)
		}
	}
	return (fastloopDetector{}).Detect(ctx, snapshot)
}


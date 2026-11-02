package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// staleFaultProvider reports the Service fault back after any write: the saved
// step treats a symptom, and the live cause reasserts it.
type staleFaultProvider struct{ fastloopObjects }

func (p staleFaultProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	view, err := p.fastloopObjects.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := view.(sdktest.Snapshot)
	for i := range snapshot.ServiceList {
		snapshot.ServiceList[i].Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	}
	return snapshot, nil
}


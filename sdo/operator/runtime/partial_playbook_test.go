package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"

)

type stillFaultedStageView struct{ SnapshotProvider }

func (v stillFaultedStageView) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	view, err := v.SnapshotProvider.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := view.(sdktest.Snapshot)
	for index := range snapshot.ServiceList {
		if snapshot.ServiceList[index].Name == "api" {
			snapshot.ServiceList[index].Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
		}
	}
	return snapshot, nil
}


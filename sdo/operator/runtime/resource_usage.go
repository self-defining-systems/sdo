package runtime

import (
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/kubeclient"
	"sdo.dev/operator/sdk/program"
)

func (v LiveReadView) usageClient(ref sdk.ObjectRef, kind string) (*kubeclient.ReadClient, error) {
	if err := kubereads.ValidUsageRef(ref, kind, v.Namespace()); err != nil {
		return nil, err
	}
	return program.Kubernetes(v)
}
func (v LiveReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	client, err := v.usageClient(ref, "Pod")
	if err != nil {
		return nil, err
	}
	return kubereads.PodUsage(v.context(), client, ref)
}
func (v LiveReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	client, err := v.usageClient(ref, "Node")
	if err != nil {
		return nil, err
	}
	return kubereads.NodeUsage(v.context(), client, ref)
}

package runtime

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/kubeclient"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

func (v LiveReadView) usageClient(ref sdk.ObjectRef, kind string) (*kubeclient.ReadClient, error) {
	if ref.Kind != kind || (ref.APIVersion != "" && ref.APIVersion != "v1") || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || (kind == "Pod" && (ref.Namespace != v.Namespace() || len(validation.IsDNS1123Label(ref.Namespace)) != 0)) || (kind == "Node" && ref.Namespace != "") {
		return nil, &metrics.UsageUnavailableError{Reason: fmt.Sprintf("usage requires an explicit %s identity in its readable scope", kind)}
	}
	return program.Kubernetes(v)
}
func (v LiveReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	client, err := v.usageClient(ref, "Pod")
	if err != nil {
		return nil, err
	}
	sample, err := client.Metrics().MetricsV1beta1().PodMetricses(ref.Namespace).Get(v.context(), ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, &metrics.UsageUnavailableError{Reason: err.Error()}
	}
	if err := metrics.ValidatePodUsage(ref, sample); err != nil {
		return nil, err
	}
	return sample, nil
}
func (v LiveReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	client, err := v.usageClient(ref, "Node")
	if err != nil {
		return nil, err
	}
	sample, err := client.Metrics().MetricsV1beta1().NodeMetricses().Get(v.context(), ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, &metrics.UsageUnavailableError{Reason: err.Error()}
	}
	if err := metrics.ValidateNodeUsage(ref, sample); err != nil {
		return nil, err
	}
	return sample, nil
}

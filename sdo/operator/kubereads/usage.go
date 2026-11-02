package kubereads

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/kubeclient"
	"sdo.dev/operator/sdk/metrics"
)

// ValidUsageRef checks that ref names one explicit Pod in namespace or one
// Node. Usage reads never list.
func ValidUsageRef(ref sdk.ObjectRef, kind, namespace string) error {
	if ref.Kind != kind || (ref.APIVersion != "" && ref.APIVersion != "v1") || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || (kind == "Pod" && (ref.Namespace != namespace || len(validation.IsDNS1123Label(ref.Namespace)) != 0)) || (kind == "Node" && ref.Namespace != "") {
		return &metrics.UsageUnavailableError{Reason: fmt.Sprintf("usage requires an explicit %s identity in its readable scope", kind)}
	}
	return nil
}

// PodUsage reads one Pod's current metrics through client.
func PodUsage(ctx context.Context, client *kubeclient.ReadClient, ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	sample, err := client.Metrics().MetricsV1beta1().PodMetricses(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, &metrics.UsageUnavailableError{Reason: err.Error()}
	}
	if err := metrics.ValidatePodUsage(ref, sample); err != nil {
		return nil, err
	}
	return sample, nil
}

// NodeUsage reads one Node's current metrics through client.
func NodeUsage(ctx context.Context, client *kubeclient.ReadClient, ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	sample, err := client.Metrics().MetricsV1beta1().NodeMetricses().Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, &metrics.UsageUnavailableError{Reason: err.Error()}
	}
	if err := metrics.ValidateNodeUsage(ref, sample); err != nil {
		return nil, err
	}
	return sample, nil
}

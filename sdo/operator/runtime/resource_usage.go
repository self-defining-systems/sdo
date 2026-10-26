package runtime

import (
	"fmt"

	"encoding/json"
	"k8s.io/apimachinery/pkg/util/validation"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

func (v LiveReadView) readUsage(ref sdk.ObjectRef, kind, path string, target any) error {
	if ref.Kind != kind || (ref.APIVersion != "" && ref.APIVersion != "v1") || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || (kind == "Pod" && (ref.Namespace != v.Namespace() || len(validation.IsDNS1123Label(ref.Namespace)) != 0)) || (kind == "Node" && ref.Namespace != "") {
		return &metrics.UsageUnavailableError{Reason: fmt.Sprintf("usage requires an explicit %s identity in its readable scope", kind)}
	}
	raw, err := v.KubernetesRead(program.KubernetesRequest{Ref: ref, Method: "GET", Path: path})
	if err != nil {
		return &metrics.UsageUnavailableError{Reason: err.Error()}
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return &metrics.UsageUnavailableError{Reason: fmt.Sprintf("decode %s metrics: %v", kind, err)}
	}
	return nil
}
func (v LiveReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	var sample metricsv1beta1.PodMetrics
	if err := v.readUsage(ref, "Pod", "/apis/metrics.k8s.io/v1beta1/namespaces/"+ref.Namespace+"/pods/"+ref.Name, &sample); err != nil {
		return nil, err
	}
	if err := metrics.ValidatePodUsage(ref, &sample); err != nil {
		return nil, err
	}
	return &sample, nil
}
func (v LiveReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	var sample metricsv1beta1.NodeMetrics
	if err := v.readUsage(ref, "Node", "/apis/metrics.k8s.io/v1beta1/nodes/"+ref.Name, &sample); err != nil {
		return nil, err
	}
	if err := metrics.ValidateNodeUsage(ref, &sample); err != nil {
		return nil, err
	}
	return &sample, nil
}

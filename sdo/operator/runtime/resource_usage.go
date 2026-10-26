package runtime

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

type usageDocument struct {
	Metadata   metav1.ObjectMeta   `json:"metadata"`
	Timestamp  time.Time           `json:"timestamp"`
	Window     metav1.Duration     `json:"window"`
	Usage      corev1.ResourceList `json:"usage"`
	Containers []struct {
		Name  string              `json:"name"`
		Usage corev1.ResourceList `json:"usage"`
	} `json:"containers"`
}

func usageUnavailable(err error) error {
	return &metrics.UsageUnavailableError{Reason: err.Error()}
}

func (v LiveReadView) usageDocument(ref sdk.ObjectRef, kind, path string) (usageDocument, error) {
	var doc usageDocument
	if ref.Kind != kind || (ref.APIVersion != "" && ref.APIVersion != "v1") || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || (kind == "Pod" && (ref.Namespace == "" || ref.Namespace != v.Namespace())) || (kind == "Node" && ref.Namespace != "") {
		return doc, usageUnavailable(fmt.Errorf("usage requires an explicit %s identity in its readable scope", kind))
	}
	raw, err := v.KubernetesRead(program.KubernetesRequest{Ref: ref, Method: "GET", Path: path})
	if err != nil {
		return doc, usageUnavailable(err)
	}
	if err = json.Unmarshal(raw, &doc); err != nil {
		return doc, usageUnavailable(fmt.Errorf("decode %s usage: %w", kind, err))
	}
	if doc.Metadata.Name != ref.Name || doc.Metadata.Namespace != ref.Namespace || doc.Timestamp.IsZero() || doc.Window.Duration <= 0 {
		return doc, usageUnavailable(fmt.Errorf("%s metrics lack matching identity, timestamp or positive window", kind))
	}
	return doc, nil
}

func usageSample(ref sdk.ObjectRef, container string, doc usageDocument, values corev1.ResourceList) (metrics.Usage, error) {
	cpu, cpuOK := values[corev1.ResourceCPU]
	memory, memoryOK := values[corev1.ResourceMemory]
	if !cpuOK || !memoryOK || cpu.Sign() < 0 || memory.Sign() < 0 {
		return metrics.Usage{}, usageUnavailable(fmt.Errorf("metrics lack nonnegative CPU and memory quantities"))
	}
	return metrics.Usage{Resource: ref, Container: container, CPU: cpu, Memory: memory, Timestamp: doc.Timestamp, Window: doc.Window.Duration}, nil
}

func (v LiveReadView) PodUsage(ref sdk.ObjectRef) ([]metrics.Usage, error) {
	path := "/apis/metrics.k8s.io/v1beta1/namespaces/" + ref.Namespace + "/pods/" + ref.Name
	doc, err := v.usageDocument(ref, "Pod", path)
	if err != nil {
		return nil, err
	}
	if len(doc.Containers) == 0 {
		return nil, usageUnavailable(fmt.Errorf("Pod metrics contain no containers"))
	}
	samples := make([]metrics.Usage, 0, len(doc.Containers))
	seen := map[string]bool{}
	for _, c := range doc.Containers {
		if c.Name == "" || seen[c.Name] {
			return nil, usageUnavailable(fmt.Errorf("Pod metrics require unique container identities"))
		}
		seen[c.Name] = true
		sample, err := usageSample(ref, c.Name, doc, c.Usage)
		if err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].Container < samples[j].Container })
	return samples, nil
}

func (v LiveReadView) NodeUsage(ref sdk.ObjectRef) (metrics.Usage, error) {
	doc, err := v.usageDocument(ref, "Node", "/apis/metrics.k8s.io/v1beta1/nodes/"+ref.Name)
	if err != nil {
		return metrics.Usage{}, err
	}
	return usageSample(ref, "", doc, doc.Usage)
}

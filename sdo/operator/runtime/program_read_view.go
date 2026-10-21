package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PrimitiveProber performs one-off probes in the isolated prober process.
type PrimitiveProber interface {
	Probe(context.Context, sdk.ObjectRef, int32, *program.HTTPProbe) (program.ProbeResult, error)
}

// LiveReadView adds read-only primitives to one detector snapshot. Its
// dependencies expose reads only; no program receives a Kubernetes writer.
type LiveReadView struct {
	sdk.DetectionContext
	Context        context.Context
	Client         kubernetes.Interface
	Prober         PrimitiveProber
	Source         PlanSource
	SourceCommit   string
	Baseline       *StateChanges
	BaselineSource StateBaseline
}

var errSnapshotObjectAbsent = errors.New("object is absent from snapshot")

func (v LiveReadView) context() context.Context {
	if v.Context != nil {
		return v.Context
	}
	return context.Background()
}

func (v LiveReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	if ref.Kind != "Pod" || ref.Namespace != v.Namespace() || ref.Name == "" || v.Client == nil {
		return "", errors.New("pod logs require a pod in the snapshot namespace and a Kubernetes reader")
	}
	seconds := int64(since.Seconds())
	if seconds < 0 {
		return "", errors.New("pod log duration must not be negative")
	}
	options := &corev1.PodLogOptions{Container: container, LimitBytes: int64ptr(64 * 1024)}
	if seconds > 0 {
		options.SinceSeconds = &seconds
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	data, err := v.Client.CoreV1().Pods(ref.Namespace).GetLogs(ref.Name, options).DoRaw(ctx)
	if err != nil {
		return "", fmt.Errorf("read logs of pod %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	return string(data), nil
}

func int64ptr(value int64) *int64 { return &value }

func (v LiveReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	return v.probe(ref, port, nil)
}

func (v LiveReadView) ProbeHTTP(ref sdk.ObjectRef, request program.HTTPProbe) (program.ProbeResult, error) {
	if ref.Kind != "Service" {
		return program.ProbeResult{}, errors.New("HTTP probe target must be a Service")
	}
	for _, service := range v.Services() {
		if service.Namespace == ref.Namespace && service.Name == ref.Name {
			if len(service.Spec.Ports) == 0 {
				return program.ProbeResult{}, errors.New("Service has no port")
			}
			return v.probe(ref, service.Spec.Ports[0].Port, &request)
		}
	}
	return program.ProbeResult{}, fmt.Errorf("Service %s/%s is absent from snapshot", ref.Namespace, ref.Name)
}

func (v LiveReadView) probe(ref sdk.ObjectRef, port int32, request *program.HTTPProbe) (program.ProbeResult, error) {
	if ref.Kind != "Service" || ref.Namespace != v.Namespace() || ref.Name == "" || port < 1 || port > 65535 || v.Prober == nil {
		return program.ProbeResult{}, errors.New("probe requires a Service in the snapshot namespace, a valid port, and the prober")
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	return v.Prober.Probe(ctx, ref, port, request)
}

func (v LiveReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if v.Source == nil || v.SourceCommit == "" {
		return program.Diff{}, errors.New("accepted source is unavailable")
	}
	observed, err := v.snapshotObject(ref)
	if err != nil && !errors.Is(err, errSnapshotObjectAbsent) {
		return program.Diff{}, err
	}
	ctx, cancel := context.WithTimeout(v.context(), DefaultDiagnosisDeadline)
	defer cancel()
	source, err := v.Source.AcceptedManifest(ctx, v.SourceCommit, ref)
	if errors.Is(err, ErrAcceptedManifestAbsent) {
		if observed == nil {
			return program.Diff{SourceAbsent: true}, nil
		}
		encoded, encodeErr := json.Marshal(comparableFields(observed.Object))
		if encodeErr != nil {
			return program.Diff{}, encodeErr
		}
		return program.Diff{Changed: true, SourceAbsent: true, Text: fmt.Sprintf("accepted: null\nobserved: %s", encoded)}, nil
	}
	if err != nil {
		return program.Diff{}, fmt.Errorf("accepted manifest for %s/%s: %w", ref.Kind, ref.Name, err)
	}
	if source == nil || !samePlanRef(source, ref) {
		return program.Diff{}, fmt.Errorf("accepted manifest does not match %s/%s", ref.Kind, ref.Name)
	}
	if observed == nil {
		if v.Client != nil {
			if _, err := dryRunAcceptedSource(ctx, v.Client, ref, source); err != nil {
				return program.Diff{}, fmt.Errorf("normalize accepted manifest for %s/%s: %w", ref.Kind, ref.Name, err)
			}
		}
		return program.Diff{Changed: true, Text: "accepted object is absent from snapshot"}, nil
	}
	left := comparableFields(source.Object)
	right := projectSourceFields(comparableFields(observed.Object), left)
	if v.Client != nil {
		normalized, normalizeErr := dryRunAcceptedSource(ctx, v.Client, ref, source)
		if normalizeErr != nil {
			return program.Diff{}, fmt.Errorf("normalize accepted manifest for %s/%s: %w", ref.Kind, ref.Name, normalizeErr)
		}
		left = comparableFields(normalized.Object)
		right = comparableFields(observed.Object)
	}
	before, err := json.Marshal(left)
	if err != nil {
		return program.Diff{}, err
	}
	after, err := json.Marshal(right)
	if err != nil {
		return program.Diff{}, err
	}
	if string(before) == string(after) {
		return program.Diff{}, nil
	}
	return program.Diff{Changed: true, Text: fmt.Sprintf("accepted: %s\nobserved: %s", before, after)}, nil
}

func dryRunAcceptedSource(ctx context.Context, client kubernetes.Interface, ref sdk.ObjectRef, source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	return applyAcceptedSource(ctx, client, ref, source, true)
}

func applyAcceptedSource(ctx context.Context, client kubernetes.Interface, ref sdk.ObjectRef, source *unstructured.Unstructured, dryRun bool) (*unstructured.Unstructured, error) {
	patch, err := json.Marshal(source.Object)
	if err != nil {
		return nil, err
	}
	force := true
	options := metav1.PatchOptions{FieldManager: "sdo-deployer", Force: &force}
	if dryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}
	var object any
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Service":
		object, err = client.CoreV1().Services(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	case "v1/ConfigMap":
		object, err = client.CoreV1().ConfigMaps(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	case "v1/Pod":
		object, err = client.CoreV1().Pods(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	case "apps/v1/Deployment":
		object, err = client.AppsV1().Deployments(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	case "apps/v1/ReplicaSet":
		object, err = client.AppsV1().ReplicaSets(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	case "v1/Endpoints":
		object, err = client.CoreV1().Endpoints(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	case "discovery.k8s.io/v1/EndpointSlice":
		object, err = client.DiscoveryV1().EndpointSlices(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	case "networking.k8s.io/v1/NetworkPolicy":
		object, err = client.NetworkingV1().NetworkPolicies(ref.Namespace).Patch(ctx, ref.Name, types.ApplyPatchType, patch, options)
	default:
		return nil, fmt.Errorf("source diff does not support %s/%s", ref.APIVersion, ref.Kind)
	}
	if err != nil {
		return nil, err
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: content}, nil
}

func comparableFields(object map[string]any) map[string]any {
	object = kruntime.DeepCopyJSON(object)
	if metadata, ok := object["metadata"].(map[string]any); ok {
		removeRevisionStamp(metadata)
	}
	if spec, ok := object["spec"].(map[string]any); ok {
		if template, ok := spec["template"].(map[string]any); ok {
			if metadata, ok := template["metadata"].(map[string]any); ok {
				removeRevisionStamp(metadata)
				if len(metadata) == 0 {
					delete(template, "metadata")
				}
			}
		}
	}
	result := map[string]any{}
	for _, key := range []string{"spec", "data", "binaryData"} {
		if value, ok := object[key]; ok {
			result[key] = value
		}
	}
	if metadata, ok := object["metadata"].(map[string]any); ok {
		selected := map[string]any{}
		for _, key := range []string{"labels", "annotations"} {
			if value, ok := metadata[key]; ok {
				selected[key] = value
			}
		}
		if len(selected) > 0 {
			result["metadata"] = selected
		}
	}
	return result
}

func removeRevisionStamp(metadata map[string]any) {
	annotations, ok := metadata["annotations"].(map[string]any)
	if !ok {
		return
	}
	delete(annotations, "sdo.dev/source-tree")
	if len(annotations) == 0 {
		delete(metadata, "annotations")
	}
}

func projectSourceFields(observed, declared any) any {
	switch source := declared.(type) {
	case map[string]any:
		actual, _ := observed.(map[string]any)
		result := map[string]any{}
		for key, value := range source {
			result[key] = projectSourceFields(actual[key], value)
		}
		return result
	case []any:
		actual, _ := observed.([]any)
		result := make([]any, 0, len(actual))
		for index, value := range actual {
			if index < len(source) {
				result = append(result, projectSourceFields(value, source[index]))
			} else {
				result = append(result, value)
			}
		}
		return result
	default:
		return observed
	}
}

func (v LiveReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	baseline := v.Baseline
	if v.BaselineSource != nil {
		baseline = v.BaselineSource.Changes(time.Now().UTC())
	}
	if baseline == nil {
		return program.Diff{}, errors.New("healthy baseline is unavailable")
	}
	if ref.Namespace != v.Namespace() {
		return program.Diff{}, errors.New("object is outside snapshot namespace")
	}
	for _, change := range baseline.Changes {
		if change.Kind == ref.Kind && change.Name == ref.Name {
			encoded, err := json.Marshal(change)
			if err != nil {
				return program.Diff{}, err
			}
			return program.Diff{Changed: true, Text: string(encoded)}, nil
		}
	}
	return program.Diff{}, nil
}

func (v LiveReadView) snapshotObject(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Namespace != v.Namespace() {
		return nil, errors.New("object is outside snapshot namespace")
	}
	var object any
	switch ref.Kind {
	case "Service":
		for _, item := range v.Services() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	case "Deployment":
		for _, item := range v.Deployments() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	case "ConfigMap":
		for _, item := range v.ConfigMaps() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	case "Pod":
		for _, item := range v.Pods() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	case "ReplicaSet":
		for _, item := range v.ReplicaSets() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	case "Endpoints":
		for _, item := range v.Endpoints() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	case "EndpointSlice":
		for _, item := range v.EndpointSlices() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	case "NetworkPolicy":
		for _, item := range v.NetworkPolicies() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				object = &item
				break
			}
		}
	default:
		return nil, fmt.Errorf("source diff does not support %s", ref.Kind)
	}
	if object == nil {
		return nil, fmt.Errorf("%w: %s/%s", errSnapshotObjectAbsent, ref.Kind, ref.Name)
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}
	result := &unstructured.Unstructured{Object: content}
	result.SetAPIVersion(ref.APIVersion)
	result.SetKind(ref.Kind)
	result.SetNamespace(ref.Namespace)
	result.SetName(ref.Name)
	return result, nil
}

package runtime

import (
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

// PrimitiveProber performs one-off probes in the isolated prober process.
// Implementations must support concurrent calls from shared diagnosis views.
// A diagnosis may also finish its reads after RunDiagnose returns on timeout.
type PrimitiveProber interface {
	Probe(context.Context, sdk.ObjectRef, int32, *program.HTTPProbe) (program.ProbeResult, error)
}

// LiveReadView adds read-only primitives to one detector snapshot. Its
// dependencies expose reads only; no program receives a Kubernetes writer.
type LiveReadView struct {
	sdk.DetectionContext
	Context            context.Context
	Client             kubernetes.Interface
	DatabaseExecConfig *rest.Config
	ProtectedNamespace string
	Prober             PrimitiveProber
	Source             PlanSource
	SourceCommit       string
	// SourceRevision resolves lazily, after snapshot acquisition has released controller locks.
	SourceRevision func() string
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
	if since < 0 {
		return "", errors.New("pod log duration must not be negative")
	}
	options := corev1.PodLogOptions{Container: container}
	if since > 0 {
		seconds := int64(since / time.Second)
		if since%time.Second != 0 {
			seconds++
		}
		options.SinceSeconds = &seconds
	}
	return v.logs(ref, options)
}

func (v LiveReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	if options.Container == "" {
		return "", errors.New("logs require an explicit container")
	}
	return v.logs(ref, options)
}
func (v LiveReadView) logs(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	if ref.Kind != "Pod" || (ref.APIVersion != "" && ref.APIVersion != "v1") || ref.Namespace != v.Namespace() || ref.Name == "" || v.Client == nil {
		return "", errors.New("pod logs require a Pod in the snapshot namespace and a Kubernetes reader")
	}
	bounded, err := program.BoundedLogOptions(options)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	stream, err := v.Client.CoreV1().Pods(ref.Namespace).GetLogs(ref.Name, &bounded).Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("read logs of pod %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, *bounded.LimitBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(data)) > *bounded.LimitBytes {
		return "", errors.New("logs exceed byte limit")
	}
	return string(data), nil
}

func int64ptr(value int64) *int64 { return &value }

func (v LiveReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	if ref.Kind == "Pod" {
		if ref.Namespace != v.Namespace() || ref.Name == "" || port < 1 || port > 65535 {
			return program.ProbeResult{}, errors.New("Pod TCP probe requires a namespaced Pod and valid port")
		}
		prober, ok := v.Prober.(interface {
			ProbePod(context.Context, sdk.ObjectRef, string, int32) (program.ProbeResult, error)
		})
		if !ok {
			return program.ProbeResult{}, errors.New("Pod TCP prober is unavailable")
		}
		for _, pod := range v.Pods() {
			if pod.Namespace == ref.Namespace && pod.Name == ref.Name {
				if pod.Spec.HostNetwork || net.ParseIP(pod.Status.PodIP) == nil {
					return program.ProbeResult{}, errors.New("Pod TCP probe requires a captured application Pod IP without host networking")
				}
				ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
				defer cancel()
				return prober.ProbePod(ctx, ref, pod.Status.PodIP, port)
			}
		}
		return program.ProbeResult{}, errors.New("Pod TCP target is absent from snapshot")
	}
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
	if v.SourceRevision != nil {
		v.SourceCommit = v.SourceRevision()
	}
	if v.Source == nil || v.SourceCommit == "" {
		return program.Diff{}, errors.New("accepted source is unavailable")
	}
	observed, err := v.snapshotObject(ref)
	if err != nil && !errors.Is(err, errSnapshotObjectAbsent) {
		return program.Diff{}, err
	}
	ctx, cancel := context.WithTimeout(v.context(), DefaultDiagnosisDeadline)
	defer cancel()
	var source *unstructured.Unstructured
	if ref.Namespace != v.Namespace() {
		reader, ok := v.Source.(interface {
			AcceptedScopedManifest(context.Context, string, string, sdk.ObjectRef) (*unstructured.Unstructured, error)
		})
		if !ok {
			return program.Diff{}, errors.New("qualified accepted source unavailable")
		}
		source, err = reader.AcceptedScopedManifest(ctx, v.SourceCommit, v.Namespace(), ref)
	} else {
		source, err = v.Source.AcceptedManifest(ctx, v.SourceCommit, ref)
	}
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
	if ref.Namespace != v.Namespace() {
		projected, projectErr := sdk.ProjectObservationObject(source)
		if projectErr != nil {
			return program.Diff{}, projectErr
		}
		source = projected
		before, _ := json.Marshal(comparableFields(source.Object))
		var actual any
		if observed != nil {
			actual = comparableFields(observed.Object)
			if !(sdk.ObservationTarget{Kind: ref.Kind}).Cluster() {
				actual = projectSourceFields(actual, comparableFields(source.Object))
			}
		}
		after, _ := json.Marshal(actual)
		return program.Diff{Changed: string(before) != string(after), Text: fmt.Sprintf("accepted: %s\nobserved: %s", before, after)}, nil
	}
	if observed == nil {
		if v.Client != nil {
			if _, err := dryRunAcceptedSource(ctx, v.Client, ref, source); err != nil {
				return program.Diff{}, fmt.Errorf("normalize accepted manifest for %s/%s: %w", ref.Kind, ref.Name, err)
			}
		}
		before, err := json.Marshal(comparableFields(source.Object))
		if err != nil {
			return program.Diff{}, err
		}
		return program.Diff{Changed: true, Text: fmt.Sprintf("accepted: %s\nobserved: null", before)}, nil
	}
	accepted := comparableFields(source.Object)
	actual := comparableFields(observed.Object)
	left := accepted
	right := projectSourceFields(actual, left)
	if v.Client != nil {
		normalized, normalizeErr := dryRunAcceptedSource(ctx, v.Client, ref, source)
		if normalizeErr != nil {
			return program.Diff{}, fmt.Errorf("normalize accepted manifest for %s/%s: %w", ref.Kind, ref.Name, normalizeErr)
		}
		left = comparableFields(normalized.Object)
		right = actual
	}
	// Apply normalization answers whether reconciliation changes live state. It
	// may retain externally owned fields and cannot represent accepted source.
	// Keep the two complete, source-grounded sides even when Changed is false,
	// so callers can compare individual fields (including source absence).
	normalizedBefore, err := json.Marshal(left)
	if err != nil {
		return program.Diff{}, err
	}
	normalizedAfter, err := json.Marshal(right)
	if err != nil {
		return program.Diff{}, err
	}
	before, err := json.Marshal(accepted)
	if err != nil {
		return program.Diff{}, err
	}
	after, err := json.Marshal(actual)
	if err != nil {
		return program.Diff{}, err
	}
	return program.Diff{Changed: string(normalizedBefore) != string(normalizedAfter), Text: fmt.Sprintf("accepted: %s\nobserved: %s", before, after)}, nil
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
	for _, key := range []string{"spec", "data", "binaryData", "webhooks", "subsets", "rules", "roleRef", "subjects", "aggregationRule"} {
		if value, ok := object[key]; ok {
			result[key] = value
		}
	}
	if metadata, ok := object["metadata"].(map[string]any); ok {
		selected := map[string]any{}
		for _, key := range []string{"labels", "annotations", "deletionTimestamp", "finalizers"} {
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
	delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
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
		if _, err := v.scopedObject(ref); err != nil && !errors.Is(err, errSnapshotObjectAbsent) {
			return program.Diff{}, err
		}
	}
	for _, change := range baseline.Changes {
		if change.Kind == ref.Kind && change.Name == ref.Name && ((change.Namespace == "" && ref.Namespace == v.Namespace()) || change.Namespace == ref.Namespace) && (change.APIVersion == "" || change.APIVersion == ref.APIVersion) {
			encoded, err := json.Marshal(change)
			if err != nil {
				return program.Diff{}, err
			}
			return program.Diff{Changed: true, Text: string(encoded)}, nil
		}
	}
	return program.Diff{}, nil
}

func (v LiveReadView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if err := (TypedPlanClient{ProtectedNamespace: v.ProtectedNamespace}).checkPlanNamespace(ref.Namespace); err != nil {
		return nil, err
	}
	// Declared scoped observations carry availability and canonical projection.
	// A generic reader must not turn that evidence into a missing object.
	if ref.Namespace != v.Namespace() {
		target := sdk.ObservationTarget{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}
		if target.Cluster() {
			target.Name = ref.Name
		}
		for _, status := range sdk.ScopedSource(v.DetectionContext).ObservationStatuses() {
			if status.Target == target {
				return v.scopedObject(ref)
			}
		}
	}
	if source, ok := v.DetectionContext.(sdk.ObjectReader); ok {
		return source.Object(ref)
	}
	if ref.Namespace != v.Namespace() {
		return v.scopedObject(ref)
	}
	return sdk.FindObject(sdk.TypedObjects(v.DetectionContext), ref)
}
func (v LiveReadView) snapshotObject(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, err := v.Object(ref)
	if errors.Is(err, sdk.ErrObjectNotCaptured) || apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %v", errSnapshotObjectAbsent, err)
	}
	return object, err
}

func (v LiveReadView) scopedObject(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	source, ok := v.DetectionContext.(sdk.ScopedObservationSource)
	if !ok {
		return nil, errors.New("scoped observations unavailable")
	}
	t := sdk.ObservationTarget{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}
	if t.Cluster() {
		t.Name = ref.Name
	}
	matched := false
	for _, status := range source.ObservationStatuses() {
		if status.Target == t {
			matched = true
			if status.State != sdk.ObservationObserved {
				return nil, errors.New("scoped observation unavailable")
			}
		}
	}
	if !matched {
		return nil, errors.New("scoped observation undeclared")
	}
	var object any
	switch ref.Kind {
	case "Deployment":
		items, err := source.ScopedDeployments(ref.Namespace)
		if err != nil {
			return nil, err
		}
		for _, o := range items {
			if o.Name == ref.Name {
				object = o
			}
		}
	case "Service":
		items, err := source.ScopedServices(ref.Namespace)
		if err != nil {
			return nil, err
		}
		for _, o := range items {
			if o.Name == ref.Name {
				object = o
			}
		}
	case "Endpoints":
		items, err := source.ScopedEndpoints(ref.Namespace)
		if err != nil {
			return nil, err
		}
		for _, o := range items {
			if o.Name == ref.Name {
				object = o
			}
		}
	case "MutatingWebhookConfiguration":
		item, err := source.MutatingWebhookConfiguration(ref.Name)
		if err != nil {
			return nil, err
		}
		if item != nil {
			object = item
		}
	case "ValidatingWebhookConfiguration":
		item, err := source.ValidatingWebhookConfiguration(ref.Name)
		if err != nil {
			return nil, err
		}
		if item != nil {
			object = item
		}
	}
	if object == nil {
		return nil, errSnapshotObjectAbsent
	}
	b, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	out := &unstructured.Unstructured{Object: m}
	out.SetAPIVersion(ref.APIVersion)
	out.SetKind(ref.Kind)
	delete(out.Object, "status")
	return out, nil
}

// ScopedRefAllowed binds drift admission to the snapshot's exact observed targets.
func (v LiveReadView) ScopedRefAllowed(ref sdk.ObjectRef) bool {
	_, err := v.scopedObject(ref)
	return err == nil || errors.Is(err, errSnapshotObjectAbsent)
}

func (v LiveReadView) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	result, err := sdk.ScopedSource(v.DetectionContext).ScopedDeployments(namespace)
	return result, err
}

func (v LiveReadView) ScopedServices(namespace string) ([]corev1.Service, error) {
	result, err := sdk.ScopedSource(v.DetectionContext).ScopedServices(namespace)
	return result, err
}

func (v LiveReadView) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	result, err := sdk.ScopedSource(v.DetectionContext).ScopedEndpoints(namespace)
	return result, err
}

func (v LiveReadView) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.DetectionContext).MutatingWebhookConfigurations()
	return result, err
}

func (v LiveReadView) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.DetectionContext).ValidatingWebhookConfigurations()
	return result, err
}

func (v LiveReadView) ObservationStatuses() []sdk.ObservationStatus {
	return sdk.ScopedSource(v.DetectionContext).ObservationStatuses()
}
func (v LiveReadView) ObservationGeneration() string {
	return sdk.ScopedSource(v.DetectionContext).ObservationGeneration()
}

func (v LiveReadView) ObservedAt() time.Time {
	if clock, ok := v.DetectionContext.(sdk.ObservationClock); ok {
		return clock.ObservedAt()
	}
	return time.Time{}
}

func (v LiveReadView) TrafficWindow(name string) (traffic.Window, bool) {
	if source, ok := v.DetectionContext.(traffic.Source); ok {
		return source.TrafficWindow(name)
	}
	return traffic.Window{}, false
}

func (v LiveReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if source, ok := v.DetectionContext.(servicehealth.ReachabilitySource); ok {
		return source.ServiceReachability()
	}
	return servicehealth.ReachabilityWindow{}, false
}

func (v LiveReadView) MutatingWebhookConfiguration(name string) (*admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.DetectionContext).MutatingWebhookConfiguration(name)
	return result, err
}

func (v LiveReadView) ValidatingWebhookConfiguration(name string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.DetectionContext).ValidatingWebhookConfiguration(name)
	return result, err
}

func (v LiveReadView) MetricResult(id string) (metrics.Result, bool) {
	return metrics.ResultFrom(v.DetectionContext, id)
}
func (v LiveReadView) AlertsResult() (metrics.AlertsResult, bool) {
	return metrics.AlertsFrom(v.DetectionContext)
}
func (v LiveReadView) QueryMetrics(q metrics.Query) metrics.Result {
	if source, ok := v.DetectionContext.(metrics.QuerySource); ok {
		return source.QueryMetrics(q)
	}
	return program.ReadFrom(v.DetectionContext).QueryMetrics(q)
}

func (v LiveReadView) StatefulSets() []appsv1.StatefulSet {
	if source, ok := v.DetectionContext.(interface{ StatefulSets() []appsv1.StatefulSet }); ok {
		return source.StatefulSets()
	}
	return nil
}

func (v LiveReadView) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	if source, ok := v.DetectionContext.(interface {
		HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
	}); ok {
		return source.HorizontalPodAutoscalers()
	}
	return nil
}

func (v LiveReadView) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	if source, ok := v.DetectionContext.(interface {
		PodDisruptionBudgets() []policyv1.PodDisruptionBudget
	}); ok {
		return source.PodDisruptionBudgets()
	}
	return nil
}

func (v LiveReadView) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if source, ok := v.DetectionContext.(sdk.ObjectCaptureSource); ok {
		return source.CaptureObjects(refs)
	}
	return sdk.TypedObjects(v.DetectionContext), nil
}

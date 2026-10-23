package sdktest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// CheckRenameInvariance requires a diagnosis to survive a consistent rename of
// the recorded application's resources, labels, images, and ports. Namespace
// names, IPs, digest-pinned images, binary ConfigMap data, and HTTP paths stay
// fixed because the corpus cannot safely substitute their external targets.
func CheckRenameInvariance(t testing.TB, p program.Diagnoser, c CorpusCase) {
	t.Helper()
	for _, failure := range renameInvarianceFailures(p, c) {
		t.Error(failure)
	}
}

func renameInvarianceFailures(p program.Diagnoser, c CorpusCase) []string {
	renamed, err := renameCorpusCase(c)
	if err != nil {
		return []string{fmt.Sprintf("program %s rename replay could not rewrite its corpus: %v", p.Spec().ID, err)}
	}
	var failures []string
	for _, tc := range []struct {
		name     string
		snapshot Snapshot
		bindings program.Bindings
		want     program.Result
	}{
		{"open", renamed.Open, renamed.Bindings, program.Match},
		{"fixed", renamed.Fixed, renamed.Bindings, program.NoMatch},
		{"healthy", renamed.Healthy, renamed.PeerBindings, program.NoMatch},
	} {
		verdict := program.RunDiagnose(context.Background(), p, &ReplayReadView{Snapshot: tc.snapshot, Calls: renamed.PhaseRecordings(tc.name)}, tc.bindings, 10*time.Second)
		if verdict.Result != tc.want {
			failures = append(failures, fmt.Sprintf("program %s matched only under original names: renamed %s got %+v, want %s; it likely hard-codes a resource name, port or image; derive these from bindings and the ReadView", p.Spec().ID, tc.name, verdict, tc.want))
		}
	}
	if m, ok := p.(program.Mitigator); ok {
		if err := CheckMitigationRepairs(p, m, renamed); err != nil {
			failures = append(failures, err.Error())
		}
	}
	return failures
}

type renameMapping struct {
	values map[string]string
}

func newRenameMapping() *renameMapping { return &renameMapping{values: map[string]string{}} }

func (m *renameMapping) value(value string) string {
	if value == "" {
		return value
	}
	if mapped, ok := m.values[value]; ok {
		return mapped
	}
	if port, err := strconv.ParseInt(value, 10, 32); err == nil && port > 0 && port <= 65535 && strconv.FormatInt(port, 10) == value {
		m.values[value] = strconv.Itoa(int(renamePort(int32(port))))
	} else {
		m.values[value] = value + "-rx7"
	}
	return m.values[value]
}

// Port rotation is a bijection on valid nonzero TCP/UDP port numbers.
func renamePort(port int32) int32 {
	if port <= 0 || port > 65535 {
		return port
	}
	return (port-1+1000)%65535 + 1
}

func (m *renameMapping) port(port int32) int32 {
	if port > 0 && port <= 65535 {
		m.values[strconv.Itoa(int(port))] = strconv.Itoa(int(renamePort(port)))
	}
	return renamePort(port)
}

func (m *renameMapping) image(image string) string {
	if image == "" || strings.Contains(image, "@") {
		return image
	}
	colon, slash := strings.LastIndex(image, ":"), strings.LastIndex(image, "/")
	if colon > slash {
		repository, tag := image[:colon], image[colon+1:]
		mapped := m.value(repository) + ":" + m.value(tag)
		m.values[image] = mapped
		return mapped
	}
	return m.value(image)
}

func (m *renameMapping) exact(value string) string {
	if mapped, ok := m.values[value]; ok {
		return mapped
	}
	return value
}

func renameTokenChar(character byte) bool {
	return character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func (m *renameMapping) text(value string) string {
	keys := make([]string, 0, len(m.values))
	for key := range m.values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) > len(keys[j])
		}
		return keys[i] < keys[j]
	})
	var result strings.Builder
	for index := 0; index < len(value); {
		matched := false
		for _, key := range keys {
			end := index + len(key)
			if end > len(value) || value[index:end] != key || index > 0 && renameTokenChar(value[index-1]) || end < len(value) && renameTokenChar(value[end]) {
				continue
			}
			result.WriteString(m.values[key])
			index = end
			matched = true
			break
		}
		if !matched {
			result.WriteByte(value[index])
			index++
		}
	}
	return result.String()
}

func renameCorpusCase(c CorpusCase) (CorpusCase, error) {
	m := newRenameMapping()
	renamed := CorpusCase{RootCauseProgramIDs: append([]string(nil), c.RootCauseProgramIDs...)}
	var err error
	for _, item := range []struct {
		source Snapshot
		target *Snapshot
	}{
		{c.Open, &renamed.Open}, {c.Fixed, &renamed.Fixed}, {c.Healthy, &renamed.Healthy},
	} {
		*item.target, err = renameSnapshot(item.source, m)
		if err != nil {
			return CorpusCase{}, err
		}
	}
	for _, snapshot := range []*Snapshot{&renamed.Open, &renamed.Fixed, &renamed.Healthy} {
		renameExactValues(snapshot, m)
	}
	renamed.Bindings = renameBindings(c.Bindings, c.Open.NamespaceName, m)
	renamed.PeerBindings = renameBindings(c.PeerBindings, c.Healthy.NamespaceName, m)
	renamed.Recordings, err = renameRecordings(c.Recordings, c.Open.NamespaceName, m)
	if err != nil {
		return CorpusCase{}, err
	}
	return renamed, nil
}

func renameBindings(bindings program.Bindings, namespace string, m *renameMapping) program.Bindings {
	renamed := program.Bindings{Objects: make(program.ObjectBindings, len(bindings.Objects))}
	for key, ref := range bindings.Objects {
		if ref.Namespace == namespace || ref.Namespace == "" && !(sdk.ObservationTarget{Kind: ref.Kind}).Cluster() {
			ref.Name = m.value(ref.Name)
		}
		renamed.Objects[key] = ref
	}
	renamed.Values = bindings.Clone().Values
	return renamed
}

func renameMeta(meta *metav1.ObjectMeta, namespace string, m *renameMapping) {
	if meta.Namespace != "" && meta.Namespace != namespace {
		return
	}
	meta.Name = m.value(meta.Name)
	for key, value := range meta.Labels {
		meta.Labels[key] = m.value(value)
	}
	for index := range meta.OwnerReferences {
		meta.OwnerReferences[index].Name = m.value(meta.OwnerReferences[index].Name)
	}
}

func renameSelector(selector *metav1.LabelSelector, m *renameMapping) {
	if selector == nil {
		return
	}
	for key, value := range selector.MatchLabels {
		selector.MatchLabels[key] = m.value(value)
	}
	for index := range selector.MatchExpressions {
		for value := range selector.MatchExpressions[index].Values {
			selector.MatchExpressions[index].Values[value] = m.value(selector.MatchExpressions[index].Values[value])
		}
	}
}

func renameObjectRef(ref *corev1.ObjectReference, namespace string, m *renameMapping) {
	if ref != nil && (ref.Namespace == namespace || ref.Namespace == "" && !(sdk.ObservationTarget{Kind: ref.Kind}).Cluster()) {
		ref.Name = m.value(ref.Name)
	}
}

func renameIntOrString(value *intstr.IntOrString, m *renameMapping) {
	if value.Type == intstr.String {
		value.StrVal = m.value(value.StrVal)
	} else {
		value.IntVal = m.port(value.IntVal)
	}
}

func renameSnapshot(source Snapshot, m *renameMapping) (Snapshot, error) {
	data, err := json.Marshal(source)
	if err != nil {
		return Snapshot{}, fmt.Errorf("marshal %s for rename replay: %w", source.SourceName, err)
	}
	var result Snapshot
	if err := json.Unmarshal(data, &result); err != nil {
		return Snapshot{}, fmt.Errorf("copy %s for rename replay: %w", source.SourceName, err)
	}
	result.SourceName, result.Traffic = source.SourceName, source.Traffic
	result.Metrics, result.Alerts = source.Metrics, source.Alerts
	namespace := source.NamespaceName
	for index := range result.ConfigMapList {
		renameMeta(&result.ConfigMapList[index].ObjectMeta, namespace, m)
	}
	for index := range result.ServiceList {
		service := &result.ServiceList[index]
		renameMeta(&service.ObjectMeta, namespace, m)
		service.Spec.HealthCheckNodePort = m.port(service.Spec.HealthCheckNodePort)
		for key, value := range service.Spec.Selector {
			service.Spec.Selector[key] = m.value(value)
		}
		for port := range service.Spec.Ports {
			item := &service.Spec.Ports[port]
			item.Name = m.value(item.Name)
			item.Port = m.port(item.Port)
			item.NodePort = m.port(item.NodePort)
			renameIntOrString(&item.TargetPort, m)
		}
	}
	for index := range result.PodList {
		pod := &result.PodList[index]
		renameMeta(&pod.ObjectMeta, namespace, m)
		renamePodSpec(&pod.Spec, m)
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses, pod.Status.EphemeralContainerStatuses} {
			for status := range statuses {
				statuses[status].Name = m.value(statuses[status].Name)
				statuses[status].Image = m.image(statuses[status].Image)
			}
		}
	}
	for index := range result.DeploymentList {
		deployment := &result.DeploymentList[index]
		renameMeta(&deployment.ObjectMeta, namespace, m)
		renameWorkloadTemplate(&deployment.Spec.Template, deployment.Spec.Selector, namespace, m)
	}
	for index := range result.ReplicaSetList {
		replicaSet := &result.ReplicaSetList[index]
		renameMeta(&replicaSet.ObjectMeta, namespace, m)
		renameWorkloadTemplate(&replicaSet.Spec.Template, replicaSet.Spec.Selector, namespace, m)
	}
	for index := range result.EndpointList {
		endpoints := &result.EndpointList[index]
		renameMeta(&endpoints.ObjectMeta, namespace, m)
		for subset := range endpoints.Subsets {
			for address := range endpoints.Subsets[subset].Addresses {
				renameObjectRef(endpoints.Subsets[subset].Addresses[address].TargetRef, namespace, m)
			}
			for address := range endpoints.Subsets[subset].NotReadyAddresses {
				renameObjectRef(endpoints.Subsets[subset].NotReadyAddresses[address].TargetRef, namespace, m)
			}
			for port := range endpoints.Subsets[subset].Ports {
				item := &endpoints.Subsets[subset].Ports[port]
				item.Name = m.value(item.Name)
				item.Port = m.port(item.Port)
			}
		}
	}
	for index := range result.EndpointSliceList {
		slice := &result.EndpointSliceList[index]
		renameMeta(&slice.ObjectMeta, namespace, m)
		for endpoint := range slice.Endpoints {
			renameObjectRef(slice.Endpoints[endpoint].TargetRef, namespace, m)
		}
		for port := range slice.Ports {
			if slice.Ports[port].Name != nil {
				*slice.Ports[port].Name = m.value(*slice.Ports[port].Name)
			}
			if slice.Ports[port].Port != nil {
				*slice.Ports[port].Port = m.port(*slice.Ports[port].Port)
			}
		}
	}
	for index := range result.NetworkPolicyList {
		policy := &result.NetworkPolicyList[index]
		renameMeta(&policy.ObjectMeta, namespace, m)
		renameSelector(&policy.Spec.PodSelector, m)
		for rule := range policy.Spec.Ingress {
			for peer := range policy.Spec.Ingress[rule].From {
				renameNetworkPolicyPeer(&policy.Spec.Ingress[rule].From[peer], m)
			}
			for port := range policy.Spec.Ingress[rule].Ports {
				renameNetworkPolicyPort(&policy.Spec.Ingress[rule].Ports[port], m)
			}
		}
		for rule := range policy.Spec.Egress {
			for peer := range policy.Spec.Egress[rule].To {
				renameNetworkPolicyPeer(&policy.Spec.Egress[rule].To[peer], m)
			}
			for port := range policy.Spec.Egress[rule].Ports {
				renameNetworkPolicyPort(&policy.Spec.Egress[rule].Ports[port], m)
			}
		}
	}
	for index := range result.EventList {
		event := &result.EventList[index]
		renameMeta(&event.ObjectMeta, namespace, m)
		renameObjectRef(&event.InvolvedObject, namespace, m)
		renameObjectRef(event.Related, namespace, m)
	}
	return result, nil
}

func renameWorkloadTemplate(template *corev1.PodTemplateSpec, selector *metav1.LabelSelector, namespace string, m *renameMapping) {
	renameSelector(selector, m)
	renameMeta(&template.ObjectMeta, namespace, m)
	renamePodSpec(&template.Spec, m)
}

func renamePodSpec(spec *corev1.PodSpec, m *renameMapping) {
	for key, value := range spec.NodeSelector {
		spec.NodeSelector[key] = m.value(value)
	}
	if spec.Affinity != nil {
		if spec.Affinity.NodeAffinity != nil {
			affinity := spec.Affinity.NodeAffinity
			if selector := affinity.RequiredDuringSchedulingIgnoredDuringExecution; selector != nil {
				for term := range selector.NodeSelectorTerms {
					renameNodeSelectorTerm(&selector.NodeSelectorTerms[term], m)
				}
			}
			for term := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
				renameNodeSelectorTerm(&affinity.PreferredDuringSchedulingIgnoredDuringExecution[term].Preference, m)
			}
		}
		if affinity := spec.Affinity.PodAffinity; affinity != nil {
			for term := range affinity.RequiredDuringSchedulingIgnoredDuringExecution {
				renamePodAffinityTerm(&affinity.RequiredDuringSchedulingIgnoredDuringExecution[term], m)
			}
			for term := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
				renamePodAffinityTerm(&affinity.PreferredDuringSchedulingIgnoredDuringExecution[term].PodAffinityTerm, m)
			}
		}
		if affinity := spec.Affinity.PodAntiAffinity; affinity != nil {
			for term := range affinity.RequiredDuringSchedulingIgnoredDuringExecution {
				renamePodAffinityTerm(&affinity.RequiredDuringSchedulingIgnoredDuringExecution[term], m)
			}
			for term := range affinity.PreferredDuringSchedulingIgnoredDuringExecution {
				renamePodAffinityTerm(&affinity.PreferredDuringSchedulingIgnoredDuringExecution[term].PodAffinityTerm, m)
			}
		}
	}
	for index := range spec.TopologySpreadConstraints {
		renameSelector(spec.TopologySpreadConstraints[index].LabelSelector, m)
	}
	spec.ServiceAccountName = m.value(spec.ServiceAccountName)
	for index := range spec.ImagePullSecrets {
		spec.ImagePullSecrets[index].Name = m.value(spec.ImagePullSecrets[index].Name)
	}
	for index := range spec.Volumes {
		volume := &spec.Volumes[index]
		volume.Name = m.value(volume.Name)
		if volume.ConfigMap != nil {
			volume.ConfigMap.Name = m.value(volume.ConfigMap.Name)
		}
		if volume.Secret != nil {
			volume.Secret.SecretName = m.value(volume.Secret.SecretName)
		}
		if volume.PersistentVolumeClaim != nil {
			volume.PersistentVolumeClaim.ClaimName = m.value(volume.PersistentVolumeClaim.ClaimName)
		}
		if volume.Projected != nil {
			for source := range volume.Projected.Sources {
				if volume.Projected.Sources[source].ConfigMap != nil {
					ref := volume.Projected.Sources[source].ConfigMap
					ref.Name = m.value(ref.Name)
				}
				if volume.Projected.Sources[source].Secret != nil {
					ref := volume.Projected.Sources[source].Secret
					ref.Name = m.value(ref.Name)
				}
			}
		}
	}
	for index := range spec.Containers {
		renameContainer(&spec.Containers[index], m)
	}
	for index := range spec.InitContainers {
		renameContainer(&spec.InitContainers[index], m)
	}
	for index := range spec.EphemeralContainers {
		container := &spec.EphemeralContainers[index].EphemeralContainerCommon
		renameContainerFields(&container.Name, &container.Image, container.Ports, container.Env, container.EnvFrom, container.VolumeMounts, m)
		renameContainerProbes(container.LivenessProbe, container.ReadinessProbe, container.StartupProbe, container.Lifecycle, m)
	}
}

func renameNodeSelectorTerm(term *corev1.NodeSelectorTerm, m *renameMapping) {
	for _, expressions := range [][]corev1.NodeSelectorRequirement{term.MatchExpressions, term.MatchFields} {
		for expression := range expressions {
			for value := range expressions[expression].Values {
				expressions[expression].Values[value] = m.value(expressions[expression].Values[value])
			}
		}
	}
}

func renamePodAffinityTerm(term *corev1.PodAffinityTerm, m *renameMapping) {
	renameSelector(term.LabelSelector, m)
	renameSelector(term.NamespaceSelector, m)
}

func renameContainer(container *corev1.Container, m *renameMapping) {
	renameContainerFields(&container.Name, &container.Image, container.Ports, container.Env, container.EnvFrom, container.VolumeMounts, m)
	renameContainerProbes(container.LivenessProbe, container.ReadinessProbe, container.StartupProbe, container.Lifecycle, m)
}

func renameContainerProbes(liveness, readiness, startup *corev1.Probe, lifecycle *corev1.Lifecycle, m *renameMapping) {
	for _, probe := range []*corev1.Probe{liveness, readiness, startup} {
		if probe != nil {
			renameProbeHandler(&probe.ProbeHandler, m)
		}
	}
	if lifecycle != nil {
		for _, handler := range []*corev1.LifecycleHandler{lifecycle.PostStart, lifecycle.PreStop} {
			if handler != nil {
				if handler.HTTPGet != nil {
					renameIntOrString(&handler.HTTPGet.Port, m)
				}
				if handler.TCPSocket != nil {
					renameIntOrString(&handler.TCPSocket.Port, m)
				}
			}
		}
	}
}

func renameProbeHandler(handler *corev1.ProbeHandler, m *renameMapping) {
	if handler.HTTPGet != nil {
		renameIntOrString(&handler.HTTPGet.Port, m)
	}
	if handler.TCPSocket != nil {
		renameIntOrString(&handler.TCPSocket.Port, m)
	}
	if handler.GRPC != nil {
		handler.GRPC.Port = m.port(handler.GRPC.Port)
	}
}

func renameContainerFields(name, image *string, ports []corev1.ContainerPort, env []corev1.EnvVar, envFrom []corev1.EnvFromSource, mounts []corev1.VolumeMount, m *renameMapping) {
	*name = m.value(*name)
	*image = m.image(*image)
	for index := range ports {
		ports[index].Name = m.value(ports[index].Name)
		ports[index].ContainerPort = m.port(ports[index].ContainerPort)
		ports[index].HostPort = m.port(ports[index].HostPort)
	}
	for index := range env {
		if env[index].ValueFrom != nil {
			if env[index].ValueFrom.ConfigMapKeyRef != nil {
				ref := env[index].ValueFrom.ConfigMapKeyRef
				ref.Name = m.value(ref.Name)
			}
			if env[index].ValueFrom.SecretKeyRef != nil {
				ref := env[index].ValueFrom.SecretKeyRef
				ref.Name = m.value(ref.Name)
			}
		}
	}
	for index := range envFrom {
		if envFrom[index].ConfigMapRef != nil {
			ref := envFrom[index].ConfigMapRef
			ref.Name = m.value(ref.Name)
		}
		if envFrom[index].SecretRef != nil {
			ref := envFrom[index].SecretRef
			ref.Name = m.value(ref.Name)
		}
	}
	for index := range mounts {
		mounts[index].Name = m.value(mounts[index].Name)
	}
}

func renameNetworkPolicyPeer(peer *networkingv1.NetworkPolicyPeer, m *renameMapping) {
	renameSelector(peer.PodSelector, m)
	renameSelector(peer.NamespaceSelector, m)
}

func renameNetworkPolicyPort(port *networkingv1.NetworkPolicyPort, m *renameMapping) {
	if port.Port != nil {
		renameIntOrString(port.Port, m)
	}
	if port.EndPort != nil {
		*port.EndPort = m.port(*port.EndPort)
	}
}

func renameExactValues(snapshot *Snapshot, m *renameMapping) {
	for index := range snapshot.ConfigMapList {
		for key, value := range snapshot.ConfigMapList[index].Data {
			snapshot.ConfigMapList[index].Data[key] = m.exact(value)
		}
	}
	for index := range snapshot.PodList {
		renamePodEnv(&snapshot.PodList[index].Spec, m)
	}
	for index := range snapshot.DeploymentList {
		renamePodEnv(&snapshot.DeploymentList[index].Spec.Template.Spec, m)
	}
	for index := range snapshot.ReplicaSetList {
		renamePodEnv(&snapshot.ReplicaSetList[index].Spec.Template.Spec, m)
	}
	for index := range snapshot.EventList {
		snapshot.EventList[index].Message = m.text(snapshot.EventList[index].Message)
	}
}

func renamePodEnv(spec *corev1.PodSpec, m *renameMapping) {
	for _, containers := range [][]corev1.Container{spec.Containers, spec.InitContainers} {
		for index := range containers {
			for env := range containers[index].Env {
				containers[index].Env[env].Value = m.exact(containers[index].Env[env].Value)
			}
		}
	}
	for index := range spec.EphemeralContainers {
		for env := range spec.EphemeralContainers[index].Env {
			spec.EphemeralContainers[index].Env[env].Value = m.exact(spec.EphemeralContainers[index].Env[env].Value)
		}
	}
}

func renameRecordings(calls []PrimitiveCall, namespace string, m *renameMapping) ([]PrimitiveCall, error) {
	renamed := make([]PrimitiveCall, len(calls))
	for index, call := range calls {
		renamed[index] = call
		if strings.HasPrefix(call.Method, "Scoped") || strings.HasPrefix(call.Method, "MutatingWebhookConfiguration") || strings.HasPrefix(call.Method, "ValidatingWebhookConfiguration") {
			continue
		}
		var args []json.RawMessage
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return nil, fmt.Errorf("recording %d %s arguments: %w", index, call.Method, err)
		}
		if len(args) == 0 {
			return nil, fmt.Errorf("recording %d %s has no resource reference", index, call.Method)
		}
		var ref sdk.ObjectRef
		if err := json.Unmarshal(args[0], &ref); err != nil {
			return nil, fmt.Errorf("recording %d %s reference: %w", index, call.Method, err)
		}
		if ref.Namespace == namespace || ref.Namespace == "" && !(sdk.ObservationTarget{Kind: ref.Kind}).Cluster() {
			ref.Name = m.value(ref.Name)
		}
		args[0], _ = json.Marshal(ref)
		switch call.Method {
		case "Logs":
			if len(args) != 3 {
				return nil, fmt.Errorf("recording %d Logs needs reference, container and duration", index)
			}
			var container, logs string
			if err := json.Unmarshal(args[1], &container); err != nil {
				return nil, fmt.Errorf("recording %d Logs container: %w", index, err)
			}
			args[1], _ = json.Marshal(m.exact(container))
			if len(call.Result) > 0 && string(call.Result) != "null" {
				if err := json.Unmarshal(call.Result, &logs); err != nil {
					return nil, fmt.Errorf("recording %d Logs result: %w", index, err)
				}
				renamed[index].Result, _ = json.Marshal(m.text(logs))
			}
		case "ProbeTCP":
			if len(args) != 2 {
				return nil, fmt.Errorf("recording %d ProbeTCP needs reference and port", index)
			}
			var port int32
			if err := json.Unmarshal(args[1], &port); err != nil {
				return nil, fmt.Errorf("recording %d ProbeTCP port: %w", index, err)
			}
			args[1], _ = json.Marshal(m.port(port))
		case "ProbeHTTP":
			if len(args) != 2 {
				return nil, fmt.Errorf("recording %d ProbeHTTP needs reference and request", index)
			}
			var request program.HTTPProbe
			if err := json.Unmarshal(args[1], &request); err != nil {
				return nil, fmt.Errorf("recording %d ProbeHTTP request: %w", index, err)
			}
			for key, value := range request.Headers {
				request.Headers[key] = m.exact(value)
			}
			args[1], _ = json.Marshal(request)
		case "DiffAgainstSource", "DiffAgainstBaseline":
			if len(args) != 1 {
				return nil, fmt.Errorf("recording %d %s needs one reference", index, call.Method)
			}
		default:
			return nil, fmt.Errorf("recording %d has unknown method %q", index, call.Method)
		}
		if call.Method == "ProbeTCP" || call.Method == "ProbeHTTP" {
			if len(call.Result) > 0 && string(call.Result) != "null" {
				var result program.ProbeResult
				if err := json.Unmarshal(call.Result, &result); err != nil {
					return nil, fmt.Errorf("recording %d %s result: %w", index, call.Method, err)
				}
				result.Detail = m.text(result.Detail)
				renamed[index].Result, _ = json.Marshal(result)
			}
		}
		if call.Method == "DiffAgainstSource" || call.Method == "DiffAgainstBaseline" {
			if len(call.Result) > 0 && string(call.Result) != "null" {
				var result program.Diff
				if err := json.Unmarshal(call.Result, &result); err != nil {
					return nil, fmt.Errorf("recording %d %s result: %w", index, call.Method, err)
				}
				result.Text = m.text(result.Text)
				renamed[index].Result, _ = json.Marshal(result)
			}
		}
		renamed[index].Arguments, _ = json.Marshal(args)
	}
	return renamed, nil
}

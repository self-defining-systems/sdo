package servicehealth

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk"
)

// Service reachability is the blackbox counterpart of the structural
// endpoint check. Kubernetes can show a Service with ready endpoints while
// its port still refuses connections: a targetPort that matches no listening
// container port, a process that binds another port, or a readiness probe
// that does not exercise the served port. The controller derives one target
// per TCP port of every cluster-IP Service with a selector from its own
// namespace snapshot, asks the credential-free prober pod to dial each one
// afresh through cluster DNS, and hands the samples to this detector. The
// detector fires only while the snapshot shows the Service backed by a ready
// selected pod; without one, the service-endpoints detector owns the fault.
//
// Blind spot: the prober is not an application pod. A NetworkPolicy can admit
// application clients but not the prober, or the reverse. When a policy that
// selects the backing pods does not admit the prober on the target port, the
// detector stays quiet for that port: such dials say nothing about the
// application, and a generic detector that no one authored must not open an
// incident the application never had. Policies that do admit the prober are
// named in the evidence.

const (
	// RuleServicePortUnreachable reports a Service whose ready backends do
	// not accept fresh TCP connections on one or more Service ports.
	RuleServicePortUnreachable = "service-port-unreachable"

	DefaultReachabilityInterval = 10 * time.Second
	DefaultReachabilityTimeout  = 1 * time.Second
	// DefaultReachabilityFailures consecutive failed dials, at least two
	// intervals apart end to end, report a port.
	DefaultReachabilityFailures = 3
	// MaxReachabilityTargets bounds the dialed Service ports per namespace.
	MaxReachabilityTargets = 64

	managedByLabel = "app.kubernetes.io/managed-by"
	managedBySDO   = "sdo"
)

// ReachabilityWatch is emitted by the controller runtime when new dial
// samples may change a verdict; it is not a Kubernetes kind.
var ReachabilityWatch = sdk.WatchKind{APIVersion: "sdo.dev/v1alpha1", Kind: "ServiceReachability"}

// ReachabilityWatches are the kinds whose changes can change the verdict.
func ReachabilityWatches() []sdk.WatchKind {
	return []sdk.WatchKind{
		{APIVersion: "v1", Kind: "Service"},
		{APIVersion: "v1", Kind: "Endpoints"},
		{APIVersion: "v1", Kind: "Pod"},
		{APIVersion: "apps/v1", Kind: "Deployment"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
		ReachabilityWatch,
	}
}

// ReachabilityTarget is one TCP port of one Service, dialed through cluster DNS.
type ReachabilityTarget struct {
	Service  string `json:"service"`
	Port     int32  `json:"port"`
	PortName string `json:"portName,omitempty"`
}

// ID names the target.
func (t ReachabilityTarget) ID() string { return fmt.Sprintf("%s:%d", t.Service, t.Port) }

// DialErrorClass classifies a failed dial.
type DialErrorClass string

const (
	DialRefused     DialErrorClass = "refused"
	DialTimeout     DialErrorClass = "timeout"
	DialDNS         DialErrorClass = "dns"
	DialReset       DialErrorClass = "reset"
	DialUnreachable DialErrorClass = "unreachable"
	DialOther       DialErrorClass = "other"
)

// DialSample is one fresh TCP dial of a target.
type DialSample struct {
	At       time.Time      `json:"at"`
	Duration time.Duration  `json:"durationNs"`
	OK       bool           `json:"ok"`
	Class    DialErrorClass `json:"class,omitempty"`
	Error    string         `json:"error,omitempty"`
}

// TargetObservations are one target's recent samples, oldest first.
type TargetObservations struct {
	Target  ReachabilityTarget `json:"target"`
	Samples []DialSample       `json:"samples"`
	// LastSuccess is the latest successful dial the controller has seen, which
	// may be older than every retained sample.
	LastSuccess *time.Time `json:"lastSuccess,omitempty"`
}

// ProberIdentity is where the dials come from, so NetworkPolicy admission
// can be evaluated against it.
type ProberIdentity struct {
	Namespace string            `json:"namespace,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// ReachabilityWindow is what the controller has observed at ObservedAt.
type ReachabilityWindow struct {
	Baseline        *ReachabilityBaseline `json:"baseline,omitempty"`
	SourceInputs    string                `json:"sourceInputs,omitempty"`
	ObjectiveDigest string                `json:"objectiveDigest,omitempty"`
	ObservedAt      time.Time             `json:"observedAt"`
	Interval        time.Duration         `json:"intervalNs"`
	Timeout         time.Duration         `json:"timeoutNs"`
	Failures        int                   `json:"failures"`
	Targets         []TargetObservations  `json:"targets"`
	// Omitted counts derived targets beyond MaxReachabilityTargets.
	Omitted int            `json:"omitted,omitempty"`
	Prober  ProberIdentity `json:"prober"`
	// LoadError is set when the prober could not be read.
	LoadError string `json:"loadError,omitempty"`
}

// ReachabilitySource is implemented by detection contexts that carry dial
// observations. The controller runtime's snapshots implement it.
type ReachabilitySource interface {
	ServiceReachability() (ReachabilityWindow, bool)
}

// ReachabilityConsumer marks detectors that need dial observations, so the
// controller runs the reachability observer only when one is installed.
type ReachabilityConsumer interface {
	ConsumesServiceReachability() bool
}

// ReachabilityFrom returns the dial observations, when the context carries them.
func ReachabilityFrom(ctx sdk.DetectionContext) (ReachabilityWindow, bool) {
	source, ok := ctx.(ReachabilitySource)
	if !ok {
		return ReachabilityWindow{}, false
	}
	return source.ServiceReachability()
}

// ReachabilityEligible reports whether a Service is dialed: it has a cluster
// IP that load-balances to selected pods. ExternalName Services are DNS
// aliases, headless Services resolve to pod IPs without targetPort
// translation, selector-less Services have hand-managed backends, and
// Services SDO creates for itself are not the application's.
func ReachabilityEligible(service corev1.Service) bool {
	return sdk.ServiceExpectsEndpoints(service) && service.Spec.ClusterIP != corev1.ClusterIPNone &&
		len(service.Spec.Selector) > 0 && service.Labels[managedByLabel] != managedBySDO
}

func servicePortTCP(port corev1.ServicePort) bool {
	return port.Protocol == "" || port.Protocol == corev1.ProtocolTCP
}

// ReachabilityTargets derives the dial targets from a namespace snapshot:
// every TCP port of every eligible Service, sorted by Service and port, at
// most MaxReachabilityTargets. It also returns how many were omitted.
func ReachabilityTargets(ctx sdk.DetectionContext) ([]ReachabilityTarget, int) {
	targets := make([]ReachabilityTarget, 0)
	for _, service := range ctx.Services() {
		if (service.Namespace != "" && service.Namespace != ctx.Namespace()) || !ReachabilityEligible(service) {
			continue
		}
		seen := map[int32]bool{}
		for _, port := range service.Spec.Ports {
			if !servicePortTCP(port) || port.Port < 1 || seen[port.Port] {
				continue
			}
			seen[port.Port] = true
			targets = append(targets, ReachabilityTarget{Service: service.Name, Port: port.Port, PortName: port.Name})
		}
	}
	sort.Slice(targets, func(left, right int) bool {
		if targets[left].Service != targets[right].Service {
			return targets[left].Service < targets[right].Service
		}
		return targets[left].Port < targets[right].Port
	})
	if len(targets) > MaxReachabilityTargets {
		return targets[:MaxReachabilityTargets], len(targets) - MaxReachabilityTargets
	}
	return targets, 0
}

type reachabilityDetector struct {
	spec sdk.DetectorSpec
}

// NewReachabilityDetector reports every eligible Service that has a ready
// selected pod while the last Failures fresh TCP dials of one of its ports,
// all made after the Service gained that ready backend, failed. A prior
// successful dial is not required: a port that never accepted a connection
// while its pods report Ready is exactly the fault this detects. Without dial
// observations in the context it reports nothing. A controller-authorized
// healthy reference retains never-served declarations as observations instead.
func NewReachabilityDetector(spec sdk.DetectorSpec) sdk.Detector {
	return reachabilityDetector{spec: spec}
}

func (d reachabilityDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d reachabilityDetector) ConsumesServiceReachability() bool { return true }

// Confirmed distinguishes an empty verdict from a window still collecting
// samples. An unavailable observer remains an error through Detect.
func (d reachabilityDetector) Confirmed(ctx sdk.DetectionContext) bool {
	w, ok := ReachabilityFrom(ctx)
	if !ok {
		return false
	}
	if w.LoadError != "" {
		return true
	}
	count := w.Failures
	if count < 2 {
		count = DefaultReachabilityFailures
	}
	interval := w.Interval
	if interval <= 0 {
		interval = DefaultReachabilityInterval
	}
	targets, omitted := ReachabilityTargets(ctx)
	if omitted > 0 || w.Omitted > 0 {
		return false
	}
	for _, target := range targets {
		found := false
		for _, o := range w.Targets {
			if o.Target != target || len(o.Samples) < count {
				continue
			}
			recent := o.Samples[len(o.Samples)-count:]
			if w.ObservedAt.Sub(recent[len(recent)-1].At) > 3*interval+w.Timeout {
				continue
			}
			valid := true
			since := time.Time{}
			for _, service := range ctx.Services() {
				if service.Name == target.Service {
					since = readySince(readySelectedPods(ctx, service))
					break
				}
			}
			for i, s := range recent {
				if s.At.IsZero() || s.At.Before(since) || s.At.After(w.ObservedAt) || (i > 0 && !s.At.After(recent[i-1].At)) {
					valid = false
				}
			}
			found = valid
		}
		if !found {
			return false
		}
	}
	return true
}

type failingPort struct {
	port     corev1.ServicePort
	run      []DialSample
	observed TargetObservations
	declared bool
	hint     string
}

func (d reachabilityDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	window, ok := ReachabilityFrom(ctx)
	if !ok {
		return nil, nil
	}
	if window.LoadError != "" {
		return nil, fmt.Errorf("service reachability: %s", window.LoadError)
	}
	failures := window.Failures
	if failures < 2 {
		failures = DefaultReachabilityFailures
	}
	interval := window.Interval
	if interval <= 0 {
		interval = DefaultReachabilityInterval
	}
	timeout := window.Timeout
	if timeout <= 0 {
		timeout = DefaultReachabilityTimeout
	}
	maxAge := 3*interval + timeout
	services := map[string]corev1.Service{}
	for _, service := range ctx.Services() {
		if service.Namespace == "" || service.Namespace == ctx.Namespace() {
			services[service.Name] = service
		}
	}
	observations := append([]TargetObservations(nil), window.Targets...)
	sort.SliceStable(observations, func(left, right int) bool {
		a, b := observations[left].Target, observations[right].Target
		if a.Service != b.Service {
			return a.Service < b.Service
		}
		return a.Port < b.Port
	})
	policies := ctx.NetworkPolicies()
	failing := map[string][]failingPort{}
	ready := map[string][]corev1.Pod{}
	admitting := map[string][]string{}
	order := make([]string, 0)
	for _, observed := range observations {
		if excludedReachabilityTarget(ctx, window, observed.Target) {
			continue
		}
		service, exists := services[observed.Target.Service]
		if !exists || !ReachabilityEligible(service) {
			continue
		}
		port, exists := findServicePort(service, observed.Target.Port)
		if !exists {
			continue
		}
		readyPods := readySelectedPods(ctx, service)
		if len(readyPods) == 0 && ctx.ReadyEndpointCountForService(service.Namespace, service.Name) == 0 {
			continue
		}
		run := failingRun(observed.Samples, readySince(readyPods), failures)
		if len(run) < failures || window.ObservedAt.Sub(run[len(run)-1].At) > maxAge {
			continue
		}
		admitted, names := proberAdmitted(policies, readyPods, port, window.Prober)
		if !admitted {
			continue
		}
		declared, hint := targetPortDeclared(port, readyPods)
		if _, seen := failing[service.Name]; !seen {
			order = append(order, service.Name)
		}
		failing[service.Name] = append(failing[service.Name], failingPort{
			port: port, run: run, observed: observed, declared: declared, hint: hint,
		})
		ready[service.Name] = readyPods
		admitting[service.Name] = mergeSorted(admitting[service.Name], names)
	}
	findings := make([]sdk.Finding, 0, len(order))
	for _, name := range order {
		findings = append(findings, d.finding(ctx, services[name], failing[name], ready[name], admitting[name], window))
	}
	return findings, nil
}

func (d reachabilityDetector) finding(
	ctx sdk.DetectionContext,
	service corev1.Service,
	ports []failingPort,
	readyPods []corev1.Pod,
	policies []string,
	window ReachabilityWindow,
) sdk.Finding {
	namespace := ctx.Namespace()
	summaries := make([]string, 0, len(ports))
	evidence := make([]string, 0, len(ports)+3)
	metadata := make([]map[string]any, 0, len(ports))
	for _, failed := range ports {
		last := failed.run[len(failed.run)-1]
		label := fmt.Sprintf("port %d", failed.port.Port)
		if failed.port.Name != "" {
			label += fmt.Sprintf(" (%s)", failed.port.Name)
		}
		summaries = append(summaries, fmt.Sprintf("%s dials %s %d consecutive times", label, classVerb(last.Class), len(failed.run)))
		lastSuccess := "never"
		if failed.observed.LastSuccess != nil {
			lastSuccess = failed.observed.LastSuccess.UTC().Format(time.RFC3339)
		}
		text := fmt.Sprintf("Port %d (name %q, targetPort %s): the last %d consecutive fresh TCP dials of %s.%s.svc:%d "+
			"from the SDO prober failed over %s (%s) while the Service had a ready selected pod; last error: %s; "+
			"last successful dial: %s.",
			failed.port.Port, failed.port.Name, targetPortString(failed.port), len(failed.run), service.Name, namespace,
			failed.port.Port, last.At.Sub(failed.run[0].At).Round(time.Millisecond), classCounts(failed.run), last.Error, lastSuccess)
		if failed.hint != "" {
			text += " Hint: " + failed.hint
		}
		evidence = append(evidence, text)
		entry := map[string]any{
			"port": failed.port.Port, "name": failed.port.Name, "target_port": targetPortString(failed.port),
			"consecutive_failures": len(failed.run), "error_class": string(last.Class), "last_error": last.Error,
			"target_port_declared": failed.declared, "last_success": "",
		}
		if failed.observed.LastSuccess != nil {
			entry["last_success"] = failed.observed.LastSuccess.UTC().Format(time.RFC3339)
		}
		if window.Baseline != nil {
			entry["baseline_status"] = reachabilityBaselineStatus(ctx, window, failed.observed.Target)
		}
		metadata = append(metadata, entry)
	}
	podNames := make([]string, 0, len(readyPods))
	for _, pod := range readyPods {
		podNames = append(podNames, pod.Name)
	}
	if len(podNames) > 0 {
		evidence = append(evidence, fmt.Sprintf("Ready selected pods: %s. Declared container ports: %s.",
			strings.Join(podNames, ", "), declaredPorts(readyPods)))
	}
	if len(policies) > 0 {
		evidence = append(evidence, fmt.Sprintf("NetworkPolicies selecting these pods (%s) admit the prober on these ports; "+
			"the prober is not an application pod, so application clients may be admitted differently.", strings.Join(policies, ", ")))
	}
	related := make([]sdk.ObjectRef, 0)
	for _, deployment := range matchingDeployments(ctx.Deployments(), service.Namespace, labels.SelectorFromSet(service.Spec.Selector)) {
		related = append(related, sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: deployment.Namespace, Name: deployment.Name})
	}
	sort.Slice(related, func(left, right int) bool { return related[left].Name < related[right].Name })
	for index, pod := range readyPods {
		if index >= 5 {
			break
		}
		related = append(related, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name})
	}
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: RuleServicePortUnreachable, Status: sdk.FindingActive,
		Severity: sdk.SeverityCritical,
		Summary: fmt.Sprintf("Service %s is unreachable although %d selected pod(s) are Ready: %s",
			service.Name, len(readyPods), strings.Join(summaries, "; ")),
		Evidence:         strings.Join(evidence, " "),
		PrimaryResource:  sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name},
		RelatedResources: related,
		Playbooks:        append([]string(nil), d.spec.Playbooks...),
		Metadata: map[string]any{
			"ports": metadata, "ready_pods": len(readyPods), "network_policies": append([]string{}, policies...),
			"dial_interval": window.Interval.String(), "dial_timeout": window.Timeout.String(),
		},
	}
}

func findServicePort(service corev1.Service, number int32) (corev1.ServicePort, bool) {
	for _, port := range service.Spec.Ports {
		if port.Port == number && servicePortTCP(port) {
			return port, true
		}
	}
	return corev1.ServicePort{}, false
}

func readySelectedPods(ctx sdk.DetectionContext, service corev1.Service) []corev1.Pod {
	selected := ctx.PodsForService(service.Namespace, service.Name)
	ready := make([]corev1.Pod, 0, len(selected))
	for _, pod := range selected {
		if pod.DeletionTimestamp == nil && podReady(pod) {
			ready = append(ready, pod)
		}
	}
	sort.Slice(ready, func(left, right int) bool { return ready[left].Name < ready[right].Name })
	return ready
}

// readySince is a lower bound of how long the Service has had a ready pod:
// the earliest Ready transition among its currently ready pods. Zero when
// unknown, which counts every sample.
func readySince(pods []corev1.Pod) time.Time {
	var since time.Time
	for index, pod := range pods {
		var transition time.Time
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady {
				transition = condition.LastTransitionTime.Time
			}
		}
		if transition.IsZero() {
			return time.Time{}
		}
		if index == 0 || transition.Before(since) {
			since = transition
		}
	}
	return since
}

// failingRun returns the trailing failed samples taken at or after since,
// up to needed of them.
func failingRun(samples []DialSample, since time.Time, needed int) []DialSample {
	start := len(samples)
	for start > 0 && len(samples)-start < needed {
		sample := samples[start-1]
		if sample.OK || sample.At.Before(since) {
			break
		}
		start--
	}
	return samples[start:]
}

// resolvedTargetPort returns the container port number and name a Service
// port reaches in pod; number is zero when a named targetPort does not resolve.
func resolvedTargetPort(port corev1.ServicePort, pod corev1.Pod) (int32, string) {
	target := port.TargetPort
	if target.Type == intstr.String && target.StrVal != "" {
		for _, container := range pod.Spec.Containers {
			for _, declared := range container.Ports {
				if declared.Name == target.StrVal && (declared.Protocol == "" || declared.Protocol == corev1.ProtocolTCP) {
					return declared.ContainerPort, declared.Name
				}
			}
		}
		return 0, target.StrVal
	}
	number := target.IntVal
	if target.Type == intstr.String || number == 0 {
		number = port.Port
	}
	for _, container := range pod.Spec.Containers {
		for _, declared := range container.Ports {
			if declared.ContainerPort == number && (declared.Protocol == "" || declared.Protocol == corev1.ProtocolTCP) {
				return number, declared.Name
			}
		}
	}
	return number, ""
}

func targetPortString(port corev1.ServicePort) string {
	if port.TargetPort.Type == intstr.String && port.TargetPort.StrVal != "" {
		return fmt.Sprintf("%q", port.TargetPort.StrVal)
	}
	if port.TargetPort.IntVal == 0 {
		return fmt.Sprintf("%d", port.Port)
	}
	return fmt.Sprintf("%d", port.TargetPort.IntVal)
}

// targetPortDeclared reports whether any ready pod declares the port the
// Service targets, and a hint when none does. Declarations are informational
// in Kubernetes for numeric targets, but a named target that no pod declares
// cannot be routed at all.
func targetPortDeclared(port corev1.ServicePort, pods []corev1.Pod) (bool, string) {
	if len(pods) == 0 {
		return false, ""
	}
	for _, pod := range pods {
		number, name := resolvedTargetPort(port, pod)
		if number != 0 && name != "" {
			return true, ""
		}
	}
	if port.TargetPort.Type == intstr.String && port.TargetPort.StrVal != "" {
		return false, fmt.Sprintf("named targetPort %q matches no containerPort of the ready selected pods, so the Service "+
			"cannot route this port to them.", port.TargetPort.StrVal)
	}
	return false, fmt.Sprintf("targetPort %s matches no containerPort declared by the ready selected pods (declarations "+
		"are informational; compare it with the port the container process listens on).", targetPortString(port))
}

func declaredPorts(pods []corev1.Pod) string {
	seen := map[string]bool{}
	for _, pod := range pods {
		for _, container := range pod.Spec.Containers {
			for _, declared := range container.Ports {
				protocol := declared.Protocol
				if protocol == "" {
					protocol = corev1.ProtocolTCP
				}
				name := declared.Name
				if name != "" {
					name += ":"
				}
				seen[fmt.Sprintf("%s %s%d/%s", container.Name, name, declared.ContainerPort, protocol)] = true
			}
		}
	}
	if len(seen) == 0 {
		return "none"
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return strings.Join(values, ", ")
}

// proberAdmitted evaluates ingress NetworkPolicies for every ready pod and
// reports whether all of them admit the prober on the Service's target port,
// with the names of the isolating policies. It errs toward "not admitted":
// ipBlock peers and namespace selectors on labels other than
// kubernetes.io/metadata.name never match the prober, so an unknown verdict
// suppresses the finding rather than opening an incident on dials a policy
// may have dropped.
func proberAdmitted(policies []networkingv1.NetworkPolicy, pods []corev1.Pod, port corev1.ServicePort, prober ProberIdentity) (bool, []string) {
	names := make([]string, 0)
	for _, pod := range pods {
		number, name := resolvedTargetPort(port, pod)
		isolated, admitted := false, false
		for _, policy := range policies {
			if policy.Namespace != pod.Namespace || !isolatesIngress(policy) {
				continue
			}
			selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
			if err != nil || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			isolated = true
			names = mergeSorted(names, []string{policy.Name})
			for _, rule := range policy.Spec.Ingress {
				if rulePortsAdmit(rule.Ports, number, name) && rulePeersAdmit(rule.From, policy.Namespace, prober) {
					admitted = true
				}
			}
		}
		if isolated && !admitted {
			return false, names
		}
	}
	return true, names
}

func isolatesIngress(policy networkingv1.NetworkPolicy) bool {
	if len(policy.Spec.PolicyTypes) == 0 {
		return true
	}
	for _, kind := range policy.Spec.PolicyTypes {
		if kind == networkingv1.PolicyTypeIngress {
			return true
		}
	}
	return false
}

func rulePortsAdmit(ports []networkingv1.NetworkPolicyPort, number int32, name string) bool {
	if len(ports) == 0 {
		return true
	}
	for _, port := range ports {
		if port.Protocol != nil && *port.Protocol != corev1.ProtocolTCP {
			continue
		}
		if port.Port == nil {
			return true
		}
		if port.Port.Type == intstr.String {
			if name != "" && port.Port.StrVal == name {
				return true
			}
			continue
		}
		end := port.Port.IntVal
		if port.EndPort != nil {
			end = *port.EndPort
		}
		if number != 0 && number >= port.Port.IntVal && number <= end {
			return true
		}
	}
	return false
}

func rulePeersAdmit(peers []networkingv1.NetworkPolicyPeer, policyNamespace string, prober ProberIdentity) bool {
	if len(peers) == 0 {
		return true
	}
	if prober.Namespace == "" {
		return false
	}
	for _, peer := range peers {
		if peer.IPBlock != nil {
			continue
		}
		if peer.NamespaceSelector == nil {
			if prober.Namespace == policyNamespace && selectorMatches(peer.PodSelector, prober.Labels) {
				return true
			}
			continue
		}
		namespaceLabels := map[string]string{"kubernetes.io/metadata.name": prober.Namespace}
		if selectorMatches(peer.NamespaceSelector, namespaceLabels) &&
			(peer.PodSelector == nil || selectorMatches(peer.PodSelector, prober.Labels)) {
			return true
		}
	}
	return false
}

func selectorMatches(selector *metav1.LabelSelector, values map[string]string) bool {
	if selector == nil {
		return false
	}
	parsed, err := metav1.LabelSelectorAsSelector(selector)
	return err == nil && parsed.Matches(labels.Set(values))
}

func classVerb(class DialErrorClass) string {
	switch class {
	case DialRefused:
		return "refused"
	case DialTimeout:
		return "timed out"
	case DialDNS:
		return "did not resolve"
	case DialReset:
		return "were reset"
	case DialUnreachable:
		return "found no route"
	default:
		return "failed"
	}
}

func classCounts(run []DialSample) string {
	counts := map[string]int{}
	for _, sample := range run {
		class := string(sample.Class)
		if class == "" {
			class = string(DialOther)
		}
		counts[class]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s x%d", key, counts[key]))
	}
	return strings.Join(parts, ", ")
}

func mergeSorted(values []string, more []string) []string {
	seen := map[string]bool{}
	merged := make([]string, 0, len(values)+len(more))
	for _, value := range append(append([]string(nil), values...), more...) {
		if !seen[value] {
			seen[value] = true
			merged = append(merged, value)
		}
	}
	sort.Strings(merged)
	return merged
}

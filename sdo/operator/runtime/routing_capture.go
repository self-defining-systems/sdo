package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const maxRoutingCaptureProbes = 24

// A probe's deadline is the client's transport bound. The prober spends half of
// it on the dial and reports a dial that exceeds that as a failed observation,
// so the deadline must dwarf a healthy round trip on a CPU-throttled node:
// a short one turns scheduler delay into a false "port is closed" observation.
// The budget bounds the whole capture, which runs on the incident-open path.
const routingCaptureBudget = 6 * time.Second
const routingCaptureProbeTimeout = 4 * time.Second

var routingCaptureMu sync.Mutex

type routingProbeTarget struct {
	ref  sdk.ObjectRef
	port int32
}

// routingCaptureNote is controller-owned capability evidence, not a probe
// observation. Reflection reads it from the incident's protected corpus mount.
type routingCaptureNote struct {
	Capability string `json:"capability"`
	Status     string `json:"status"`
	Reason     string `json:"reason"`
	Phase      string `json:"phase"`
	Detail     string `json:"detail,omitempty"`
}

func recordRoutingProbeUnavailable(dir, incidentID, phase, reason, detail string) error {
	file := filepath.Join(dir, "routing-probe-note.json")
	if _, err := os.Stat(file); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	data, err := json.Marshal(routingCaptureNote{Capability: "routing_probe", Status: "unavailable", Reason: reason, Phase: phase, Detail: detail})
	if err != nil {
		return err
	}
	if err := os.WriteFile(file, data, 0444); err != nil {
		return err
	}
	slog.Warn("routing_probe=unavailable:"+reason, "incident_id", incidentID, "phase", phase, "detail", detail)
	return nil
}

// captureRoutingEvidence samples real TCP observations even before a program
// exists. It uses the protected phase snapshot and never probes on corpus replay.
func captureRoutingEvidence(ctx context.Context, root, incidentID, phase string, findings []sdk.Finding, view program.ReadView) error {
	live, ok := view.(LiveReadView)
	if !ok {
		if pointer, yes := view.(*LiveReadView); yes && pointer != nil {
			live = *pointer
			ok = true
		}
	}
	routingCaptureMu.Lock()
	defer routingCaptureMu.Unlock()
	dir, err := memory.IncidentCorpusDir(root, incidentID)
	if err != nil {
		return err
	}
	marker := filepath.Join(dir, phase+"-routing.done")
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(dir, phase+".json"))
	if err != nil {
		return err
	}
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return err
	}
	targets := routingProbeTargets(snapshot, findings)
	if len(targets) > 0 && (!ok || live.Prober == nil) {
		reason := "prober_not_configured"
		if !ok {
			reason = "read_view_not_live"
		}
		return recordRoutingProbeUnavailable(dir, incidentID, phase, reason, "")
	}
	live.DetectionContext = snapshot
	destinations := []string{phase}
	observationPhase := ""
	if phase == "fixed" {
		observationPhase = "fixed"
		destinations = nil
		for _, open := range []string{"open@detect", "open@settle", "open@pre-apply"} {
			if _, err := os.Stat(filepath.Join(dir, open+"-recordings.json")); err == nil {
				destinations = append(destinations, open)
			} else if !os.IsNotExist(err) {
				return err
			}
		}
	}
	recorded := map[string]map[string]bool{}
	for _, destination := range destinations {
		raw, err := os.ReadFile(filepath.Join(dir, destination+"-recordings.json"))
		if err != nil {
			return err
		}
		var calls []sdktest.PrimitiveCall
		if err := json.Unmarshal(raw, &calls); err != nil {
			return err
		}
		recorded[destination] = map[string]bool{}
		for _, call := range calls {
			if call.Method == "ProbeTCP" && call.Phase == observationPhase {
				recorded[destination][string(call.Arguments)] = true
			}
		}
	}
	budget, cancel := context.WithTimeout(ctx, routingCaptureBudget)
	defer cancel()
	for index, target := range targets {
		if index >= maxRoutingCaptureProbes {
			break
		}
		if budget.Err() != nil {
			if err := recordRoutingProbeUnavailable(dir, incidentID, phase, "budget_exhausted", fmt.Sprintf("%d of %d probes skipped", len(targets)-index, len(targets))); err != nil {
				return err
			}
			break
		}
		args, _ := json.Marshal([]any{target.ref, target.port})
		needed := false
		for _, destination := range destinations {
			needed = needed || !recorded[destination][string(args)]
		}
		if !needed {
			continue
		}
		probeCtx, probeCancel := context.WithTimeout(budget, routingCaptureProbeTimeout)
		live.Context = probeCtx
		result, probeErr := live.ProbeTCP(target.ref, target.port)
		probeCancel()
		encoded, _ := json.Marshal(result)
		call := sdktest.PrimitiveCall{Phase: observationPhase, Method: "ProbeTCP", Arguments: args, Result: encoded}
		if probeErr != nil {
			call.Error = probeErr.Error()
			if err := recordRoutingProbeUnavailable(dir, incidentID, phase, "probe_failed", call.Error); err != nil {
				return err
			}
		}
		for _, destination := range destinations {
			if !recorded[destination][string(args)] {
				if err := memory.AppendCapturedRecording(root, incidentID, destination, call); err != nil {
					return err
				}
				recorded[destination][string(args)] = true
			}
		}
	}
	return os.WriteFile(marker, []byte("sampled\n"), 0444)
}

func routingProbeTargets(snapshot sdktest.Snapshot, findings []sdk.Finding) []routingProbeTarget {
	relevant := map[string]bool{}
	for _, finding := range findings {
		refs := append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...)
		for _, ref := range finding.ParameterBindings {
			refs = append(refs, ref)
		}
		for _, ref := range refs {
			if ref.Kind == "Service" && ref.Namespace == snapshot.NamespaceName {
				relevant[ref.Name] = true
			}
		}
	}
	if len(relevant) == 0 {
		return nil
	}
	services := []corev1.Service{}
	for _, service := range snapshot.Services() {
		if service.Namespace == snapshot.NamespaceName && relevant[service.Name] {
			services = append(services, service)
		}
	}
	if len(services) == 0 {
		return nil
	}
	// Mandatory healthy-peer diagnosis also needs actual listener observations.
	peers := 0
	for _, service := range snapshot.Services() {
		if service.Namespace != snapshot.NamespaceName || relevant[service.Name] || peers >= 2 || service.Spec.Type == corev1.ServiceTypeExternalName {
			continue
		}
		pods := snapshot.PodsForService(service.Namespace, service.Name)
		ready := len(pods) > 0
		for _, pod := range pods {
			ready = ready && routingPodReady(pod)
		}
		if ready {
			services = append(services, service)
			peers++
		}
	}
	targets := []routingProbeTarget{}
	seen := map[routingProbeTarget]bool{}
	add := func(ref sdk.ObjectRef, port int32) {
		target := routingProbeTarget{ref: ref, port: port}
		if port > 0 && port <= 65535 && !seen[target] {
			seen[target] = true
			targets = append(targets, target)
		}
	}
	for _, service := range services {
		if service.Spec.Type == corev1.ServiceTypeExternalName {
			continue
		}
		pods := snapshot.PodsForService(service.Namespace, service.Name)
		if len(pods) > 2 {
			pods = pods[:2]
		}
		for _, pod := range pods {
			ports := map[int32]bool{}
			for _, container := range pod.Spec.Containers {
				for _, port := range container.Ports {
					if port.Protocol == "" || port.Protocol == corev1.ProtocolTCP {
						ports[port.ContainerPort] = true
					}
				}
			}
			for _, port := range service.Spec.Ports {
				if port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
					continue
				}
				if port.TargetPort.Type == intstr.Int {
					target := port.TargetPort.IntVal
					if target == 0 {
						target = port.Port
					}
					ports[target] = true
				}
			}
			sorted := make([]int32, 0, len(ports))
			for port := range ports {
				sorted = append(sorted, port)
			}
			sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
			if len(sorted) > 4 {
				sorted = sorted[:4]
			}
			for _, port := range sorted {
				add(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, port)
			}
		}
		for _, port := range service.Spec.Ports {
			if port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
				continue
			}
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}
			add(ref, port.Port)
			if port.TargetPort.Type == intstr.Int {
				add(ref, port.TargetPort.IntVal)
			}
		}
		if len(targets) >= maxRoutingCaptureProbes {
			break
		}
	}
	return targets
}

func routingPodReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

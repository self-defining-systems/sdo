package generichealth

import (
	"fmt"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"strings"
)

type template struct {
	r       sdk.ObjectRef
	t       core.PodTemplateSpec
	rolling bool
}

func deploymentRolling(d apps.Deployment) bool {
	if d.Generation > d.Status.ObservedGeneration {
		return true
	}
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	return !d.Spec.Paused && (d.Status.UpdatedReplicas < desired || d.Status.Replicas > d.Status.UpdatedReplicas)
}
func (e *evaluation) templates() []template {
	var ts []template
	for _, v := range e.ctx.Deployments() {
		ts = append(ts, template{ref("Deployment", "apps/v1", &v), v.Spec.Template, deploymentRolling(v)})
	}
	for _, v := range e.ctx.StatefulSets() {
		desired := int32(1)
		if v.Spec.Replicas != nil {
			desired = *v.Spec.Replicas
		}
		ts = append(ts, template{ref("StatefulSet", "apps/v1", &v), v.Spec.Template, v.Generation > v.Status.ObservedGeneration || v.Status.UpdatedReplicas < desired})
	}
	for _, v := range e.ctx.DaemonSets() {
		ts = append(ts, template{ref("DaemonSet", "apps/v1", &v), v.Spec.Template, v.Generation > v.Status.ObservedGeneration || v.Status.UpdatedNumberScheduled < v.Status.DesiredNumberScheduled})
	}
	for _, v := range e.ctx.Jobs() {
		ts = append(ts, template{ref("Job", "batch/v1", &v), v.Spec.Template, v.Status.CompletionTime != nil})
	}
	for _, v := range e.ctx.CronJobs() {
		ts = append(ts, template{ref("CronJob", "batch/v1", &v), v.Spec.JobTemplate.Spec.Template, v.Spec.Suspend != nil && *v.Spec.Suspend})
	}
	return ts
}
func (e *evaluation) podOwner(p core.Pod) sdk.ObjectRef {
	for _, o := range p.OwnerReferences {
		if o.Controller == nil || !*o.Controller {
			continue
		}
		r := sdk.ObjectRef{APIVersion: o.APIVersion, Kind: o.Kind, Namespace: p.Namespace, Name: o.Name}
		if o.Kind == "ReplicaSet" {
			for _, rs := range e.ctx.ReplicaSets() {
				if rs.Namespace == p.Namespace && rs.Name == o.Name {
					for _, parent := range rs.OwnerReferences {
						if parent.Controller != nil && *parent.Controller {
							return sdk.ObjectRef{APIVersion: parent.APIVersion, Kind: parent.Kind, Namespace: p.Namespace, Name: parent.Name}
						}
					}
				}
			}
		}
		return r
	}
	return ref("Pod", "v1", &p)
}
func (e *evaluation) podRolling(p core.Pod) bool {
	owner := e.podOwner(p)
	for _, t := range e.templates() {
		if t.r == owner {
			return t.rolling
		}
	}
	return false
}
func protocol(p core.Protocol) core.Protocol {
	if p == "" {
		return core.ProtocolTCP
	}
	return p
}
func portDeclared(p core.Pod, port core.ServicePort) bool {
	// Numeric target ports route directly; containerPort declarations are optional.
	if port.TargetPort.Type == intstr.Int {
		return true
	}
	for _, c := range p.Spec.Containers {
		for _, cp := range c.Ports {
			if protocol(cp.Protocol) != protocol(port.Protocol) {
				continue
			}
			if port.TargetPort.Type == intstr.String && cp.Name == port.TargetPort.StrVal {
				return true
			}

		}
	}
	return false
}
func (e *evaluation) wiring() {
	ns := e.ctx.Namespace()
	ts := e.templates()
	for _, s := range e.ctx.Services() {
		if s.Namespace != ns || s.DeletionTimestamp != nil {
			continue
		}
		r := ref("Service", "v1", &s)
		if s.Spec.Type == core.ServiceTypeExternalName {
			parts := strings.Split(strings.TrimSuffix(s.Spec.ExternalName, "."), ".")
			if len(parts) >= 3 && parts[1] == ns && parts[2] == "svc" {
				found := false
				for _, peer := range e.ctx.Services() {
					found = found || peer.Namespace == ns && peer.Name == parts[0]
				}
				if !found {
					e.emit("externalname-missing-service", r, "ExternalName target="+s.Spec.ExternalName+" names an absent in-namespace Service", false)
				}
			}
			continue
		}
		if len(s.Spec.Selector) == 0 {
			continue
		}
		selector := labels.SelectorFromSet(s.Spec.Selector)
		pods := []core.Pod{}
		owners := map[sdk.ObjectRef]bool{}
		rolling, templateMatch := false, false
		for _, t := range ts {
			if t.r.Namespace == ns && selector.Matches(labels.Set(t.t.Labels)) {
				templateMatch = true
				rolling = rolling || t.rolling
			}
		}
		for _, p := range e.ctx.PodsForService(ns, s.Name) {
			if p.DeletionTimestamp != nil || p.Status.Phase == core.PodSucceeded || p.Status.Phase == core.PodFailed {
				continue
			}
			pods = append(pods, p)
			rolling = rolling || e.podRolling(p)
			owners[e.podOwner(p)] = true
		}
		if rolling {
			continue
		}
		if len(pods) == 0 && !templateMatch {
			e.emit("service-selector-empty", r, fmt.Sprintf("selector=%v matches neither active pods nor workload templates", s.Spec.Selector), false)
		}
		// Standalone pods are not evidence of multiple workload owners.
		workloadOwners := 0
		for o := range owners {
			if o.Kind != "Pod" {
				workloadOwners++
			}
		}
		if workloadOwners > 1 {
			e.emit("service-multiple-workloads", r, fmt.Sprintf("selector=%v selects %d distinct workload owners", s.Spec.Selector, workloadOwners), false)
		}
		for _, port := range s.Spec.Ports {
			for _, p := range pods {
				if !portDeclared(p, port) {
					e.emit("service-target-port", r, fmt.Sprintf("Service port=%d protocol=%s targetPort=%s not declared by pod %s containers", port.Port, protocol(port.Protocol), port.TargetPort.String(), p.Name), false)
					break
				}
			}
		}
		for _, p := range pods {
			e.networkDeny(s, p)
		}
	}
	for _, t := range ts {
		if t.r.Namespace != ns || t.rolling {
			continue
		}
		for _, dep := range sdk.PodTemplateReferences(t.t) {
			if !dep.Optional && !e.referenceExists(ns, dep) {
				r := sdk.ObjectRef{APIVersion: "v1", Kind: dep.Kind, Namespace: ns, Name: dep.Name}
				e.emit("template-missing-reference", t.r, fmt.Sprintf("pod template requires %s/%s key=%q, absent in namespace %s", dep.Kind, dep.Name, dep.Key, ns), false)
				e.findings[len(e.findings)-1].RelatedResources = []sdk.ObjectRef{r}
				e.findings[len(e.findings)-1].Fingerprint += "/" + dep.Kind + "/" + dep.Name + "/" + dep.Key
			}
		}
	}
	for _, v := range e.ctx.PersistentVolumeClaims() {
		if v.Namespace == ns && v.DeletionTimestamp == nil && v.Status.Phase != core.ClaimBound && e.old(v.CreationTimestamp) {
			e.emit("pvc-not-bound", ref("PersistentVolumeClaim", "v1", &v), "phase="+string(v.Status.Phase)+" expected Bound past progress window", false)
		}
	}
	for _, v := range e.ctx.Ingresses() {
		if v.Namespace != ns || v.DeletionTimestamp != nil {
			continue
		}
		if v.Spec.DefaultBackend != nil {
			e.backend(ref("Ingress", "networking.k8s.io/v1", &v), *v.Spec.DefaultBackend)
		}
		for _, rule := range v.Spec.Rules {
			if rule.HTTP != nil {
				for _, path := range rule.HTTP.Paths {
					e.backend(ref("Ingress", "networking.k8s.io/v1", &v), path.Backend)
				}
			}
		}
	}
}
func (e *evaluation) referenceExists(ns string, r sdk.PodTemplateReference) bool {
	switch r.Kind {
	case "ConfigMap":
		for _, v := range e.ctx.ConfigMaps() {
			if v.Namespace == ns && v.Name == r.Name {
				_, a := v.Data[r.Key]
				_, b := v.BinaryData[r.Key]
				return r.Key == "" || a || b
			}
		}
	case "Secret":
		for _, v := range e.ctx.Secrets() {
			if v.Namespace == ns && v.Name == r.Name {
				if r.Key == "" {
					return true
				}
				for _, k := range v.DataKeys {
					if k == r.Key {
						return true
					}
				}
			}
		}
	case "PersistentVolumeClaim":
		for _, v := range e.ctx.PersistentVolumeClaims() {
			if v.Namespace == ns && v.Name == r.Name {
				return true
			}
		}
	case "ServiceAccount":
		for _, v := range e.ctx.ServiceAccounts() {
			if v.Namespace == ns && v.Name == r.Name {
				return true
			}
		}
	}
	return false
}
func (e *evaluation) backend(r sdk.ObjectRef, b netv1.IngressBackend) {
	if b.Service == nil {
		return
	}
	for _, s := range e.ctx.Services() {
		if s.Namespace == r.Namespace && s.Name == b.Service.Name {
			for _, p := range s.Spec.Ports {
				if b.Service.Port.Name != "" && p.Name == b.Service.Port.Name || b.Service.Port.Number != 0 && p.Port == b.Service.Port.Number {
					return
				}
			}
			e.emit("ingress-service-port", r, fmt.Sprintf("backend Service=%s port=%+v absent", b.Service.Name, b.Service.Port), false)
			return
		}
	}
	e.emit("ingress-service-missing", r, "backend Service="+b.Service.Name+" absent", false)
}
func selects(p netv1.NetworkPolicy, pod core.Pod) bool {
	sel, err := meta.LabelSelectorAsSelector(&p.Spec.PodSelector)
	return err == nil && p.Namespace == pod.Namespace && sel.Matches(labels.Set(pod.Labels))
}
func policyDirection(p netv1.NetworkPolicy, d netv1.PolicyType) bool {
	if len(p.Spec.PolicyTypes) == 0 {
		return d == netv1.PolicyTypeIngress || d == netv1.PolicyTypeEgress && len(p.Spec.Egress) > 0
	}
	for _, t := range p.Spec.PolicyTypes {
		if t == d {
			return true
		}
	}
	return false
}
func (e *evaluation) networkDeny(s core.Service, pod core.Pod) {
	for _, direction := range []netv1.PolicyType{netv1.PolicyTypeIngress, netv1.PolicyTypeEgress} {
		isolated, anyAllow := false, false
		for _, p := range e.ctx.NetworkPolicies() {
			if !selects(p, pod) || !policyDirection(p, direction) {
				continue
			}
			isolated = true
			anyAllow = anyAllow || direction == netv1.PolicyTypeIngress && len(p.Spec.Ingress) > 0 || direction == netv1.PolicyTypeEgress && len(p.Spec.Egress) > 0
		}
		if isolated && !anyAllow {
			e.emit("network-deny-all-"+strings.ToLower(string(direction)), ref("Pod", "v1", &pod), "all selecting NetworkPolicies have no "+string(direction)+" allow rules; pod backs Service "+s.Name, false)
		}
	}
}

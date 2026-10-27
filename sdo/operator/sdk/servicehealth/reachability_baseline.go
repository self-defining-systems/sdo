package servicehealth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"maps"
	"sdo.dev/operator/sdk"
	"sort"
	"time"
)

// BaselineAuthorization is supplied only by trusted health acceptance or an
// explicit healthy-baseline gate after independently checking the goal.
type BaselineAuthorization struct {
	SourceInputs    string `json:"sourceInputs"`
	ObjectiveDigest string `json:"objectiveDigest"`
	Reason          string `json:"reason"`
	GoalHealthy     bool   `json:"goalHealthy"`
}

// ReachabilityBaseline is visible, health-judge-owned accepted diagnostic data.
// It records measured failures, never an inference from missing prior success.
type ReachabilityBaseline struct {
	Version       int                     `json:"version"`
	Authorization BaselineAuthorization   `json:"authorization"`
	CapturedAt    time.Time               `json:"capturedAt"`
	Exclusions    []ReachabilityExclusion `json:"exclusions"`
}

type ReachabilityExclusion struct {
	Namespace     string             `json:"namespace"`
	Target        ReachabilityTarget `json:"target"`
	ServiceUID    string             `json:"serviceUID,omitempty"`
	TargetPort    string             `json:"targetPort"`
	Protocol      string             `json:"protocol"`
	Selector      map[string]string  `json:"selector"`
	Configuration string             `json:"configuration"`
	Samples       []DialSample       `json:"samples"`
}

func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == sha256.Size
}

func (b *ReachabilityBaseline) Validate() error {
	if b == nil || b.Version != 1 || !validDigest(b.Authorization.SourceInputs) || !validDigest(b.Authorization.ObjectiveDigest) || !b.Authorization.GoalHealthy || b.CapturedAt.IsZero() {
		return fmt.Errorf("invalid reachability baseline identity or healthy authorization")
	}
	if b.Authorization.Reason != "healthy-baseline" && b.Authorization.Reason != "health-acceptance" {
		return fmt.Errorf("reachability baseline requires health acceptance or explicit healthy-baseline authority")
	}
	seen := map[string]bool{}
	for _, e := range b.Exclusions {
		key := fmt.Sprintf("%s/%s/%d", e.Namespace, e.Target.Service, e.Target.Port)
		if e.Namespace == "" || e.Target.Service == "" || e.Target.Port < 1 || e.Target.Port > 65535 || e.TargetPort == "" || e.Protocol != string(corev1.ProtocolTCP) || len(e.Selector) == 0 || !validDigest(e.Configuration) || len(e.Samples) < DefaultReachabilityFailures || seen[key] {
			return fmt.Errorf("invalid or duplicate reachability exclusion %s", key)
		}
		seen[key] = true
		for i, s := range e.Samples {
			if s.OK || s.Class == "" || s.At.IsZero() || s.At.After(b.CapturedAt) || (i > 0 && !s.At.After(e.Samples[i-1].At)) {
				return fmt.Errorf("reachability exclusion lacks ordered failed probe evidence")
			}
		}
	}
	return nil
}

type baselineContext struct {
	sdk.DetectionContext
	window ReachabilityWindow
}

func (c baselineContext) ServiceReachability() (ReachabilityWindow, bool) { return c.window, true }

// CaptureReachabilityBaseline captures only targets the ordinary detector
// independently diagnoses from fresh failed probes in this authorized snapshot.
func CaptureReachabilityBaseline(ctx sdk.DetectionContext, a BaselineAuthorization) (*ReachabilityBaseline, error) {
	w, ok := ReachabilityFrom(ctx)
	if !ok {
		return nil, fmt.Errorf("healthy baseline requires measured reachability observations")
	}
	b := &ReachabilityBaseline{Version: 1, Authorization: a, CapturedAt: w.ObservedAt, Exclusions: []ReachabilityExclusion{}}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	w.Baseline = nil
	findings, err := NewReachabilityDetector(sdk.DetectorSpec{}).Detect(context.Background(), baselineContext{ctx, w})
	if err != nil {
		return nil, err
	}
	failing := map[string]bool{}
	for _, f := range findings {
		ports, _ := f.Metadata["ports"].([]map[string]any)
		for _, p := range ports {
			port, _ := p["port"].(int32)
			failing[fmt.Sprintf("%s/%d", f.PrimaryResource.Name, port)] = true
		}
	}
	for _, o := range w.Targets {
		if o.LastSuccess != nil {
			continue
		}
		served := false
		for _, sample := range o.Samples {
			served = served || sample.OK
		}
		if served {
			continue
		}
		if !failing[fmt.Sprintf("%s/%d", o.Target.Service, o.Target.Port)] {
			continue
		}
		signature, ok := ReachabilityTargetConfiguration(ctx, o.Target)
		if !ok {
			continue
		}
		count := w.Failures
		if count < 2 {
			count = DefaultReachabilityFailures
		}
		if count < DefaultReachabilityFailures {
			count = DefaultReachabilityFailures
		}
		if len(o.Samples) < count {
			continue
		}
		for _, service := range ctx.Services() {
			if service.Name != o.Target.Service || service.Namespace != "" && service.Namespace != ctx.Namespace() {
				continue
			}
			port, ok := findServicePort(service, o.Target.Port)
			if !ok {
				continue
			}
			b.Exclusions = append(b.Exclusions, ReachabilityExclusion{Namespace: ctx.Namespace(), Target: o.Target, ServiceUID: string(service.UID), TargetPort: targetPortString(port), Protocol: string(corev1.ProtocolTCP), Selector: maps.Clone(service.Spec.Selector), Configuration: signature, Samples: append([]DialSample(nil), o.Samples[len(o.Samples)-count:]...)})
		}
	}
	sort.Slice(b.Exclusions, func(i, j int) bool {
		a, b := b.Exclusions[i], b.Exclusions[j]
		return fmt.Sprintf("%s/%s/%d", a.Namespace, a.Target.Service, a.Target.Port) < fmt.Sprintf("%s/%s/%d", b.Namespace, b.Target.Service, b.Target.Port)
	})
	return b, b.Validate()
}

// StaleExclusions returns the recorded exclusions whose target configuration no
// longer matches the snapshot. A target that is gone has nothing left to exclude
// and is not stale. Staleness only says the record must be measured again by the
// healthy-baseline gate; it never authorizes a new exclusion.
func (b *ReachabilityBaseline) StaleExclusions(ctx sdk.DetectionContext) []ReachabilityExclusion {
	stale := []ReachabilityExclusion{}
	if b == nil {
		return stale
	}
	for _, e := range b.Exclusions {
		if config, ok := ReachabilityTargetConfiguration(ctx, e.Target); ok && config != e.Configuration {
			stale = append(stale, e)
		}
	}
	return stale
}

// ReachabilityTargetConfiguration binds a target to its observed Service,
// selected backend configuration, referenced ConfigMaps and selecting network policy.
// Incarnation metadata and allocated routing addresses/ports are not application
// configuration. The probe uses Service DNS + service port, not NodePort or an
// allocated IP family. Headless versus virtual-IP routing remains significant.
// It never reads credentials or Secret resources.
func ReachabilityTargetConfiguration(ctx sdk.DetectionContext, target ReachabilityTarget) (string, bool) {
	for _, s := range ctx.Services() {
		if s.Name != target.Service || (s.Namespace != "" && s.Namespace != ctx.Namespace()) {
			continue
		}
		if _, ok := findServicePort(s, target.Port); !ok {
			return "", false
		}
		backends := []string{}
		backendSet, referenced := map[string]bool{}, map[string]bool{}
		pods := readySelectedPods(ctx, s)
		for _, pod := range pods {
			inputs := []string{}
			for _, volume := range pod.Spec.Volumes {
				if volume.ConfigMap != nil {
					referenced[volume.ConfigMap.Name] = true
					raw, _ := json.Marshal(volume.ConfigMap)
					inputs = append(inputs, string(raw))
				}
				if volume.Projected != nil {
					for _, source := range volume.Projected.Sources {
						if source.ConfigMap != nil {
							referenced[source.ConfigMap.Name] = true
							raw, _ := json.Marshal(source.ConfigMap)
							inputs = append(inputs, string(raw))
						}
					}
				}
			}
			sort.Strings(inputs)
			containers := append(append([]corev1.Container{}, pod.Spec.Containers...), pod.Spec.InitContainers...)
			for i := range containers {
				mounts := []corev1.VolumeMount{}
				for _, m := range containers[i].VolumeMounts {
					if m.MountPath != "/var/run/secrets/kubernetes.io/serviceaccount" {
						mounts = append(mounts, m)
					}
				}
				containers[i].VolumeMounts = mounts
				for _, e := range containers[i].Env {
					if e.ValueFrom != nil && e.ValueFrom.ConfigMapKeyRef != nil {
						referenced[e.ValueFrom.ConfigMapKeyRef.Name] = true
					}
				}
				for _, e := range containers[i].EnvFrom {
					if e.ConfigMapRef != nil {
						referenced[e.ConfigMapRef.Name] = true
					}
				}
			}
			podLabels := maps.Clone(pod.Labels)
			delete(podLabels, "pod-template-hash")
			delete(podLabels, "controller-revision-hash")
			raw, _ := json.Marshal(struct {
				Containers        []corev1.Container
				RegularContainers int
				Inputs            []string
				Labels            map[string]string
			}{containers, len(pod.Spec.Containers), inputs, podLabels})
			if !backendSet[string(raw)] {
				backends = append(backends, string(raw))
				backendSet[string(raw)] = true
			}
		}
		sort.Strings(backends)
		policies := []string{}
		for _, p := range ctx.NetworkPolicies() {
			if p.Namespace != "" && p.Namespace != ctx.Namespace() {
				continue
			}
			selector, err := metav1.LabelSelectorAsSelector(&p.Spec.PodSelector)
			selects := err != nil
			if err == nil {
				for _, pod := range pods {
					if selector.Matches(labels.Set(pod.Labels)) {
						selects = true
						break
					}
				}
			}
			if selects {
				raw, _ := json.Marshal(p.Spec)
				policies = append(policies, p.Name+string(raw))
			}
		}
		sort.Strings(policies)
		configMaps := []string{}
		for _, cm := range ctx.ConfigMaps() {
			if !referenced[cm.Name] || cm.Namespace != "" && cm.Namespace != ctx.Namespace() {
				continue
			}
			raw, _ := json.Marshal(struct {
				Name       string
				Data       map[string]string
				BinaryData map[string][]byte
			}{cm.Name, cm.Data, cm.BinaryData})
			configMaps = append(configMaps, string(raw))
		}
		sort.Strings(configMaps)
		spec := s.Spec.DeepCopy()
		headless := spec.ClusterIP == corev1.ClusterIPNone
		spec.ClusterIP, spec.ClusterIPs = "", nil
		spec.IPFamilies, spec.IPFamilyPolicy = nil, nil
		spec.HealthCheckNodePort = 0
		for i := range spec.Ports {
			spec.Ports[i].NodePort = 0
		}
		raw, _ := json.Marshal(struct {
			Namespace                      string
			Headless                       bool
			Spec                           *corev1.ServiceSpec
			Backends, Policies, ConfigMaps []string
		}{ctx.Namespace(), headless, spec, backends, policies, configMaps})
		return fmt.Sprintf("%x", sha256.Sum256(raw)), true
	}
	return "", false
}

func reachabilityBaselineStatus(ctx sdk.DetectionContext, w ReachabilityWindow, target ReachabilityTarget) string {
	b := w.Baseline
	if b == nil {
		return "not-recorded"
	}
	if b.Validate() != nil {
		return "invalid-record"
	}
	if b.Authorization.SourceInputs != w.SourceInputs {
		return "source-changed"
	}
	if b.Authorization.ObjectiveDigest != w.ObjectiveDigest {
		return "objective-changed"
	}
	config, ok := ReachabilityTargetConfiguration(ctx, target)
	if !ok {
		return "target-unavailable"
	}
	for _, e := range b.Exclusions {
		if e.Namespace == ctx.Namespace() && e.Target == target {
			if e.Configuration == config {
				return "accepted-exclusion"
			}
			return "target-configuration-changed"
		}
	}
	return "target-not-recorded"
}

func excludedReachabilityTarget(ctx sdk.DetectionContext, w ReachabilityWindow, target ReachabilityTarget) bool {
	for _, o := range w.Targets {
		if o.Target != target {
			continue
		}
		if o.LastSuccess != nil && !o.LastSuccess.Before(w.baselineCapturedAt()) {
			return false
		}
		for _, s := range o.Samples {
			if s.OK && !s.At.Before(w.baselineCapturedAt()) {
				return false
			}
		}
	}
	return reachabilityBaselineStatus(ctx, w, target) == "accepted-exclusion"
}

func (w ReachabilityWindow) baselineCapturedAt() time.Time {
	if w.Baseline == nil {
		return time.Time{}
	}
	return w.Baseline.CapturedAt
}

// ReachabilityObservation is reference evidence, never a health finding.
type ReachabilityObservation struct {
	Target      ReachabilityTarget `json:"target"`
	Status      string             `json:"status"`
	ReferenceAt time.Time          `json:"referenceAt"`
}

// ReachabilityObservations reports currently applicable never-served evidence.
func ReachabilityObservations(ctx sdk.DetectionContext) []ReachabilityObservation {
	observations := []ReachabilityObservation{}
	w, ok := ReachabilityFrom(ctx)
	if !ok || w.Baseline == nil {
		return observations
	}
	for _, e := range w.Baseline.Exclusions {
		if excludedReachabilityTarget(ctx, w, e.Target) {
			observations = append(observations, ReachabilityObservation{Target: e.Target, Status: "declared-but-never-served", ReferenceAt: w.Baseline.CapturedAt})
		}
	}
	return observations
}

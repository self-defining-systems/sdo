package program

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/datacheck"
	"sdo.dev/operator/sdk/storeops"
)

// Data roles of the application map. An authoritative store is the source of
// truth for its data; a derived one (a cache, an index) is rebuilt from the
// components it is derived from.
const (
	DataRoleAuthoritative = "authoritative"
	DataRoleDerived       = "derived"
)

// AppMap is the accepted application map, read-only: the components, the
// workload each runs as, the data each holds and the calls between them. A
// program reads it through ReadView.AppMap to resolve the store, cache or
// source to act on from a bound object, instead of naming a component.
type AppMap struct {
	Components []AppComponent `json:"components"`
	Edges      []AppEdge      `json:"edges,omitempty"`
}

// AppComponent is one component of the application map. Workload is
// "Kind/name" in the application namespace.
type AppComponent struct {
	Name      string    `json:"name"`
	Workload  string    `json:"workload"`
	Services  []string  `json:"services,omitempty"`
	HoldsData bool      `json:"holds_data"`
	Data      *DataRole `json:"data,omitempty"`
}

// DataRole declares that a component is a data store: its engine, whether it
// is authoritative or derived, and the components a derived copy is rebuilt
// from.
type DataRole struct {
	Engine      string   `json:"engine"`
	Role        string   `json:"role"`
	DerivedFrom []string `json:"derived_from,omitempty"`
}

// AppEdge is one directed call between components.
type AppEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Component returns the component named name.
func (m AppMap) Component(name string) (AppComponent, bool) {
	for _, c := range m.Components {
		if c.Name == name {
			return c, true
		}
	}
	return AppComponent{}, false
}

// Derived lists the derived components rebuilt from source, by name.
func (m AppMap) Derived(source string) []AppComponent {
	var out []AppComponent
	for _, c := range m.Components {
		if c.Data != nil && c.Data.Role == DataRoleDerived && slices.Contains(c.Data.DerivedFrom, source) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Sources lists the components derived is rebuilt from.
func (m AppMap) Sources(derived string) []AppComponent {
	c, ok := m.Component(derived)
	if !ok || c.Data == nil {
		return nil
	}
	var out []AppComponent
	for _, name := range c.Data.DerivedFrom {
		if source, ok := m.Component(name); ok {
			out = append(out, source)
		}
	}
	return out
}

// ComponentOf returns the component whose workload owns ref: the workload
// itself, or a Pod or ReplicaSet it controls, followed through the view's
// controller references.
func (m AppMap) ComponentOf(view sdk.DetectionContext, ref sdk.ObjectRef) (AppComponent, bool) {
	workload := WorkloadOf(view, ref)
	for _, c := range m.Components {
		if c.Workload == workload {
			return c, true
		}
	}
	return AppComponent{}, false
}

// WorkloadRef is the component's workload in namespace.
func (c AppComponent) WorkloadRef(namespace string) sdk.ObjectRef {
	kind, name, _ := strings.Cut(c.Workload, "/")
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: namespace, Name: name}
}

// WorkloadOf returns "Kind/name" of the workload that controls ref: a Pod's
// controller, a ReplicaSet's Deployment, or ref itself.
func WorkloadOf(view sdk.DetectionContext, ref sdk.ObjectRef) string {
	kind, name := ref.Kind, ref.Name
	if kind == "Pod" {
		for _, pod := range view.Pods() {
			if pod.Namespace == ref.Namespace && pod.Name == name {
				if owner := metav1.GetControllerOf(&pod); owner != nil {
					kind, name = owner.Kind, owner.Name
				}
				break
			}
		}
	}
	if kind == "ReplicaSet" {
		for _, replicaSet := range view.ReplicaSets() {
			if replicaSet.Namespace == ref.Namespace && replicaSet.Name == name {
				if owner := metav1.GetControllerOf(&replicaSet); owner != nil {
					kind, name = owner.Kind, owner.Name
				}
				break
			}
		}
	}
	return kind + "/" + name
}

// StoreTargetFor resolves a store target from the application map.
// It takes the component's engine, one running Pod of its workload (ready
// first, then by name) and the container whose image the engine's record
// operations select, or the Pod's only container. It fails when the component
// declares no data role or has no running Pod.
func StoreTargetFor(view ReadView, component string) (StoreTarget, error) {
	m, err := view.AppMap()
	if err != nil {
		return StoreTarget{}, err
	}
	c, ok := m.Component(component)
	if !ok {
		return StoreTarget{}, fmt.Errorf("the application map has no component %q", component)
	}
	if c.Data == nil {
		return StoreTarget{}, fmt.Errorf("component %s declares no data role in the application map", component)
	}
	var pods []corev1.Pod
	for _, pod := range view.Pods() {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil &&
			WorkloadOf(view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}) == c.Workload {
			pods = append(pods, pod)
		}
	}
	if len(pods) == 0 {
		return StoreTarget{}, fmt.Errorf("component %s has no running Pod", component)
	}
	sort.SliceStable(pods, func(i, j int) bool {
		if ready(pods[i]) != ready(pods[j]) {
			return ready(pods[i])
		}
		return pods[i].Name < pods[j].Name
	})
	pod := pods[0]
	container, err := storeContainer(pod, c.Data.Engine)
	if err != nil {
		return StoreTarget{}, fmt.Errorf("component %s Pod %s: %w", component, pod.Name, err)
	}
	return StoreTarget{
		Component: c.Name, Engine: c.Data.Engine, Container: container,
		Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name},
	}, nil
}

func ready(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// storeContainer picks the container of pod that runs engine.
func storeContainer(pod corev1.Pod, engine string) (string, error) {
	if ops, ok := storeops.For(engine); ok {
		pattern, err := regexp.Compile(ops.Image())
		if err != nil {
			return "", err
		}
		var matched []string
		for _, container := range pod.Spec.Containers {
			if pattern.MatchString(container.Image) {
				matched = append(matched, container.Name)
			}
		}
		if len(matched) == 1 {
			return matched[0], nil
		}
	}
	if len(pod.Spec.Containers) == 1 {
		return pod.Spec.Containers[0].Name, nil
	}
	return "", fmt.Errorf("no single container runs %s", engine)
}

// AppMap passes through a carried view's map; a detector snapshot has none.
func (v snapshotReadView) AppMap() (AppMap, error) {
	if source, ok := v.DetectionContext.(interface{ AppMap() (AppMap, error) }); ok {
		return source.AppMap()
	}
	return AppMap{}, fmt.Errorf("application map unavailable in detector context")
}

// DataChecks passes through a carried view's accepted data checks.
func (v snapshotReadView) DataChecks() (map[string]string, error) {
	if source, ok := v.DetectionContext.(datacheck.Accepted); ok {
		return source.DataChecks()
	}
	return nil, fmt.Errorf("data checks unavailable in detector context")
}

func (v *checkedReadView) AppMap() (AppMap, error) {
	m, err := v.ReadView.AppMap()
	v.check(err)
	return m, err
}

func (v *checkedReadView) DataChecks() (map[string]string, error) {
	checks, err := v.ReadView.DataChecks()
	v.check(err)
	return checks, err
}

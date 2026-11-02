package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"reflect"
	controllercore "sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sort"
	"time"
)

// scopedObjects accepts only the already projected snapshot; raw Kubernetes
// objects never enter event evidence, drift or the scoped baseline.
func scopedObjects(s sdk.ScopedSnapshot) map[string]map[string]*unstructured.Unstructured {
	out := map[string]map[string]*unstructured.Unstructured{}
	add := func(t sdk.ObservationTarget, object any) {
		b, _ := json.Marshal(object)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		o := &unstructured.Unstructured{Object: m}
		o.SetAPIVersion(t.APIVersion)
		o.SetKind(t.Kind)
		if out[t.Key()] == nil {
			out[t.Key()] = map[string]*unstructured.Unstructured{}
		}
		out[t.Key()][o.GetName()] = o
	}
	for _, status := range s.ObservationStatuses() {
		if status.State != sdk.ObservationObserved {
			continue
		}
		t := status.Target
		out[t.Key()] = map[string]*unstructured.Unstructured{}
		switch t.Kind {
		case "Deployment":
			objects, _ := s.ScopedDeployments(t.Namespace)
			for _, o := range objects {
				add(t, o)
			}
		case "Service":
			objects, _ := s.ScopedServices(t.Namespace)
			for _, o := range objects {
				add(t, o)
			}
		case "Endpoints":
			objects, _ := s.ScopedEndpoints(t.Namespace)
			for _, o := range objects {
				add(t, o)
			}
		case "MutatingWebhookConfiguration":
			for _, o := range s.Mutating {
				if o.Name == t.Name {
					add(t, sdk.ProjectMutatingWebhook(o))
				}
			}
		case "ValidatingWebhookConfiguration":
			for _, o := range s.Validating {
				if o.Name == t.Name {
					add(t, sdk.ProjectValidatingWebhook(o))
				}
			}
		}
	}
	return out
}
func (c *KubernetesCache) observeScoped(s sdk.ScopedSnapshot) {
	objects := scopedObjects(s)
	c.mu.Lock()
	if c.scopedPrevious == nil {
		c.scopedPrevious = map[string]map[string]*unstructured.Unstructured{}
		c.scopedStatuses = map[string]string{}
	}
	var events []sdk.WatchKind
	for _, status := range s.ObservationStatuses() {
		t := status.Target
		key := t.Key()
		w := sdk.WatchKind{APIVersion: t.APIVersion, Kind: t.Kind, Namespace: t.Namespace}
		if c.scopedStatuses[key] != status.State {
			events = append(events, w)
		}
		c.scopedStatuses[key] = status.State
		if status.State != sdk.ObservationObserved {
			continue
		}
		previous, initialized := c.scopedPrevious[key]
		current := objects[key]
		names := map[string]bool{}
		for n := range previous {
			names[n] = true
		}
		for n := range current {
			names[n] = true
		}
		ordered := []string{}
		for n := range names {
			ordered = append(ordered, n)
		}
		sort.Strings(ordered)
		for _, n := range ordered {
			old, newObject := previous[n], current[n]
			if reflect.DeepEqual(old, newObject) {
				continue
			}
			events = append(events, w)
			if !c.config.TrackDrift || t.Kind == "Endpoints" {
				continue
			}
			identity := newObject
			if identity == nil {
				identity = old
			}
			eventKey := identity.GetAPIVersion() + "/" + identity.GetKind() + "/" + identity.GetNamespace() + "/" + identity.GetName()
			if prior, ok := c.changes[eventKey]; ok {
				old = prior.Old
			} else {
				c.changeOrder = append(c.changeOrder, eventKey)
			}
			c.changes[eventKey] = ObjectChange{Old: old, New: newObject, Initial: !initialized}
		}
		c.scopedPrevious[key] = current
	}
	c.mu.Unlock()
	if c.scopedTracker != nil {
		c.scopedTracker.UpdateScoped(s)
	}
	for _, w := range events {
		c.enqueue(w)
	}
}

func scopedState(object *unstructured.Unstructured) objectState {
	state := objectState{}
	fields := flattenDriftObject(object)
	if webhooks, ok := object.Object["webhooks"]; ok {
		fields["webhooks"] = webhooks
	}
	if subsets, ok := object.Object["subsets"]; ok {
		fields["subsets"] = subsets
	}
	for field, value := range fields {
		b, _ := json.Marshal(value)
		if len(b) > maxRenderedValue {
			h := sha256.Sum256(b)
			state[field] = fmt.Sprintf("sha256:%x", h)
		} else {
			state[field] = string(b)
		}
	}
	return state
}

// RetryObjectChange retains the oldest pre-image while allowing newer observed
// state to coalesce; ordinary scheduling supplies the bounded retry cadence.
func (c *KubernetesCache) RetryObjectChange(change ObjectChange) {
	object := change.New
	if object == nil {
		object = change.Old
	}
	if object == nil {
		return
	}
	key := object.GetAPIVersion() + "/" + object.GetKind() + "/" + object.GetNamespace() + "/" + object.GetName()
	c.mu.Lock()
	defer c.mu.Unlock()
	if queued, ok := c.changes[key]; ok {
		queued.Old = change.Old
		c.changes[key] = queued
	} else {
		c.changes[key] = change
		c.changeOrder = append(c.changeOrder, key)
	}
}

func (c *KubernetesCache) reconcileScope(scope sdk.ObservationScope, revision string) {
	if c.config.Scope.Identity() == scope.Identity() && c.config.Generation == revision {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	allowed := map[string]bool{}
	for _, t := range scope.Targets {
		allowed[t.Key()] = true
	}
	for key := range c.scopedPrevious {
		if !allowed[key] {
			delete(c.scopedPrevious, key)
			delete(c.scopedStatuses, key)
			delete(c.scopedRetries, key)
			delete(c.scopedLastStatus, key)
		}
	}
	order := []string{}
	for _, key := range c.changeOrder {
		change := c.changes[key]
		o := change.New
		if o == nil {
			o = change.Old
		}
		if o != nil && o.GetNamespace() != c.config.Namespace {
			t := sdk.ObservationTarget{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace()}
			if t.Cluster() {
				t.Name = o.GetName()
			}
			if !allowed[t.Key()] {
				delete(c.changes, key)
				continue
			}
		}
		order = append(order, key)
	}
	c.changeOrder = order
	c.config.Scope = scope
	c.config.Generation = revision
}

type scopedRetry struct {
	attempts int
	next     time.Time
}

// readScoped retries unavailable targets with capped per-target backoff. It
// never restores availability without a fresh successful read.
func (c *KubernetesCache) readScoped(ctx context.Context) (sdk.ScopedSnapshot, error) {
	now := time.Now()
	if c.config.Now != nil {
		now = c.config.Now()
	}
	if c.scopedRetries == nil {
		c.scopedRetries = map[string]scopedRetry{}
		c.scopedLastStatus = map[string]sdk.ObservationStatus{}
	}
	scope := sdk.ObservationScope{Version: c.config.Scope.Version}
	skipped := []sdk.ObservationStatus{}
	for _, target := range c.config.Scope.Targets {
		retry := c.scopedRetries[target.Key()]
		if now.Before(retry.next) {
			skipped = append(skipped, c.scopedLastStatus[target.Key()])
			continue
		}
		scope.Targets = append(scope.Targets, target)
	}
	result, err := controllercore.ObserveScoped(ctx, c.config.Client, scope, c.config.Ceiling, c.config.Generation)
	if err != nil {
		return result, err
	}
	for _, status := range result.Statuses {
		key := status.Target.Key()
		c.scopedLastStatus[key] = status
		if status.State == sdk.ObservationObserved {
			delete(c.scopedRetries, key)
		} else {
			retry := c.scopedRetries[key]
			if retry.attempts < 5 {
				retry.attempts++
			}
			delay := time.Duration(1<<retry.attempts) * time.Second
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			retry.next = now.Add(delay)
			c.scopedRetries[key] = retry
		}
	}
	result.Scope = c.config.Scope
	result.Scope.Targets = append([]sdk.ObservationTarget(nil), c.config.Scope.Targets...)
	sort.Slice(result.Scope.Targets, func(i, j int) bool { return result.Scope.Targets[i].Key() < result.Scope.Targets[j].Key() })
	result.Generation = c.config.Generation + ":" + c.config.Ceiling.Identity() + ":" + c.config.Scope.Identity()
	result.Statuses = append(result.Statuses, skipped...)
	sort.Slice(result.Statuses, func(i, j int) bool { return result.Statuses[i].Target.Key() < result.Statuses[j].Target.Key() })
	return result, nil
}

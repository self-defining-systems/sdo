package install

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// StopResult describes runtime objects only. Stop never deletes repository PVCs,
// ConfigMaps, Secrets, namespaces, or accepted Git state.
type StopResult struct {
	Removed   []string `json:"removed"`
	Leftovers []string `json:"leftovers"`
}

type stopObject struct {
	name       string
	controller bool
	remove     func(context.Context) error
}

func runtimeLabels(labels map[string]string) bool {
	name := labels["app.kubernetes.io/name"]
	return slices.Contains([]string{"sdo-controller", "sdo-operate", "sdo-worker", "sdo-prober"}, name) ||
		labels["sdo.dev/validator-isolation"] != "" || labels["sdo.dev/validator-canary"] != "" ||
		labels["sdo.dev/component"] == "image-build"
}

func runtimeInventory(ctx context.Context, client kubernetes.Interface, namespace string) ([]stopObject, error) {
	if client == nil {
		return nil, errors.New("Kubernetes client is required")
	}
	if namespace != "" && !namespaceName.MatchString(namespace) {
		return nil, fmt.Errorf("invalid control namespace %q", namespace)
	}
	var result []stopObject
	options := func(uid types.UID) metav1.DeleteOptions {
		zero := int64(0)
		propagation := metav1.DeletePropagationForeground
		opts := metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &propagation}
		if uid != "" {
			opts.Preconditions = &metav1.Preconditions{UID: &uid}
		}
		return opts
	}
	jobs, err := client.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("inventory SDO Jobs: %w", err)
	}
	ownedJobs := map[string]bool{}
	for _, j := range jobs.Items {
		control := j.Name == ControllerJobName || strings.HasPrefix(j.Name, "sdo-operate-") && j.Spec.Template.Spec.ServiceAccountName == "sdo-operate"
		if !control && !runtimeLabels(j.Labels) && !runtimeLabels(j.Spec.Template.Labels) {
			continue
		}
		ownedJobs[j.Namespace+"/"+j.Name] = true
		result = append(result, stopObject{"job/" + j.Namespace + "/" + j.Name, control, func(ctx context.Context) error {
			return client.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, options(j.UID))
		}})
	}
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("inventory SDO Pods: %w", err)
	}
	for _, p := range pods.Items {
		control := p.Labels["app.kubernetes.io/name"] == "sdo-controller" || p.Labels["app.kubernetes.io/name"] == "sdo-operate" || p.Labels["job-name"] == ControllerJobName || p.Labels["batch.kubernetes.io/job-name"] == ControllerJobName
		owned := control || runtimeLabels(p.Labels) || p.Name == RepositorySyncPodName
		for _, owner := range p.OwnerReferences {
			if owner.Kind == "Job" && ownedJobs[p.Namespace+"/"+owner.Name] {
				owned = true
			}
		}
		if !owned {
			continue
		}
		result = append(result, stopObject{"pod/" + p.Namespace + "/" + p.Name, control, func(ctx context.Context) error {
			return client.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, options(p.UID))
		}})
	}
	services, err := client.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("inventory SDO Services: %w", err)
	}
	for _, s := range services.Items {
		if !runtimeLabels(s.Labels) {
			continue
		}
		result = append(result, stopObject{"service/" + s.Namespace + "/" + s.Name, false, func(ctx context.Context) error {
			return client.CoreV1().Services(s.Namespace).Delete(ctx, s.Name, options(s.UID))
		}})
	}
	policies, err := client.NetworkingV1().NetworkPolicies(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("inventory SDO NetworkPolicies: %w", err)
	}
	for _, p := range policies.Items {
		if !runtimeLabels(p.Labels) {
			continue
		}
		result = append(result, stopObject{"networkpolicy/" + p.Namespace + "/" + p.Name, false, func(ctx context.Context) error {
			return client.NetworkingV1().NetworkPolicies(p.Namespace).Delete(ctx, p.Name, options(p.UID))
		}})
	}
	return result, nil
}

// InspectRuntime lists owned runtime objects without depending on a live controller.
// An empty namespace inspects the cluster; callers can scope it to a control namespace.
func InspectRuntime(ctx context.Context, client kubernetes.Interface, namespace string) (StopResult, error) {
	result := StopResult{Removed: []string{}, Leftovers: []string{}}
	objects, err := runtimeInventory(ctx, client, namespace)
	for _, obj := range objects {
		result.Leftovers = append(result.Leftovers, obj.name)
	}
	slices.Sort(result.Leftovers)
	return result, err
}

// Stop first quiesces dispatchers, then removes workers and their sidecars.
// It polls actual deletion, including foreground garbage collection, so success
// means no owned runtime remains. A caller deadline bounds stuck finalizers.
func Stop(ctx context.Context, client kubernetes.Interface, namespace string) (StopResult, error) {
	result := StopResult{Removed: []string{}, Leftovers: []string{}}
	removed := map[string]bool{}
	for _, controllersOnly := range []bool{true, false} {
		for {
			objects, err := runtimeInventory(ctx, client, namespace)
			if err != nil {
				return result, err
			}
			result.Leftovers = nil
			active := false
			var failures []error
			for _, obj := range objects {
				result.Leftovers = append(result.Leftovers, obj.name)
				if controllersOnly && !obj.controller {
					continue
				}
				active = true
				if err := obj.remove(ctx); err != nil && !apierrors.IsNotFound(err) {
					failures = append(failures, fmt.Errorf("remove %s: %w", obj.name, err))
					continue
				}
				if !removed[obj.name] {
					removed[obj.name] = true
					result.Removed = append(result.Removed, obj.name)
				}
			}
			if len(failures) > 0 {
				return result, errors.Join(failures...)
			}
			if !active {
				break
			}
			select {
			case <-ctx.Done():
				return result, fmt.Errorf("SDO runtime did not stop; leftovers %v: %w", result.Leftovers, ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	result.Leftovers = []string{}
	slices.Sort(result.Removed)
	return result, nil
}

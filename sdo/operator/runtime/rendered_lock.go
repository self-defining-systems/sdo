package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/sdk"
)

// RenderedLockReconciler observes a completed deployment before proposing a lock.
// A current lock is carried forward without consulting live incident state.
type RenderedLockReconciler struct {
	Source GitPlanSource
	Client kubernetes.Interface
	Broker *broker.Broker
}

func (r RenderedLockReconciler) Reconcile(ctx context.Context, revision, namespace string) (string, error) {
	if r.Broker != nil {
		head, err := r.Broker.Head(ctx)
		if err != nil {
			return revision, err
		}
		revision = head
	}
	if r.Source.LockCurrent(ctx, revision, namespace) {
		return revision, nil
	}
	if r.Broker == nil || r.Client == nil {
		return revision, errors.New("rendered lock requires controller broker and Kubernetes observation")
	}
	objects, err := r.Source.RawManifests(ctx, revision, namespace)
	if err != nil {
		return revision, err
	}
	// Reuse FX1's image evidence store, independently of the live diff baseline.
	evidence := NewStateTracker(StateTrackerConfig{Namespace: namespace})
	digests := NewStateTracker(StateTrackerConfig{Namespace: namespace})
	gaps := map[sdk.ObjectRef]string{}
	for _, object := range objects {
		ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: namespace, Name: object.GetName()}
		var live any
		ready := false
		var selector *metav1.LabelSelector
		switch ref.Kind {
		case "Deployment":
			o, e := r.Client.AppsV1().Deployments(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if e != nil {
				return revision, e
			}
			wanted := int32(1)
			if o.Spec.Replicas != nil {
				wanted = *o.Spec.Replicas
			}
			ready = o.Status.ObservedGeneration >= o.Generation && o.Status.UpdatedReplicas == wanted && o.Status.AvailableReplicas >= wanted && o.Status.ReadyReplicas >= wanted
			live = o
			selector = o.Spec.Selector
		case "StatefulSet":
			o, e := r.Client.AppsV1().StatefulSets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if e != nil {
				return revision, e
			}
			wanted := int32(1)
			if o.Spec.Replicas != nil {
				wanted = *o.Spec.Replicas
			}
			ready = o.Status.ObservedGeneration >= o.Generation && o.Status.ReadyReplicas == wanted && o.Status.UpdatedReplicas == wanted && o.Status.CurrentRevision == o.Status.UpdateRevision
			live = o
			selector = o.Spec.Selector
		case "DaemonSet":
			o, e := r.Client.AppsV1().DaemonSets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if e != nil {
				return revision, e
			}
			ready = o.Status.ObservedGeneration >= o.Generation && o.Status.NumberReady == o.Status.DesiredNumberScheduled && o.Status.UpdatedNumberScheduled == o.Status.DesiredNumberScheduled
			live = o
			selector = o.Spec.Selector
		case "Job":
			o, e := r.Client.BatchV1().Jobs(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if e != nil {
				return revision, e
			}
			ready = o.Status.Succeeded > 0
			live = o
			selector = o.Spec.Selector
		default:
			continue
		}
		if !ready {
			return revision, fmt.Errorf("rendered lock: %s/%s rollout is incomplete", ref.Kind, ref.Name)
		}
		doc, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(live)
		if err != nil {
			return revision, err
		}
		tree, _, _ := unstructured.NestedString(doc, "spec", "template", "metadata", "annotations", "sdo.dev/source-tree")
		deployedIdentity := tree
		if deployedIdentity == "" {
			deployedIdentity = r.Source.DeployedCommit
		}
		same, identityErr := r.Source.SameInputs(ctx, deployedIdentity, revision)
		if identityErr != nil || !same {
			return revision, fmt.Errorf("rendered lock: %s/%s rollout has no matching accepted deployment identity: %v", ref.Kind, ref.Name, identityErr)
		}
		// Each container is keyed by name and role, as in FX1's image store.
		for _, field := range []string{"initContainers", "containers"} {
			containers, _, _ := unstructured.NestedSlice(doc, "spec", "template", "spec", field)
			for _, raw := range containers {
				c := raw.(map[string]any)
				name, _ := c["name"].(string)
				image, _ := c["image"].(string)
				imageReference := image
				if !strings.Contains(image, "@sha256:") {
					image, err = r.podDigest(ctx, namespace, selector, tree, name, field == "initContainers", image)
					if err != nil {
						gaps[ref] = err.Error()
						continue
					}
				}
				evidence.recordDeployedImage(ref, name, field == "initContainers", imageReference, tree)
				digests.recordDeployedImage(ref, name, field == "initContainers", image, tree)
			}
		}
	}
	source := r.Source
	source.Images = evidence
	source.Digests = digests
	source.Gaps = gaps
	snapshot, err := source.BuildLock(ctx, revision, namespace)
	if err != nil {
		return revision, err
	}
	// Rollout is complete; per-object rendering gaps remain explicit entries.
	return r.Broker.RecordRenderedLock(ctx, revision, snapshot)
}

func (r RenderedLockReconciler) podDigest(ctx context.Context, namespace string, selector *metav1.LabelSelector, tree, name string, init bool, source string) (string, error) {
	if selector == nil {
		return "", errors.New("rendered lock: mutable image needs a workload selector")
	}
	labels, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return "", err
	}
	pods, err := r.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.String()})
	if err != nil {
		return "", err
	}
	digest := ""
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp != nil || pod.Annotations["sdo.dev/source-tree"] != tree {
			continue
		}
		specs := pod.Spec.Containers
		statuses := pod.Status.ContainerStatuses
		if init {
			specs = pod.Spec.InitContainers
			statuses = pod.Status.InitContainerStatuses
		}
		matches := false
		for _, c := range specs {
			if c.Name == name && c.Image == source {
				matches = true
			}
		}
		if !matches {
			continue
		}
		for _, status := range statuses {
			if status.Name != name {
				continue
			}
			image := strings.TrimPrefix(strings.TrimPrefix(status.ImageID, "docker-pullable://"), "containerd://")
			if strings.HasPrefix(image, "sha256:") {
				repository := strings.Split(source, "@")[0]
				if i := strings.LastIndex(repository, ":"); i > strings.LastIndex(repository, "/") {
					repository = repository[:i]
				}
				image = repository + "@" + image
			}
			if !strings.Contains(image, "@sha256:") {
				continue
			}
			if digest != "" && digest != image {
				return "", fmt.Errorf("rendered lock: container %s has inconsistent deployed digests", name)
			}
			digest = image
		}
	}
	if digest == "" {
		return "", fmt.Errorf("rendered lock: deployed digest missing for container %s (init=%t)", name, init)
	}
	return digest, nil
}

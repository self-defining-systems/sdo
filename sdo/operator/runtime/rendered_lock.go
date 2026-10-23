package runtime

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/rendered"
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
	proofs, err := r.Broker.RenderedDeploymentEvidence(ctx, revision)
	if err != nil {
		return revision, err
	}
	for _, object := range objects {
		ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: namespace, Name: object.GetName()}
		var live any
		ready := false
		switch ref.Kind {
		case "Deployment":
			o, e := r.Client.AppsV1().Deployments(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if e != nil {
				return revision, e
			}
			ready = deployment.DeploymentRolloutComplete(o)
			live = o
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
		case "DaemonSet":
			o, e := r.Client.AppsV1().DaemonSets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if e != nil {
				return revision, e
			}
			ready = o.Status.ObservedGeneration >= o.Generation && o.Status.NumberReady == o.Status.DesiredNumberScheduled && o.Status.UpdatedNumberScheduled == o.Status.DesiredNumberScheduled
			live = o
		case "Job":
			o, e := r.Client.BatchV1().Jobs(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if e != nil {
				return revision, e
			}
			ready = deployment.JobRolloutComplete(o)
			live = o
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
		same, identityErr := r.Source.SameWorkloadInputs(ctx, deployedIdentity, revision, object, proofs)
		if identityErr != nil || !same {
			// Accepted source repairs can invalidate an existing workload's
			// deployment evidence. Keep that loss explicit and object-scoped:
			// unrelated Services, ConfigMaps and confirmed absences still need
			// a current lock. Never authorize its images from this live read.
			gaps[ref] = "rollout source inputs differ from accepted source"
			if identityErr != nil {
				gaps[ref] = fmt.Sprintf("rollout deployment identity unavailable: %v", identityErr)
			}
			continue
		}
		// Preserve source pins. Other identities come only from the deployer's
		// durable named evidence, never from this observation or matching Pods.
		for _, field := range []string{"initContainers", "containers"} {
			containers, _, _ := unstructured.NestedSlice(object.Object, "spec", "template", "spec", field)
			for _, raw := range containers {
				c := raw.(map[string]any)
				name, _ := c["name"].(string)
				applied, _ := c["image"].(string)
				digest := applied
				init := field == "initContainers"
				if !rendered.IsImmutableImage(applied) {
					found := false
					for _, proof := range proofs {
						if image, ok := proof.Image(ref, name, init); ok {
							if found && (applied != image.Applied || digest != image.Digest) {
								gaps[ref] = "conflicting named deployment image evidence"
								continue
							}
							applied, digest, found = image.Applied, image.Digest, true
						}
					}
					if !found {
						gaps[ref] = fmt.Sprintf("trusted deployment image evidence missing for container %s (init=%t)", name, init)
						continue
					}
				}
				evidence.recordDeployedImage(ref, name, init, applied, tree)
				digests.recordDeployedImage(ref, name, init, digest, tree)
			}
		}
	}
	source := r.Source
	source.Evidence = proofs
	source.Images = evidence
	source.Digests = digests
	source.Gaps = gaps
	snapshot, err := source.BuildLock(ctx, revision, namespace)
	if err != nil {
		return revision, err
	}
	// Rollout is complete; per-object rendering gaps remain explicit entries.
	commit, err := r.Broker.RecordRenderedLock(ctx, revision, snapshot)
	if err != nil {
		// Publication may have succeeded with a lost reply, or another broker
		// transaction advanced HEAD. Recover only the broker-verified head.
		head, headErr := r.Broker.Head(ctx)
		if headErr != nil {
			return revision, errors.Join(err, headErr)
		}
		if head != "" {
			revision = head
		}
		return revision, err
	}
	if commit == "" {
		return revision, errors.New("rendered lock publication returned an empty accepted revision")
	}
	return commit, nil
}

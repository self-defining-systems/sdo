package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
)

// KubernetesPlanClient applies plan patches through the Kubernetes dynamic client.
type KubernetesPlanClient struct {
	Client dynamic.Interface
	Mapper meta.RESTMapper
}

func (c KubernetesPlanClient) resource(ref sdk.ObjectRef) (dynamic.ResourceInterface, error) {
	if c.Client == nil || c.Mapper == nil {
		return nil, errors.New("Kubernetes plan client and REST mapper are required")
	}
	gvk := schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)
	mapping, err := c.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, fmt.Errorf("map %s/%s: %w", ref.Kind, ref.Name, err)
	}
	if mapping.Scope.Name() == meta.RESTScopeNameRoot && ref.Namespace == "" && ref.APIVersion == "admissionregistration.k8s.io/v1" && (ref.Kind == "MutatingWebhookConfiguration" || ref.Kind == "ValidatingWebhookConfiguration") {
		return c.Client.Resource(mapping.Resource), nil
	}
	if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
		return nil, fmt.Errorf("plan object %s/%s is not namespaced", ref.Kind, ref.Name)
	}
	return c.Client.Resource(mapping.Resource).Namespace(ref.Namespace), nil
}

func (c KubernetesPlanClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	resource, err := c.resource(ref)
	if err != nil {
		return nil, err
	}
	return resource.Get(ctx, ref.Name, metav1.GetOptions{})
}

func (c KubernetesPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	resource, err := c.resource(ref)
	if err != nil {
		return err
	}
	_, err = resource.Patch(ctx, ref.Name, kind, patch, metav1.PatchOptions{})
	return err
}

// KubernetesPlanRollouts waits for touched Deployments to finish rolling out.
type KubernetesPlanRollouts struct {
	Client kubernetes.Interface
}

func (w KubernetesPlanRollouts) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	if w.Client == nil {
		return errors.New("Kubernetes rollout client is required")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		settled := true
		for _, ref := range refs {
			if ref.Kind != "Deployment" || ref.APIVersion != "apps/v1" {
				continue
			}
			deploymentObject, err := w.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("observe rollout %s/%s: %w", ref.Namespace, ref.Name, err)
			}
			if !deployment.DeploymentRolloutComplete(deploymentObject) {
				settled = false
			}
		}
		if settled {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c KubernetesPlanClient) Create(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if object == nil || !samePlanRef(object, ref) || ref.APIVersion != "v1" || (ref.Kind != "ConfigMap" && ref.Kind != "Service") || ref.Namespace == "" {
		return nil, fmt.Errorf("object create kind/scope is unsupported")
	}
	resource, err := c.resource(ref)
	if err != nil {
		return nil, err
	}
	return resource.Create(ctx, object, metav1.CreateOptions{})
}
func (c KubernetesPlanClient) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, version string) error {
	if uid == "" || version == "" {
		return fmt.Errorf("delete requires UID/version preconditions")
	}
	allowed := ref.Namespace != "" && ((ref.APIVersion == "v1" && (ref.Kind == "ConfigMap" || ref.Kind == "Service" || ref.Kind == "Pod")) || (ref.APIVersion == "networking.k8s.io/v1" && ref.Kind == "NetworkPolicy"))
	allowed = allowed || (ref.Namespace == "" && ref.APIVersion == "admissionregistration.k8s.io/v1" && (ref.Kind == "MutatingWebhookConfiguration" || ref.Kind == "ValidatingWebhookConfiguration"))
	if !allowed {
		return fmt.Errorf("object delete kind/scope is unsupported")
	}
	resource, err := c.resource(ref)
	if err != nil {
		return err
	}
	return resource.Delete(ctx, ref.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}})
}

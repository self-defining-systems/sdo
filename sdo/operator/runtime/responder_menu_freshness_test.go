package runtime

import (
	"context"
	"encoding/json"

	jsonpatch "github.com/evanphx/json-patch"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// client-go v0.30 drops PatchOptions when building fake actions. Preserve the
// real API's dry-run boundary before that information is lost to the tracker.
type productionMenuAPI struct{ acceptedSourceBridge }

func (c *productionMenuAPI) CoreV1() coreclient.CoreV1Interface {
	return productionMenuCore{c.Clientset.CoreV1()}
}

type productionMenuCore struct{ coreclient.CoreV1Interface }

func (c productionMenuCore) Services(namespace string) coreclient.ServiceInterface {
	return productionMenuServices{c.CoreV1Interface.Services(namespace)}
}

type productionMenuServices struct{ coreclient.ServiceInterface }

func (c productionMenuServices) Patch(ctx context.Context, name string, kind types.PatchType, patch []byte, options metav1.PatchOptions, subresources ...string) (*corev1.Service, error) {
	if len(options.DryRun) == 0 {
		return c.ServiceInterface.Patch(ctx, name, kind, patch, options, subresources...)
	}
	current, err := c.ServiceInterface.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	before, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	after, err := jsonpatch.MergePatch(before, patch)
	if err != nil {
		return nil, err
	}
	projected := new(corev1.Service)
	if err := json.Unmarshal(after, projected); err != nil {
		return nil, err
	}
	return projected, nil
}

package runtime

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/deployment"
)

type admissionLiveVerifier struct {
	client          kubernetes.Interface
	namespace, name string
}

func (v admissionLiveVerifier) Verify(ctx context.Context) (bool, error) {
	object, err := v.client.AppsV1().Deployments(v.namespace).Get(ctx, v.name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return deployment.DeploymentRolloutComplete(object), nil
}

package runtime

import (
	"context"
	"encoding/json"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk/program"
)

type clientGoServiceView struct {
	program.ReadView
	client kubernetes.Interface
}

func (v clientGoServiceView) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	parts := strings.Split(q.Path, "/")
	object, err := v.client.CoreV1().Services(parts[4]).Get(context.Background(), parts[6], metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return json.Marshal(object)
}


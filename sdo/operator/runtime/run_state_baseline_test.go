package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"strings"
	"sync"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
)

// capturingDispatcher records the incident request the controller dispatches.
type capturingDispatcher struct{ requests chan IncidentRequest }

func (d capturingDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	return completedResult(request.IncidentID), nil
}

type lockedLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *lockedLines) has(prefix string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func (l *lockedLines) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func collectLines(reader io.Reader, into *lockedLines) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		into.mu.Lock()
		into.lines = append(into.lines, scanner.Text())
		into.mu.Unlock()
	}
}

func readyFrontend() []corev1.Pod {
	return []corev1.Pod{{
		ObjectMeta: metav1.ObjectMeta{
			Name: "frontend-7d9", Namespace: hotel, Labels: map[string]string{"io.kompose.service": "frontend"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}}
}

func frontendEndpoints(ready int) *corev1.Endpoints {
	endpoints := &corev1.Endpoints{ObjectMeta: metav1.ObjectMeta{Name: "frontend", Namespace: hotel}}
	if ready > 0 {
		endpoints.Subsets = []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{IP: "10.0.0.7"}},
			Ports:     []corev1.EndpointPort{{Name: "5000", Port: 5000}},
		}}
	}
	return endpoints
}

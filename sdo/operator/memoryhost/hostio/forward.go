package hostio

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sdo.dev/operator/sdk/traffic"
)

// Forwards reaches in-cluster Services from outside the cluster: each target
// gets one port forward to a ready Pod behind its Service.
type Forwards struct {
	Namespace string
	Client    kubernetes.Interface
	Config    *rest.Config

	mu    sync.Mutex
	ports map[string]*forward
}

type forward struct {
	port uint16
	done chan struct{}
}

// BaseURL addresses target through a local port; a target that cannot be
// forwarded gets an address that refuses, so its sample says the dial failed.
func (f *Forwards) BaseURL(target traffic.Target) string {
	scheme := target.Scheme
	if scheme == "" {
		scheme = "http"
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fmt.Sprintf("%s:%d", target.Service, target.Port)
	if open := f.ports[key]; open != nil {
		select {
		case <-open.done:
		default:
			return fmt.Sprintf("%s://127.0.0.1:%d", scheme, open.port)
		}
	}
	open, err := f.open(target)
	if err != nil {
		return scheme + "://127.0.0.1:1"
	}
	if f.ports == nil {
		f.ports = map[string]*forward{}
	}
	f.ports[key] = open
	return fmt.Sprintf("%s://127.0.0.1:%d", scheme, open.port)
}

func (f *Forwards) open(target traffic.Target) (*forward, error) {
	ctx := context.Background()
	service, err := f.Client.CoreV1().Services(f.Namespace).Get(ctx, target.Service, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	pods, err := f.Client.CoreV1().Pods(f.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String()})
	if err != nil {
		return nil, err
	}
	for _, pod := range pods.Items {
		port, ok := targetPort(*service, target.Port, pod)
		if !ok || !podReady(pod) {
			continue
		}
		return f.dial(pod.Name, port)
	}
	return nil, fmt.Errorf("no ready Pod behind Service %s port %d", target.Service, target.Port)
}

func (f *Forwards) dial(pod string, port int32) (*forward, error) {
	transport, upgrader, err := spdy.RoundTripperFor(f.Config)
	if err != nil {
		return nil, err
	}
	url := f.Client.CoreV1().RESTClient().Post().Resource("pods").Namespace(f.Namespace).Name(pod).SubResource("portforward").URL()
	ready, stop := make(chan struct{}), make(chan struct{})
	forwarder, err := portforward.New(spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url), []string{fmt.Sprintf("0:%d", port)}, stop, ready, io.Discard, io.Discard)
	if err != nil {
		return nil, err
	}
	open := &forward{done: make(chan struct{})}
	failed := make(chan error, 1)
	go func() {
		failed <- forwarder.ForwardPorts()
		close(open.done)
	}()
	select {
	case err := <-failed:
		return nil, fmt.Errorf("forward to Pod %s: %w", pod, err)
	case <-ready:
	}
	bound, err := forwarder.GetPorts()
	if err != nil {
		return nil, err
	}
	open.port = bound[0].Local
	return open, nil
}

func targetPort(service corev1.Service, port int, pod corev1.Pod) (int32, bool) {
	for _, p := range service.Spec.Ports {
		if int(p.Port) != port {
			continue
		}
		if name := p.TargetPort.StrVal; name != "" {
			for _, c := range pod.Spec.Containers {
				for _, cp := range c.Ports {
					if cp.Name == name {
						return cp.ContainerPort, true
					}
				}
			}
			return 0, false
		}
		if p.TargetPort.IntVal != 0 {
			return p.TargetPort.IntVal, true
		}
		return p.Port, true
	}
	return 0, false
}

func podReady(pod corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Pause waits for d or until ctx ends.
func Pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// RunProgram runs a built program with input on stdin, against the cluster
// kubeconfig names (the environment's when empty).
func RunProgram(ctx context.Context, binary, kubeconfig string, in io.Reader, out, errOut io.Writer) error {
	command := exec.CommandContext(ctx, binary)
	command.Stdin, command.Stdout, command.Stderr = in, out, errOut
	command.Env = os.Environ()
	if kubeconfig != "" {
		command.Env = append(command.Env, "KUBECONFIG="+kubeconfig)
	}
	return command.Run()
}

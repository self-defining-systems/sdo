package operatorcli

import (
	"context"
	"io"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
)

type podExecutor func(context.Context, string, string, []string, io.Writer, io.Writer) error

// containerExecutor runs in a named container; "" selects the Pod's default.
type containerExecutor func(ctx context.Context, namespace, pod, container string, command []string, stdout, stderr io.Writer) error

func inContainer(execute containerExecutor, container string) podExecutor {
	return func(ctx context.Context, namespace, pod string, command []string, stdout, stderr io.Writer) error {
		return execute(ctx, namespace, pod, container, command, stdout, stderr)
	}
}

func newPodExecutor(config *rest.Config, client kubernetes.Interface) (podExecutor, error) {
	execute, err := newContainerExecutor(config, client)
	if err != nil {
		return nil, err
	}
	return inContainer(execute, ""), nil
}

// The upgrade transport is built once beside the ordinary client. Each SPDY
// exec requires its own upgraded stream; ordinary API calls retain their pool.
func newContainerExecutor(config *rest.Config, client kubernetes.Interface) (containerExecutor, error) {
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}
	// client-go's SPDY upgrader stores the current connection internally.
	// Serialize this evidence executor rather than sharing that mutable slot.
	active := make(chan struct{}, 1)
	return func(ctx context.Context, namespace, pod, container string, command []string, stdout, stderr io.Writer) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		select {
		case active <- struct{}{}:
			defer func() { <-active }()
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		target := client.CoreV1().RESTClient().Post().Namespace(namespace).Resource("pods").Name(pod).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: stdout != nil, Stderr: stderr != nil}, scheme.ParameterCodec).URL()
		executor, err := remotecommand.NewSPDYExecutorForTransports(transport, upgrader, http.MethodPost, target)
		if err != nil {
			return err
		}
		return executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: stdout, Stderr: stderr})
	}, nil
}

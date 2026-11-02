package kubereads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	spdytransport "k8s.io/client-go/transport/spdy"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// ErrExecUnavailable means no exec request crossed the API boundary.
var ErrExecUnavailable = errors.New("exec transport unavailable")

// PodExec streams one command in a container. A nonzero exit is reported as a
// utilexec.ExitError. It is the only exec capability in this package; the
// readers run only the fixed read commands they construct.
type PodExec interface {
	Stream(ctx context.Context, pod sdk.ObjectRef, container string, command []string, stdin string, stdout io.Writer) error
}

// SPDYExec executes through the Kubernetes pod exec subresource.
type SPDYExec struct {
	Client kubernetes.Interface
	Config *rest.Config
}

func (e SPDYExec) Stream(ctx context.Context, pod sdk.ObjectRef, container string, command []string, stdin string, stdout io.Writer) error {
	if e.Client == nil || e.Config == nil || e.Client.CoreV1().RESTClient() == nil {
		return ErrExecUnavailable
	}
	request := e.Client.CoreV1().RESTClient().Post().Resource("pods").Name(pod.Name).Namespace(pod.Namespace).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdin: true, Stdout: stdout != nil, Stderr: false, TTY: false}, scheme.ParameterCodec)
	transport, upgrader, err := spdytransport.RoundTripperFor(rest.CopyConfig(e.Config))
	if err != nil {
		return ErrExecUnavailable
	}
	executor, err := remotecommand.NewSPDYExecutorRejectRedirects(transport, upgrader, "POST", request.URL())
	if err != nil {
		return ErrExecUnavailable
	}
	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: strings.NewReader(stdin), Stdout: stdout})
}

// GetPod reads one Pod as an unstructured object, refusing the control namespace.
func GetPod(ctx context.Context, client kubernetes.Interface, protectedNamespace string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if protectedNamespace != "" && ref.Namespace == protectedNamespace {
		return nil, apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, ref.Namespace, fmt.Errorf("generic plans cannot access the SDO control namespace"))
	}
	if client == nil || ref.APIVersion+"/"+ref.Kind != "v1/Pod" {
		return nil, fmt.Errorf("Kubernetes Pod reader is required")
	}
	pod, err := client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		return nil, err
	}
	object := &unstructured.Unstructured{Object: fields}
	object.SetAPIVersion(ref.APIVersion)
	object.SetKind(ref.Kind)
	return object, nil
}

// DatabaseReader lists nonsecret catalog users. It never creates or updates one.
type DatabaseReader struct {
	Get                func(context.Context, sdk.ObjectRef) (*unstructured.Unstructured, error)
	Exec               PodExec
	Client             kubernetes.Interface
	ProtectedNamespace string
}

func (r DatabaseReader) catalog(ctx context.Context, t program.DatabaseTarget) ([]byte, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if r.Exec == nil || r.Get == nil {
		return nil, DatabaseOperationFailure(DatabaseExecTransport, nil)
	}
	object, err := r.Get(ctx, t.Pod)
	if err != nil || object == nil || object.GetUID() == "" {
		return nil, DatabaseOperationFailure(DatabaseContainerLookup, err)
	}
	if err := ValidateDatabaseContainer(object, t.Container); err != nil {
		return nil, err
	}
	shell := t.Shell
	if shell == "" {
		shell = "mongo"
	}
	input, err := MongoInput(ctx, r.Client, r.ProtectedNamespace, t, nil)
	if err != nil {
		return nil, err
	}
	output := &catalogOutput{}
	if err = r.Exec.Stream(ctx, t.Pod, t.Container, MongoCommandArgs(shell, DatabaseCredentialRefs(t, nil)...), input, output); err != nil {
		if errors.Is(err, ErrExecUnavailable) {
			return nil, DatabaseOperationFailure(DatabaseExecTransport, nil)
		}
		return nil, DatabaseStreamFailure(ctx, t, err)
	}
	current, readErr := r.Get(ctx, t.Pod)
	if readErr != nil || current == nil || current.GetUID() != object.GetUID() {
		return nil, &DatabaseOperationError{Step: DatabaseContainerLookup, Reason: DatabaseIdentityChanged}
	}
	return output.Bytes(), nil
}

// Users runs the fixed catalog listing and returns the normalized users.
func (r DatabaseReader) Users(ctx context.Context, t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	raw, err := r.catalog(ctx, t)
	if err != nil {
		return nil, err
	}
	var users []program.DatabaseUser
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err = d.Decode(&users); err != nil || d.Decode(new(any)) != io.EOF {
		return nil, &DatabaseOperationError{Step: DatabaseCatalogParse, Reason: DatabaseInvalidProjection}
	}
	normalized, err := program.NormalizeDatabaseUsers(users)
	if err != nil {
		return nil, &DatabaseOperationError{Step: DatabaseCatalogParse, Reason: DatabaseInvalidProjection}
	}
	return normalized, nil
}

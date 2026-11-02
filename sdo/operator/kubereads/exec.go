package kubereads

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilexec "k8s.io/client-go/util/exec"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// HTTPCommand renders the curl invocation for one HTTP request in a Pod.
func HTTPCommand(h program.HTTPRequest) program.Command {
	args := []string{"curl", "--silent", "--show-error", "--fail", "--output", "/dev/null", "--max-time", "30", "--request", h.Method}
	keys := make([]string, 0, len(h.Headers))
	for k := range h.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "--header", k+": "+h.Headers[k])
	}
	if h.Body != "" {
		args = append(args, "--data-binary", "@-")
	}
	args = append(args, "--", h.URL)
	if h.Bearer != nil || len(h.HeaderEnv) > 0 {
		script := `exec "$0"`
		if h.Bearer != nil {
			script += ` --header "Authorization: Bearer ${` + h.Bearer.Env + `}"`
		}
		keys = keys[:0]
		for name := range h.HeaderEnv {
			keys = append(keys, name)
		}
		sort.Strings(keys)
		quote := func(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
		for _, name := range keys {
			script += ` --header ` + quote(name+": ") + `"${` + h.HeaderEnv[name].Env + `}"`
		}
		script += ` "$@"`
		args = append([]string{"sh", "-c", script}, args...)
	}
	return program.Command{Pod: h.Pod, Container: h.Container, Args: args, Stdin: h.Body}
}

// RunCommand executes one validated command and returns its exit status.
// Nonzero exits are results, not errors; transport and authorization failures
// are errors. The control namespace is refused.
func RunCommand(ctx context.Context, exec PodExec, protectedNamespace string, command program.Command) (int32, error) {
	if protectedNamespace != "" && command.Pod.Namespace == protectedNamespace {
		return 0, apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, command.Pod.Namespace, fmt.Errorf("generic plans cannot access the SDO control namespace"))
	}
	if err := command.Validate(); err != nil {
		return 0, err
	}
	if exec == nil {
		return 0, ErrExecUnavailable
	}
	err := exec.Stream(ctx, command.Pod, command.Container, command.Args, command.Stdin, nil)
	if err == nil {
		return 0, nil
	}
	if errors.Is(err, ErrExecUnavailable) {
		return 0, err
	}
	var exited utilexec.ExitError
	if errors.As(err, &exited) {
		return int32(exited.ExitStatus()), nil
	}
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		return 0, err
	}
	return 0, fmt.Errorf("exec transport failed")
}

// RunOperation runs an Observe-style operation: an exec command or an HTTP
// request issued from inside a Pod.
func RunOperation(ctx context.Context, exec PodExec, protectedNamespace string, o program.ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	if o.Exec != nil {
		return RunCommand(ctx, exec, protectedNamespace, *o.Exec)
	}
	return RunCommand(ctx, exec, protectedNamespace, HTTPCommand(*o.HTTP))
}

// Observe runs one operation in the Pod captured by the snapshot. The Pod's
// identity must be unchanged before and after, so a replaced Pod never answers.
func Observe(ctx context.Context, get func(context.Context, sdk.ObjectRef) (*unstructured.Unstructured, error), exec PodExec, protectedNamespace string, captured *unstructured.Unstructured, o program.ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	if captured == nil || captured.GetUID() == "" {
		return 0, fmt.Errorf("command Pod absent from snapshot")
	}
	before, err := get(ctx, o.Pod())
	if err != nil || before == nil || before.GetUID() != captured.GetUID() {
		return 0, fmt.Errorf("command Pod identity changed")
	}
	code, err := RunOperation(ctx, exec, protectedNamespace, o)
	if err != nil {
		return 0, err
	}
	after, err := get(ctx, o.Pod())
	if err != nil || after == nil || after.GetUID() != captured.GetUID() {
		return 0, fmt.Errorf("command Pod identity changed")
	}
	return code, nil
}

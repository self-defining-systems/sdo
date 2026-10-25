package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	spdytransport "k8s.io/client-go/transport/spdy"
	utilexec "k8s.io/client-go/util/exec"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type PlanOperationClient interface {
	RunOperation(context.Context, program.ExternalOperation) (int32, error)
}
type PlanEnsureOperationClient interface {
	EnsureOperation(context.Context, program.ExternalOperation, program.ExternalOperation) (int32, error)
}

type PlanJobClient interface {
	WaitJob(context.Context, program.RunJob) error
}

func concreteExternalAction(a program.Action) program.Action {
	switch p := a.(type) {
	case *program.EnsureExec:
		if p != nil {
			return *p
		}
	case *program.EnsureHTTP:
		if p != nil {
			return *p
		}
	case *program.RunJob:
		if p != nil {
			return *p
		}
	}
	return a
}
func isExternalAction(a program.Action) bool {
	switch concreteExternalAction(a).(type) {
	case program.EnsureExec, program.EnsureHTTP, program.RunJob:
		return true
	}
	return false
}
func externalPair(action program.Action) (observe, ensure program.ExternalOperation, expect int32) {
	switch a := concreteExternalAction(action).(type) {
	case program.EnsureExec:
		return program.ExternalOperation{Exec: &a.Observe}, program.ExternalOperation{Exec: &a.Ensure}, a.Expect
	case program.EnsureHTTP:
		return program.ExternalOperation{HTTP: &a.Observe}, program.ExternalOperation{HTTP: &a.Ensure}, a.Expect
	}
	return
}
func externalRef(a program.Action) (sdk.ObjectRef, bool) {
	switch a := concreteExternalAction(a).(type) {
	case program.EnsureExec:
		return a.Ensure.Pod, true
	case program.EnsureHTTP:
		return a.Ensure.Pod, true
	case program.RunJob:
		return a.Object.Ref, true
	}
	return sdk.ObjectRef{}, false
}
func (e PlanExecutor) externalStep(ctx context.Context, action program.Action, dry bool) (r PlanApplicationResult, err error) {
	r.Plan = program.Plan{Actions: []program.Action{action}}
	r.DryRun = dry
	r.ActionReports = newPlanReports(r.Plan)
	if job, ok := action.(program.RunJob); ok {
		a := job.Object
		a.Mode = "create"
		r, err = e.generalStep(ctx, IncidentRequest{}, a, dry)
		r.Plan = program.Plan{Actions: []program.Action{action}}
		if err != nil || dry {
			return r, err
		}
		r.ActionsApplied = []program.Action{action}
		// A started script can have external effects even before completion.
		for i := range r.Patches {
			r.Patches[i].Irreversible = true
		}
		if client, ok := e.Client.(PlanJobClient); ok {
			err = client.WaitJob(ctx, job)
		} else {
			err = e.waitForField(ctx, job.Completion)
		}
		if err != nil {
			r.ActionReports[0].Status = "unknown"
			return r, err
		}
		observed, getErr := e.Client.Get(ctx, job.Object.Ref)
		if getErr != nil {
			return r, getErr
		}
		status, _, statusErr := unstructured.NestedMap(observed.Object, "status")
		if statusErr != nil {
			return r, statusErr
		}
		args, _ := json.Marshal([]any{job})
		value, _ := json.Marshal(status)
		r.ActionReports[0].JobStatus = value
		if e.RecordPrimitive != nil {
			if recordErr := e.RecordPrimitive(sdktest.PrimitiveCall{Method: "JobCompletion", Arguments: args, Result: value}); recordErr != nil {
				r.ActionReports[0].RecordingError = "job completion capture unavailable"
			}
		}
		return r, nil
	}
	client, ok := e.Client.(PlanOperationClient)
	if !ok {
		return r, fmt.Errorf("external operation transport unavailable")
	}
	observe, ensure, expect := externalPair(action)
	identities := map[sdk.ObjectRef]string{}
	for _, op := range []program.ExternalOperation{observe, ensure} {
		pod, getErr := e.Client.Get(ctx, op.Pod())
		if getErr != nil || pod == nil || pod.GetUID() == "" {
			return r, fmt.Errorf("operation Pod identity unavailable")
		}
		identities[op.Pod()] = string(pod.GetUID())
	}
	if dry {
		return r, nil
	}
	before, err := client.RunOperation(ctx, observe)
	if err != nil {
		return r, fmt.Errorf("external observation unavailable")
	}
	if before == expect {
		r.ActionReports[0].Status = "applied"
		return r, nil
	}
	for ref, uid := range identities {
		pod, getErr := e.Client.Get(ctx, ref)
		if getErr != nil || pod == nil || string(pod.GetUID()) != uid {
			return r, fmt.Errorf("operation Pod identity changed")
		}
	}
	r.Applied = true
	r.ActionsApplied = []program.Action{action}
	r.ActionReports[0].Status = "unknown"
	r.Patches = []PlanPatch{{Ref: ensure.Pod(), Operation: "external", Irreversible: true}}
	var code int32
	var runErr error
	if paired, ok := e.Client.(PlanEnsureOperationClient); ok {
		code, runErr = paired.EnsureOperation(ctx, observe, ensure)
	} else {
		code, runErr = client.RunOperation(ctx, ensure)
	}
	if runErr != nil {
		return r, fmt.Errorf("external operation outcome unknown")
	}
	r.ActionReports[0].ExitCode = &code
	after, err := client.RunOperation(ctx, observe)
	if err != nil {
		return r, fmt.Errorf("external result observation unavailable")
	}
	for ref, uid := range identities {
		pod, getErr := e.Client.Get(ctx, ref)
		if getErr != nil || pod == nil || string(pod.GetUID()) != uid {
			return r, fmt.Errorf("operation Pod identity changed")
		}
	}
	effect := program.ExternalEffect{Observe: observe, Ensure: ensure, Before: before, After: after, ExitCode: code}
	r.ActionReports[0].ExternalEffect = &effect
	if e.RecordPrimitive != nil {
		// The controller records the measured transition; authored expected state is
		// never treated as effect evidence.
		args, _ := json.Marshal([]any{observe, ensure})
		value, _ := json.Marshal(effect)
		if recordErr := e.RecordPrimitive(sdktest.PrimitiveCall{Method: "ExternalEffect", Arguments: args, Result: value}); recordErr != nil {
			r.ActionReports[0].RecordingError = "external effect capture unavailable"
		}
	}
	if code != 0 || after != expect {
		return r, fmt.Errorf("external ensure failed (exit %d, observation %d, wanted %d)", code, after, expect)
	}
	r.ActionReports[0].Status = "applied"
	return r, nil
}

func (c TypedPlanClient) RunOperation(ctx context.Context, o program.ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	if o.Exec != nil {
		return c.runCommand(ctx, *o.Exec)
	}
	return c.runCommand(ctx, httpCommand(*o.HTTP))
}
func httpCommand(h program.HTTPRequest) program.Command {
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
func (c TypedPlanClient) runCommand(ctx context.Context, command program.Command) (int32, error) {
	if err := command.Validate(); err != nil {
		return 0, err
	}
	if c.Client == nil || c.ExecConfig == nil || c.Client.CoreV1().RESTClient() == nil {
		return 0, fmt.Errorf("%w: controller exec transport unavailable", errPlanExecRefused)
	}
	request := c.Client.CoreV1().RESTClient().Post().Resource("pods").Name(command.Pod.Name).Namespace(command.Pod.Namespace).SubResource("exec").VersionedParams(&corev1.PodExecOptions{Container: command.Container, Command: command.Args, Stdin: true, Stdout: false, Stderr: false, TTY: false}, scheme.ParameterCodec)
	transport, upgrader, err := spdytransport.RoundTripperFor(c.ExecConfig)
	if err != nil {
		return 0, fmt.Errorf("exec transport unavailable")
	}
	executor, err := remotecommand.NewSPDYExecutorRejectRedirects(transport, upgrader, "POST", request.URL())
	if err != nil {
		return 0, fmt.Errorf("exec transport unavailable")
	}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: strings.NewReader(command.Stdin)})
	if err == nil {
		return 0, nil
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

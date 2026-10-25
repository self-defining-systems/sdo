package runtime

import (
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/types/known/durationpb"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

func commandProto(c program.Command) *contracts.Command {
	return &contracts.Command{Pod: planRefProto(c.Pod), Container: c.Container, Args: c.Args, Stdin: c.Stdin}
}
func commandState(c *contracts.Command) program.Command {
	return program.Command{Pod: objectRefState(c.GetPod()), Container: c.GetContainer(), Args: c.GetArgs(), Stdin: c.GetStdin()}
}
func httpProto(h program.HTTPRequest) *contracts.HttpRequest {
	out := &contracts.HttpRequest{Pod: planRefProto(h.Pod), Container: h.Container, Method: h.Method, Url: h.URL, Headers: h.Headers, Body: h.Body}
	if h.Bearer != nil {
		out.Bearer = &contracts.CredentialRef{Env: h.Bearer.Env}
	}
	if h.HeaderEnv != nil {
		out.HeaderEnv = map[string]*contracts.CredentialRef{}
		for name, ref := range h.HeaderEnv {
			out.HeaderEnv[name] = &contracts.CredentialRef{Env: ref.Env}
		}
	}

	return out
}
func httpState(h *contracts.HttpRequest) program.HTTPRequest {
	out := program.HTTPRequest{Pod: objectRefState(h.GetPod()), Container: h.GetContainer(), Method: h.GetMethod(), URL: h.GetUrl(), Headers: h.GetHeaders(), Body: h.GetBody()}
	if h.GetBearer() != nil {
		out.Bearer = &program.CredentialRef{Env: h.GetBearer().GetEnv()}
	}
	if h.GetHeaderEnv() != nil {
		out.HeaderEnv = map[string]program.CredentialRef{}
		for name, ref := range h.GetHeaderEnv() {
			out.HeaderEnv[name] = program.CredentialRef{Env: ref.GetEnv()}
		}
	}

	return out
}
func applyProto(a program.ApplyObject) (*contracts.ApplyObject, error) {
	raw, err := json.Marshal(a.Object)
	return &contracts.ApplyObject{Ref: planRefProto(a.Ref), ObjectJson: raw, Mode: a.Mode, Reason: a.Reason}, err
}
func applyState(a *contracts.ApplyObject) (program.ApplyObject, error) {
	out := program.ApplyObject{Ref: objectRefState(a.GetRef()), Mode: a.GetMode(), Reason: a.GetReason()}
	err := utiljson.Unmarshal(a.GetObjectJson(), &out.Object)
	return out, err
}
func waitProto(w program.WaitFor) (*contracts.WaitFor, error) {
	raw, err := json.Marshal(w.Equals)
	return &contracts.WaitFor{Ref: planRefProto(w.Ref), Path: w.Path, EqualsJson: raw, Timeout: durationpb.New(w.Timeout), Interval: durationpb.New(w.Interval)}, err
}
func waitState(w *contracts.WaitFor) (program.WaitFor, error) {
	out := program.WaitFor{Ref: objectRefState(w.GetRef()), Path: w.GetPath(), Timeout: w.GetTimeout().AsDuration(), Interval: w.GetInterval().AsDuration()}
	err := utiljson.Unmarshal(w.GetEqualsJson(), &out.Equals)
	return out, err
}
func generalActionProto(action program.Action) (*contracts.Action, error) {
	if err := (program.Plan{Actions: []program.Action{action}}).Validate(); err != nil {
		return nil, err
	}
	switch a := generalAction(action).(type) {
	case program.PatchObject:
		out := &contracts.PatchObject{Ref: planRefProto(a.Ref), Type: a.Type, PatchJson: a.Patch, Reason: a.Reason}
		for _, edit := range a.Edits {
			match, err := json.Marshal(edit.Match)
			if err != nil {
				return nil, err
			}
			value, err := json.Marshal(edit.Value)
			if err != nil {
				return nil, err
			}
			out.Edits = append(out.Edits, &contracts.ListEdit{Path: edit.Path, MatchJson: match, Op: edit.Op, ValueJson: value})
		}
		return &contracts.Action{Kind: &contracts.Action_PatchObject{PatchObject: out}}, nil
	case program.ApplyObject:
		out, err := applyProto(a)
		return &contracts.Action{Kind: &contracts.Action_ApplyObject{ApplyObject: out}}, err
	case program.KubernetesRequest:
		return &contracts.Action{Kind: &contracts.Action_KubernetesRequest{KubernetesRequest: &contracts.KubernetesRequest{Ref: planRefProto(a.Ref), Method: a.Method, Path: a.Path, Query: a.Query, ContentType: a.ContentType, Body: a.Body, Reason: a.Reason}}}, nil
	case program.WaitFor:
		out, err := waitProto(a)
		return &contracts.Action{Kind: &contracts.Action_WaitFor{WaitFor: out}}, err
	case program.EnsureExec:
		return &contracts.Action{Kind: &contracts.Action_EnsureExec{EnsureExec: &contracts.EnsureExec{Observe: commandProto(a.Observe), Ensure: commandProto(a.Ensure), Expect: a.Expect, Reason: a.Reason}}}, nil
	case program.EnsureHTTP:
		return &contracts.Action{Kind: &contracts.Action_EnsureHttp{EnsureHttp: &contracts.EnsureHttp{Observe: httpProto(a.Observe), Ensure: httpProto(a.Ensure), Expect: a.Expect, Reason: a.Reason}}}, nil
	case program.RunJob:
		object, err := applyProto(a.Object)
		if err != nil {
			return nil, err
		}
		completion, err := waitProto(a.Completion)
		return &contracts.Action{Kind: &contracts.Action_RunJob{RunJob: &contracts.RunJob{Object: object, Completion: completion}}}, err
	}
	return nil, fmt.Errorf("unsupported general action %T", action)
}
func generalActionState(action *contracts.Action) (program.Action, error) {
	switch kind := action.GetKind().(type) {
	case *contracts.Action_PatchObject:
		a := kind.PatchObject
		out := program.PatchObject{Ref: objectRefState(a.GetRef()), Type: a.GetType(), Patch: a.GetPatchJson(), Reason: a.GetReason()}
		for _, e := range a.GetEdits() {
			edit := program.ListEdit{Path: e.GetPath(), Op: e.GetOp()}
			if err := utiljson.Unmarshal(e.GetMatchJson(), &edit.Match); err != nil {
				return nil, err
			}
			if err := utiljson.Unmarshal(e.GetValueJson(), &edit.Value); err != nil {
				return nil, err
			}
			out.Edits = append(out.Edits, edit)
		}
		return out, nil
	case *contracts.Action_ApplyObject:
		return applyState(kind.ApplyObject)
	case *contracts.Action_KubernetesRequest:
		a := kind.KubernetesRequest
		return program.KubernetesRequest{Ref: objectRefState(a.GetRef()), Method: a.GetMethod(), Path: a.GetPath(), Query: a.GetQuery(), ContentType: a.GetContentType(), Body: a.GetBody(), Reason: a.GetReason()}, nil
	case *contracts.Action_WaitFor:
		return waitState(kind.WaitFor)
	case *contracts.Action_EnsureExec:
		a := kind.EnsureExec
		return program.EnsureExec{Observe: commandState(a.GetObserve()), Ensure: commandState(a.GetEnsure()), Expect: a.GetExpect(), Reason: a.GetReason()}, nil
	case *contracts.Action_EnsureHttp:
		a := kind.EnsureHttp
		return program.EnsureHTTP{Observe: httpState(a.GetObserve()), Ensure: httpState(a.GetEnsure()), Expect: a.GetExpect(), Reason: a.GetReason()}, nil
	case *contracts.Action_RunJob:
		a := kind.RunJob
		object, err := applyState(a.GetObject())
		if err != nil {
			return nil, err
		}
		completion, err := waitState(a.GetCompletion())
		return program.RunJob{Object: object, Completion: completion}, err
	}
	return nil, fmt.Errorf("unknown general action")
}

// Package planwire converts program plan actions to and from their versioned
// protobuf form. The memory host and the controller share it so neither can
// drift from the other's action vocabulary.
package planwire

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"
	utiljson "k8s.io/apimachinery/pkg/util/json"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func objectRefState(ref *contracts.ObjectRef) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()}
}

// RefProto converts an object identity to its wire form.
func RefProto(ref sdk.ObjectRef) *contracts.ObjectRef { return refProto(ref) }

func ActionProto(action program.Action) (*contracts.Action, error) {
	if IsGeneralAction(action) {
		if _, legacyDelete := GeneralAction(action).(program.DeleteObject); !legacyDelete {
			return generalActionProto(action)
		}
	}
	switch a := action.(type) {
	case program.RemoveServicePort:
		return &contracts.Action{Kind: &contracts.Action_RemoveServicePort{RemoveServicePort: &contracts.RemoveServicePort{Ref: refProto(a.Ref), Name: a.Name, Port: a.Port, Protocol: a.Protocol, AllowEmpty: a.AllowEmpty, Reason: a.Reason}}}, nil
	case *program.RemoveServicePort:
		if a != nil {
			return ActionProto(*a)
		}
	case program.EnsureDatabaseUser:
		return databaseActionProto(a), nil
	case *program.EnsureDatabaseUser:
		if a != nil {
			return ActionProto(*a)
		}
	case program.CreateFromSource:
		return &contracts.Action{Kind: &contracts.Action_CreateFromSource{CreateFromSource: &contracts.CreateFromSource{Ref: refProto(a.Ref), Reason: a.Reason}}}, nil
	case program.DeleteObject:
		return &contracts.Action{Kind: &contracts.Action_DeleteObject{DeleteObject: &contracts.DeleteObject{Ref: refProto(a.Ref), Reason: a.Reason}}}, nil
	case *program.CreateFromSource:
		if a != nil {
			return ActionProto(*a)
		}
	case *program.DeleteObject:
		if a != nil {
			return ActionProto(*a)
		}
	case program.ReconcileFromSource:
		return &contracts.Action{Kind: &contracts.Action_ReconcileFromSource{ReconcileFromSource: &contracts.ReconcileFromSource{Ref: refProto(a.Ref)}}}, nil
	case program.Rollback:
		return &contracts.Action{Kind: &contracts.Action_Rollback{Rollback: &contracts.Rollback{Ref: refProto(a.Ref), ToRevision: a.ToRevision}}}, nil
	case program.SetField:
		value, err := planValueProto(a.Value)
		if err != nil {
			return nil, err
		}
		return &contracts.Action{Kind: &contracts.Action_SetField{SetField: &contracts.SetField{Ref: refProto(a.Ref), Path: a.Path, Value: value, Reason: a.Reason, DocumentFormat: a.DocumentFormat, DocumentPath: a.DocumentPath}}}, nil
	case program.DeletePod:
		return &contracts.Action{Kind: &contracts.Action_DeletePod{DeletePod: &contracts.DeletePod{Ref: refProto(a.Ref)}}}, nil
	case program.ScaleWorkload:
		value, err := planValueProto(a.Replicas)
		if err != nil {
			return nil, err
		}
		return &contracts.Action{Kind: &contracts.Action_ScaleWorkload{ScaleWorkload: &contracts.ScaleWorkload{Ref: refProto(a.Ref), Replicas: value}}}, nil
	case program.RetryAdmission:
		return &contracts.Action{Kind: &contracts.Action_RetryAdmission{RetryAdmission: &contracts.RetryAdmission{Ref: refProto(a.Ref)}}}, nil
	case *program.RetryAdmission:
		if a != nil {
			return ActionProto(*a)
		}
	case program.RetireOldRevision:
		return &contracts.Action{Kind: &contracts.Action_RetireOldRevision{RetireOldRevision: &contracts.RetireOldRevision{Ref: refProto(a.Ref)}}}, nil
	case program.Exec:
		command := make([]*contracts.PlanValue, 0, len(a.Command))
		for _, v := range a.Command {
			value, err := planValueProto(v)
			if err != nil {
				return nil, err
			}
			command = append(command, value)
		}
		return &contracts.Action{Kind: &contracts.Action_Exec{Exec: &contracts.Exec{Pod: refProto(a.Pod), Container: a.Container, Command: command}}}, nil
	case *program.DeletePod:
		if a != nil {
			return ActionProto(*a)
		}
	case *program.ScaleWorkload:
		if a != nil {
			return ActionProto(*a)
		}
	case *program.RetireOldRevision:
		if a != nil {
			return ActionProto(*a)
		}
	case *program.Exec:
		if a != nil {
			return ActionProto(*a)
		}

	case program.RestartRollout:
		return &contracts.Action{Kind: &contracts.Action_RestartRollout{RestartRollout: &contracts.RestartRollout{Ref: refProto(a.Ref), Reason: a.Reason}}}, nil
	case *program.ReconcileFromSource:
		if a != nil {
			return ActionProto(*a)
		}
	case *program.Rollback:
		if a != nil {
			return ActionProto(*a)
		}
	case *program.SetField:
		if a != nil {
			return ActionProto(*a)
		}
	case *program.RestartRollout:
		if a != nil {
			return ActionProto(*a)
		}
	}
	return nil, fmt.Errorf("unsupported plan action %T", action)
}

func refProto(ref sdk.ObjectRef) *contracts.ObjectRef {
	return &contracts.ObjectRef{ApiVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

func ActionState(action *contracts.Action) program.Action {
	var decoded program.Action
	if general, err := generalActionState(action); err == nil {
		if (program.Plan{Actions: []program.Action{general}}).Validate() == nil {
			return general
		}
		return nil
	}
	switch kind := action.GetKind().(type) {
	case *contracts.Action_RemoveServicePort:
		a := kind.RemoveServicePort
		decoded = program.RemoveServicePort{Ref: objectRefState(a.GetRef()), Name: a.GetName(), Port: a.GetPort(), Protocol: a.GetProtocol(), AllowEmpty: a.GetAllowEmpty(), Reason: a.GetReason()}
	case *contracts.Action_EnsureDatabaseUser:
		decoded = databaseActionState(kind.EnsureDatabaseUser)
	case *contracts.Action_CreateFromSource:
		decoded = program.CreateFromSource{Ref: objectRefState(kind.CreateFromSource.GetRef()), Reason: kind.CreateFromSource.GetReason()}
	case *contracts.Action_DeleteObject:
		decoded = program.DeleteObject{Ref: objectRefState(kind.DeleteObject.GetRef()), Reason: kind.DeleteObject.GetReason()}
	case *contracts.Action_ReconcileFromSource:
		decoded = program.ReconcileFromSource{Ref: objectRefState(kind.ReconcileFromSource.GetRef())}
	case *contracts.Action_Rollback:
		decoded = program.Rollback{Ref: objectRefState(kind.Rollback.GetRef()), ToRevision: kind.Rollback.GetToRevision()}
	case *contracts.Action_SetField:
		decoded = program.SetField{Ref: objectRefState(kind.SetField.GetRef()), Path: kind.SetField.GetPath(), Value: planValueState(kind.SetField.GetValue()), Reason: kind.SetField.GetReason(), DocumentFormat: kind.SetField.GetDocumentFormat(), DocumentPath: kind.SetField.GetDocumentPath()}
	case *contracts.Action_DeletePod:
		decoded = program.DeletePod{Ref: objectRefState(kind.DeletePod.GetRef())}
	case *contracts.Action_ScaleWorkload:
		decoded = program.ScaleWorkload{Ref: objectRefState(kind.ScaleWorkload.GetRef()), Replicas: planValueState(kind.ScaleWorkload.GetReplicas())}
	case *contracts.Action_RetryAdmission:
		decoded = program.RetryAdmission{Ref: objectRefState(kind.RetryAdmission.GetRef())}
	case *contracts.Action_RetireOldRevision:
		decoded = program.RetireOldRevision{Ref: objectRefState(kind.RetireOldRevision.GetRef())}
	case *contracts.Action_Exec:
		a := program.Exec{Pod: objectRefState(kind.Exec.GetPod()), Container: kind.Exec.GetContainer()}
		for _, v := range kind.Exec.GetCommand() {
			a.Command = append(a.Command, planValueState(v))
		}
		decoded = a
	case *contracts.Action_RestartRollout:
		decoded = program.RestartRollout{Ref: objectRefState(kind.RestartRollout.GetRef()), Reason: kind.RestartRollout.GetReason()}
	}
	return decoded
}

func planValueProto(v program.Value) (*contracts.PlanValue, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	field := &contracts.ValueField{Ref: refProto(v.Ref()), Path: v.Path()}
	switch v.Origin() {
	case program.SourceValue:
		return &contracts.PlanValue{Origin: &contracts.PlanValue_Source{Source: field}}, nil
	case program.ObservedValue:
		return &contracts.PlanValue{Origin: &contracts.PlanValue_Observed{Observed: field}}, nil
	case program.ParameterValue:
		return &contracts.PlanValue{Origin: &contracts.PlanValue_Parameter{Parameter: v.Param()}}, nil
	}
	return nil, fmt.Errorf("invalid value origin")
}

func planValueState(v *contracts.PlanValue) program.Value {
	switch origin := v.GetOrigin().(type) {
	case *contracts.PlanValue_Source:
		return program.FromSource(objectRefState(origin.Source.GetRef()), origin.Source.GetPath())
	case *contracts.PlanValue_Observed:
		return program.FromObserved(objectRefState(origin.Observed.GetRef()), origin.Observed.GetPath())
	case *contracts.PlanValue_Parameter:
		return program.FromParam(origin.Parameter)
	}
	return program.Value{}
}

func GeneralAction(a program.Action) program.Action {
	switch p := a.(type) {
	case *program.SyncWebhookCA:
		if p != nil {
			return *p
		}
	case *program.PatchObject:
		if p != nil {
			return *p
		}
	case *program.ApplyObject:
		if p != nil {
			return *p
		}
	case *program.KubernetesRequest:
		if p != nil {
			return *p
		}
	case *program.WaitFor:
		if p != nil {
			return *p
		}
	case *program.DeleteObject:
		if p != nil {
			return *p
		}
	}
	return ConcreteExternalAction(a)
}

func IsGeneralAction(a program.Action) bool {
	switch GeneralAction(a).(type) {
	case program.SyncWebhookCA, program.PatchObject, program.ApplyObject, program.KubernetesRequest, program.WaitFor, program.DeleteObject:
		return true
	}
	return IsExternalAction(a)
}

func ConcreteExternalAction(a program.Action) program.Action {
	switch p := a.(type) {
	case *program.RunScript:
		if p != nil {
			return *p
		}
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

func IsExternalAction(a program.Action) bool {
	switch ConcreteExternalAction(a).(type) {
	case program.EnsureExec, program.EnsureHTTP, program.RunJob, program.RunScript:
		return true
	}
	return false
}

func commandProto(c program.Command) *contracts.Command {
	return &contracts.Command{Pod: refProto(c.Pod), Container: c.Container, Args: c.Args, Stdin: c.Stdin}
}
func commandState(c *contracts.Command) program.Command {
	return program.Command{Pod: objectRefState(c.GetPod()), Container: c.GetContainer(), Args: c.GetArgs(), Stdin: c.GetStdin()}
}
func httpProto(h program.HTTPRequest) *contracts.HttpRequest {
	out := &contracts.HttpRequest{Pod: refProto(h.Pod), Container: h.Container, Method: h.Method, Url: h.URL, Headers: h.Headers, Body: h.Body}
	if h.Bearer != nil {
		out.Bearer = credentialRefProto(*h.Bearer)
	}
	if h.HeaderEnv != nil {
		out.HeaderEnv = map[string]*contracts.CredentialRef{}
		for name, ref := range h.HeaderEnv {
			out.HeaderEnv[name] = credentialRefProto(ref)
		}
	}

	return out
}
func httpState(h *contracts.HttpRequest) program.HTTPRequest {
	out := program.HTTPRequest{Pod: objectRefState(h.GetPod()), Container: h.GetContainer(), Method: h.GetMethod(), URL: h.GetUrl(), Headers: h.GetHeaders(), Body: h.GetBody()}
	if h.GetBearer() != nil {
		out.Bearer = credentialRefPointer(h.GetBearer())
	}
	if h.GetHeaderEnv() != nil {
		out.HeaderEnv = map[string]program.CredentialRef{}
		for name, ref := range h.GetHeaderEnv() {
			out.HeaderEnv[name] = credentialRefState(ref)
		}
	}

	return out
}
func applyProto(a program.ApplyObject) (*contracts.ApplyObject, error) {
	raw, err := json.Marshal(a.Object)
	return &contracts.ApplyObject{Ref: refProto(a.Ref), ObjectJson: raw, Mode: a.Mode, Reason: a.Reason}, err
}
func applyState(a *contracts.ApplyObject) (program.ApplyObject, error) {
	out := program.ApplyObject{Ref: objectRefState(a.GetRef()), Mode: a.GetMode(), Reason: a.GetReason()}
	err := utiljson.Unmarshal(a.GetObjectJson(), &out.Object)
	return out, err
}
func waitProto(w program.WaitFor) (*contracts.WaitFor, error) {
	raw, err := json.Marshal(w.Equals)
	return &contracts.WaitFor{Ref: refProto(w.Ref), Path: w.Path, EqualsJson: raw, Timeout: durationpb.New(w.Timeout), Interval: durationpb.New(w.Interval)}, err
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
	switch a := GeneralAction(action).(type) {
	case program.SyncWebhookCA:
		t := a.Target
		return &contracts.Action{Kind: &contracts.Action_SyncWebhookCa{SyncWebhookCa: &contracts.SyncWebhookCA{Ref: refProto(t.Ref), Webhook: t.Webhook, SecretNamespace: t.SecretNamespace, SecretName: t.SecretName, SecretKey: t.SecretKey, Reason: a.Reason}}}, nil
	case program.RunScript:
		env := map[string]*contracts.PlanValue{}
		for name, value := range a.Env {
			converted, err := planValueProto(value)
			if err != nil {
				return nil, err
			}
			env[name] = converted
		}
		return &contracts.Action{Kind: &contracts.Action_RunScript{RunScript: &contracts.RunScript{Script: a.Script, Env: env, Timeout: durationpb.New(a.Timeout), Reason: a.Reason}}}, nil
	case program.PatchObject:
		out := &contracts.PatchObject{Ref: refProto(a.Ref), Type: a.Type, PatchJson: a.Patch, Reason: a.Reason}
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
		return &contracts.Action{Kind: &contracts.Action_KubernetesRequest{KubernetesRequest: &contracts.KubernetesRequest{Ref: refProto(a.Ref), Method: a.Method, Path: a.Path, Query: a.Query, ContentType: a.ContentType, Body: a.Body, Reason: a.Reason}}}, nil
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
	case *contracts.Action_SyncWebhookCa:
		a := kind.SyncWebhookCa
		return program.SyncWebhookCA{Target: program.WebhookCA{Ref: objectRefState(a.GetRef()), Webhook: a.GetWebhook(), SecretNamespace: a.GetSecretNamespace(), SecretName: a.GetSecretName(), SecretKey: a.GetSecretKey()}, Reason: a.GetReason()}, nil
	case *contracts.Action_RunScript:
		a := kind.RunScript
		env := map[string]program.Value{}
		for name, value := range a.GetEnv() {
			env[name] = planValueState(value)
		}
		return program.RunScript{Script: a.GetScript(), Env: env, Timeout: a.GetTimeout().AsDuration(), Reason: a.GetReason()}, nil
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

func credentialRefPointer(r *contracts.CredentialRef) *program.CredentialRef {
	out := credentialRefState(r)
	return &out
}

func databaseActionProto(a program.EnsureDatabaseUser) *contracts.Action {
	t := a.Target
	target := &contracts.DatabaseTarget{Engine: t.Engine, Pod: refProto(t.Pod), Container: t.Container, Database: t.Database, Shell: t.Shell}
	if t.Auth != nil {
		target.Auth = &contracts.DatabaseAuth{Database: t.Auth.Database, Username: credentialRefProto(t.Auth.Username), Password: credentialRefProto(t.Auth.Password)}
	}
	out := &contracts.EnsureDatabaseUser{Target: target, User: a.User, Password: credentialRefProto(a.Password), Reason: a.Reason}
	for _, r := range a.Roles {
		out.Roles = append(out.Roles, &contracts.DatabaseRole{Database: r.Database, Name: r.Name})
	}
	return &contracts.Action{Kind: &contracts.Action_EnsureDatabaseUser{EnsureDatabaseUser: out}}
}
func databaseActionState(a *contracts.EnsureDatabaseUser) program.Action {
	t := a.GetTarget()
	target := program.DatabaseTarget{Engine: t.GetEngine(), Pod: objectRefState(t.GetPod()), Container: t.GetContainer(), Database: t.GetDatabase(), Shell: t.GetShell()}
	if t != nil && t.Auth != nil {
		target.Auth = &program.DatabaseAuth{Database: t.Auth.GetDatabase(), Username: credentialRefState(t.Auth.GetUsername()), Password: credentialRefState(t.Auth.GetPassword())}
	}
	out := program.EnsureDatabaseUser{Target: target, User: a.GetUser(), Password: credentialRefState(a.GetPassword()), Reason: a.GetReason()}
	for _, r := range a.GetRoles() {
		out.Roles = append(out.Roles, program.DatabaseRole{Database: r.GetDatabase(), Name: r.GetName()})
	}
	return out
}

func credentialRefProto(r program.CredentialRef) *contracts.CredentialRef {
	if r.ConfigMap != nil {
		k := r.ConfigMap
		return &contracts.CredentialRef{Kind: &contracts.CredentialRef_ConfigMap{ConfigMap: &contracts.ConfigMapCredentialRef{Name: k.Name, Key: k.Key, Assignment: k.Assignment}}}
	}
	return &contracts.CredentialRef{Kind: &contracts.CredentialRef_Env{Env: r.Env}}
}
func credentialRefState(r *contracts.CredentialRef) program.CredentialRef {
	if k := r.GetConfigMap(); k != nil {
		return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: k.GetName(), Key: k.GetKey(), Assignment: k.GetAssignment()}}
	}
	return program.CredentialRef{Env: r.GetEnv()}
}

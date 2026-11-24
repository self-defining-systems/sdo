package runtime

import (
	"encoding/json"
	"fmt"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func structuralSecret(document string, ref sdk.ObjectRef, path string) (sdk.ObjectRef, bool) {
	if isSecretRef(ref) {
		return ref, true
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) >= 5 && strings.Join(parts[:3], "/") == "api/v1/namespaces" && parts[4] == "secrets" {
		return sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: parts[3], Name: strings.SplitN(strings.Join(parts[5:], "/"), "/", 2)[0]}, true
	}
	namespace, name, ok := render.SecretDocumentIdentity(document)
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: namespace, Name: name}, ok
}

type secretToolIdentity struct {
	Action string        `json:"action"`
	Ref    sdk.ObjectRef `json:"ref"`
	Reason string        `json:"reason,omitempty"`
}

func (l *ResponderLoop) recordToolCall(request ToolRequest, response ToolResponse, applied []selectedPlan) {
	raw, _ := json.Marshal(request)
	record := ToolCall{Command: request.Command, At: l.clock().UTC(), Error: response.Error, Summary: response.Summary}
	var identities []secretToolIdentity
	add := func(kind string, ref sdk.ObjectRef, document, reason string) {
		if secret, ok := structuralSecret(document, ref, document); ok {
			identities = append(identities, secretToolIdentity{kind, secret, redactDetectText(reason, 256)})
		}
	}
	if request.Ref != nil {
		add(request.Command, *request.Ref, "", request.Parameters["reason"])
	}
	addTarget := func(kind string, parameters map[string]string) {
		ref, _ := sdk.ParseObjectRef(parameters["ref"])
		if _, known := rawMenuActions()[kind]; !known {
			kind = request.Command
		}
		for key, text := range parameters {
			if key != "reason" && key != "script" {
				if _, ok := structuralSecret(text, sdk.ObjectRef{}, text); !ok {
					continue
				}
				add(kind, ref, text, parameters["reason"])
				return
			}
		}
		add(kind, ref, "", parameters["reason"])
	}
	for _, target := range request.Targets {
		parameters := make(map[string]string, len(target.Parameters))
		for name, values := range target.Parameters {
			parameters[name] = strings.Join(values, "\n")
		}
		addTarget(target.Name, parameters)
	}
	if len(request.Targets) == 0 {
		addTarget(request.Selection.GetOptionId(), request.Parameters)
		for _, option := range request.SelectionSet.GetOptions() {
			addTarget(option.GetOptionId(), option.GetParameters())
		}
	}
	plans := []program.Plan{l.plans[response.PlanID].plan, l.applications[request.ApplicationID].Plan}
	for _, plan := range applied {
		plans = append(plans, plan.plan)
	}
	for _, round := range response.MenuExecution {
		plans = append(plans, l.applications[round.ApplicationID].Plan)
	}
	for _, plan := range plans {
		for _, action := range plan.Actions {
			if identity, ok := structuralSecretAction(action); ok {
				identities = append(identities, identity)
			}
		}
	}
	if len(identities) == 0 {
		record.RequestJSON, record.ResponseJSON = string(raw), string(recordedToolResponseJSON(response, applied))
	} else {
		safe, _ := json.Marshal(ToolRequest{Command: request.Command, ProgramID: request.ProgramID, PlanIDs: request.PlanIDs, Bindings: request.Bindings, stage: request.stage})
		code := response.Report.GetStatus()
		if code != contracts.MitigationReport_STATUS_REFUSED && code != contracts.MitigationReport_STATUS_EXECUTION_ERROR {
			code = contracts.MitigationReport_STATUS_UNSPECIFIED
			if response.Error != "" {
				code = contracts.MitigationReport_STATUS_EXECUTION_ERROR
			}
		}
		type plainResponse ToolResponse
		receipt, _ := json.Marshal(struct {
			plainResponse
			Actions []secretToolIdentity `json:"actions"`
		}{plainResponse: plainResponse{Command: request.Command, PlanID: response.PlanID, ApplicationID: response.ApplicationID, ProgramRuns: response.ProgramRuns, RecordedErrorCode: code},
			Actions: identities})
		record.RequestJSON, record.ResponseJSON, record.Error, record.Summary = string(safe), string(receipt), "", ""
	}
	l.calls = append(l.calls, record)
}

func structuralSecretAction(action program.Action) (secretToolIdentity, bool) {
	ref, _ := planActionRef(action)
	body, _ := json.Marshal(action)
	var fields struct {
		Reason, Path string
		Body, Patch  []byte
		Object       json.RawMessage
	}
	_ = json.Unmarshal(body, &fields)
	for _, document := range []string{string(fields.Object), string(fields.Body), string(fields.Patch)} {
		if found, ok := structuralSecret(document, ref, fields.Path); ok {
			kind := strings.TrimPrefix(strings.TrimPrefix(fmt.Sprintf("%T", action), "*"), "program.")
			return secretToolIdentity{kind, found, redactDetectText(fields.Reason, 256)}, true
		}
	}
	return secretToolIdentity{}, false
}

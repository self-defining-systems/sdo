package runtime

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var secretToolDocument = regexp.MustCompile(`(?i)(?:^|[\s{,])["']?kind["']?\s*:\s*["']?secret(?:["'}\s,]|$)`)
var secretToolName = regexp.MustCompile(`(?i)["']?metadata["']?\s*:\s*\{?\s*["']?name["']?\s*:\s*["']?([a-z0-9][a-z0-9.-]*)`)

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
		if !isSecretRef(ref) && !secretToolDocument.MatchString(document) {
			return
		}
		if !isSecretRef(ref) {
			ref = sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: l.Request.Namespace}
			if name := secretToolName.FindStringSubmatch(document); len(name) == 2 {
				ref.Name = name[1]
			}
		}
		identities = append(identities, secretToolIdentity{kind, ref, redactDetectText(reason, 256)})
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
			if key != "reason" && secretToolDocument.MatchString(text) {
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
			ref, _ := planActionRef(action)
			body, _ := json.Marshal(action)
			var fields struct {
				Reason, Script string
				Body, Patch    []byte
				Object         json.RawMessage
			}
			_ = json.Unmarshal(body, &fields)
			add(strings.TrimPrefix(strings.TrimPrefix(fmt.Sprintf("%T", action), "*"), "program."), ref, fields.Script+"\n"+string(fields.Object)+"\n"+string(fields.Body)+"\n"+string(fields.Patch), fields.Reason)
		}
	}
	if len(identities) == 0 {
		record.RequestJSON, record.ResponseJSON = string(raw), string(recordedToolResponseJSON(response, applied))
	} else {
		safe, _ := json.Marshal(ToolRequest{Command: request.Command, stage: request.stage})
		code := response.Report.GetStatus()
		if code != contracts.MitigationReport_STATUS_REFUSED && code != contracts.MitigationReport_STATUS_EXECUTION_ERROR {
			code = contracts.MitigationReport_STATUS_UNSPECIFIED
			if response.Error != "" {
				code = contracts.MitigationReport_STATUS_EXECUTION_ERROR
			}
		}
		receipt, _ := json.Marshal(struct {
			Command       string                            `json:"command"`
			ApplicationID string                            `json:"application_id,omitempty"`
			Actions       []secretToolIdentity              `json:"actions"`
			Code          contracts.MitigationReport_Status `json:"error_code,omitempty"`
		}{request.Command, response.ApplicationID, identities, code})
		record.RequestJSON, record.ResponseJSON, record.Error, record.Summary = string(safe), string(receipt), "", ""
	}
	l.calls = append(l.calls, record)
}

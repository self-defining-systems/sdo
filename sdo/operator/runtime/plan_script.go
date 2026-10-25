package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// ScriptExecutionRequest exists only at the process boundary. Resolved values
// never become part of durable plan or provenance records.
type ScriptExecutionRequest struct {
	ApplicationID string
	IncidentID    string
	Namespace     string
	Script        string
	Env           map[string]string
	Timeout       time.Duration
	Deadline      time.Time
	Inputs        map[string]program.ValueProvenance
}

type ScriptAPIReceipt = program.ScriptAPIReceipt
type ScriptObjectCapture = program.ScriptObjectCapture
type ScriptExecutionResult = program.ScriptExecutionResult

type PlanScriptClient interface {
	RunScript(context.Context, ScriptExecutionRequest) (ScriptExecutionResult, error)
}

func (e PlanExecutor) scriptStep(ctx context.Context, incident IncidentRequest, a program.RunScript, dry bool) (r PlanApplicationResult, err error) {
	r = PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{a}}, DryRun: dry, ActionReports: newPlanReports(program.Plan{Actions: []program.Action{a}})}
	if err = a.Validate(); err != nil {
		return r, err
	}
	input := e.valueInputs(ctx, incident)
	env := map[string]string{}
	keys := make([]string, 0, len(a.Env))
	for key := range a.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, provenance, resolveErr := program.ResolveValue(a.Env[key], input)
		if resolveErr != nil {
			return r, fmt.Errorf("resolve script environment %s: %w", key, resolveErr)
		}
		text, textErr := program.ScriptEnvText(value)
		if textErr != nil {
			return r, fmt.Errorf("script environment %s: %w", key, textErr)
		}
		env[key] = text
		r.ActionReports[0].Values = append(r.ActionReports[0].Values, provenance)
	}
	if dry {
		return r, nil
	}
	client, ok := e.Client.(PlanScriptClient)
	if !ok {
		return r, fmt.Errorf("script Job transport unavailable")
	}
	timeout := a.Timeout
	if timeout == 0 {
		timeout = program.DefaultScriptTimeout
	}
	if !incident.ResponseDeadline.IsZero() && time.Until(incident.ResponseDeadline) < timeout {
		timeout = time.Until(incident.ResponseDeadline)
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < timeout {
		timeout = time.Until(deadline)
	}
	if timeout <= 0 {
		return r, context.DeadlineExceeded
	}
	id := e.ApplicationID
	if id == "" {
		id = fmt.Sprintf("%x", sha256.Sum256([]byte(incident.IncidentID+"\x00"+program.ScriptDigest(a.Script))))
	}
	id = fmt.Sprintf("%s-script-%d", id, e.scriptActionIndex)
	inputs := map[string]program.ValueProvenance{}
	for i, key := range keys {
		inputs[key] = r.ActionReports[0].Values[i]
	}
	run, runErr := client.RunScript(ctx, ScriptExecutionRequest{Deadline: time.Now().Add(timeout), Inputs: inputs, ApplicationID: id, IncidentID: incident.IncidentID, Namespace: incident.Namespace, Script: a.Script, Env: env, Timeout: timeout})
	if len(run.Inputs) > 0 {
		r.ActionReports[0].Values = nil
		for _, key := range keys {
			r.ActionReports[0].Values = append(r.ActionReports[0].Values, run.Inputs[key])
		}
	}
	public := run
	for _, finding := range incident.Findings {
		if finding.PrimaryResource.Kind != "" {
			public.Scope = append(public.Scope, finding.PrimaryResource)
		}
	}
	public.ResultRef = "ConfigMap/" + incident.Namespace + "/" + run.JobName
	public.StdoutBytes = len(run.Stdout)
	public.StderrBytes = len(run.Stderr)
	public.Stdout = ""
	public.Stderr = ""
	public.Objects = make([]ScriptObjectCapture, 0, len(run.Objects))
	for _, object := range run.Objects {
		public.Objects = append(public.Objects, ScriptObjectCapture{Ref: object.Ref, Written: object.Written})
	}
	r.ActionReports[0].Script = &public
	r.ActionReports[0].ExitCode = run.ExitCode
	if run.JobName != "" {
		r.Applied = true
		r.ActionsApplied = []program.Action{a}
		r.ActionReports[0].Status = "unknown"
		r.Patches = []PlanPatch{{Ref: sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: incident.Namespace, Name: run.JobName}, Operation: "run-script", Irreversible: true}}
	}
	for _, object := range run.Objects {
		if object.Before != nil {
			r.PreImages = append(r.PreImages, PlanPreImage{Ref: object.Ref, Object: object.Before})
		}
	}
	if e.RecordPrimitive != nil && run.JobName != "" {
		raw, _ := json.Marshal(public)
		if recordErr := e.RecordPrimitive(sdktest.PrimitiveCall{Method: "RunScriptReceipt", Arguments: json.RawMessage("[]"), Result: raw}); recordErr != nil {
			r.ActionReports[0].RecordingError = "script receipt capture unavailable"
		}
	}
	if runErr != nil {
		return r, runErr
	}
	if run.ExitCode == nil {
		return r, fmt.Errorf("script exit status unavailable")
	}
	if run.TimedOut {
		return r, fmt.Errorf("script exceeded its incident deadline")
	}
	if *run.ExitCode != 0 {
		return r, fmt.Errorf("script failed (exit %d)", *run.ExitCode)
	}
	r.ActionReports[0].Status = "applied"
	return r, nil
}

func hasAppliedScript(actions []program.Action) bool {
	for _, a := range actions {
		if _, ok := concreteExternalAction(a).(program.RunScript); ok {
			return true
		}
	}
	return false
}

// scriptRepairResources describes the selected opaque repair scope and measured
// API mutations. Scope alone never proves a Kubernetes object was written.
func scriptRepairResources(report PlanActionReport) []sdk.ObjectRef {
	if report.Script == nil {
		return nil
	}
	refs := append([]sdk.ObjectRef(nil), report.Script.Scope...)
	for _, object := range report.Script.Objects {
		if object.Written {
			refs = append(refs, object.Ref)
		}
	}
	seen := map[sdk.ObjectRef]bool{}
	var result []sdk.ObjectRef
	for _, ref := range refs {
		if ref.Kind != "" && ref.Name != "" && !seen[ref] {
			seen[ref] = true
			result = append(result, ref)
		}
	}
	return result
}

package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"slices"
	"sort"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// ScriptExecutionRequest exists only at the process boundary. Resolved values
// never become part of durable plan or provenance records.
type ScriptExecutionRequest struct {
	recoverOnly   bool
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

// ScriptRefusedReason is the failure reason of a script SDO did not run. It
// says nothing about the playbook, so it costs no trust.
const ScriptRefusedReason = "refused_by_sdo_not_run"

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
	if !incident.ResponseDeadline.IsZero() && lifecycleio.Until(incident.ResponseDeadline) < timeout {
		timeout = lifecycleio.Until(incident.ResponseDeadline)
	}
	if deadline, ok := ctx.Deadline(); ok && lifecycleio.Until(deadline) < timeout {
		timeout = lifecycleio.Until(deadline)
	}
	if timeout <= 0 {
		return r, context.DeadlineExceeded
	}
	id := e.ApplicationID
	if id == "" {
		id = fmt.Sprintf("%x", sha256.Sum256([]byte(incident.IncidentID+"\x00"+program.ScriptDigest(a.Script))))
	}
	id = scriptApplicationID(id, e.scriptActionIndex)
	inputs := map[string]program.ValueProvenance{}
	for i, key := range keys {
		inputs[key] = r.ActionReports[0].Values[i]
	}
	run, runErr := client.RunScript(ctx, ScriptExecutionRequest{Deadline: lifecycleio.Now().Add(timeout), Inputs: inputs, ApplicationID: id, IncidentID: incident.IncidentID, Namespace: incident.Namespace, Script: a.Script, Env: env, Timeout: timeout})
	if len(run.Inputs) > 0 {
		r.ActionReports[0].Values = nil
		for _, key := range keys {
			r.ActionReports[0].Values = append(r.ActionReports[0].Values, run.Inputs[key])
		}
	}
	resultErr := r.attachScriptResult(incident, a, 0, run, runErr)
	if e.RecordPrimitive != nil && run.JobName != "" {
		raw, _ := json.Marshal(r.ActionReports[0].Script)
		if recordErr := e.RecordPrimitive(sdktest.PrimitiveCall{Method: "RunScriptReceipt", Arguments: json.RawMessage("[]"), Result: raw}); recordErr != nil {
			r.ActionReports[0].RecordingError = "script receipt capture unavailable"
		}
	}
	return r, resultErr
}

func (r *PlanApplicationResult) attachScriptResult(incident IncidentRequest, a program.RunScript, index int, run ScriptExecutionResult, runErr error) error {
	public := run
	for _, finding := range incident.Findings {
		if finding.PrimaryResource.Kind != "" {
			public.Scope = append(public.Scope, finding.PrimaryResource)
			public.TargetFindings = append(public.TargetFindings, FindingFingerprint(finding))
		}
	}
	public.Objects = make([]ScriptObjectCapture, 0, len(run.Objects))
	for _, object := range run.Objects {
		public.Objects = append(public.Objects, ScriptObjectCapture{Ref: object.Ref, Written: object.Written, ImagesRef: object.ImagesRef, BeforeAbsent: object.BeforeAbsent, AfterAbsent: object.AfterAbsent})
	}
	r.ActionReports[index].Script = &public
	r.ActionReports[index].ExitCode = run.ExitCode
	// No reservation exists before a Job has a name.
	if runErr != nil && (run.Refused != "" || run.JobName == "") {
		r.ActionReports[index].Status = "refused"
		r.ActionReports[index].FailureReason = ScriptRefusedReason
		if run.Refused != "" {
			return runErr
		}
		public.Refused = runErr.Error()
		return scriptRefusedError(public.Refused)
	}
	if run.JobName != "" {
		r.Applied = true
		if !slices.ContainsFunc(r.ActionsApplied, func(other program.Action) bool {
			script, ok := concreteExternalAction(other).(program.RunScript)
			return ok && program.ScriptDigest(script.Script) == run.ScriptDigest
		}) {
			r.ActionsApplied = append(r.ActionsApplied, a)
		}
		r.ActionReports[index].Status = "unknown"
		r.Patches = append(r.Patches, PlanPatch{Ref: sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: incident.Namespace, Name: run.JobName}, Operation: "run-script", Irreversible: true})
	}
	seen := map[sdk.ObjectRef]bool{}
	addEffect := func(ref sdk.ObjectRef, operation, uid string) {
		seen[ref] = true
		effect := PlanObjectEffect{Ref: ref, Operation: operation, UID: uid, Confirmed: true, State: "applied", Irreversible: true}
		if !slices.Contains(r.ObjectEffects, effect) {
			r.ObjectEffects = append(r.ObjectEffects, effect)
		}
	}
	for _, receipt := range run.Receipts {
		operation := "patch"
		if receipt.Subresource == "" {
			switch receipt.Method {
			case "post":
				operation = "create"
			case "delete":
				operation = "delete"
			}
		}
		for _, ref := range receipt.Written {
			if ref.Kind != "" && ref.Name != "" {
				addEffect(ref, operation, receipt.WrittenUIDs[ref.Name])
			}
		}
	}
	for _, ref := range scriptWriteRefs(&run) {
		if !seen[ref] {
			addEffect(ref, "patch", "")
		}
	}
	for _, object := range run.Objects {
		if object.Before != nil || object.BeforeAbsent {
			r.PreImages = append(r.PreImages, newPlanPreImage(object.Ref, object.Before))
		}
	}
	if runErr != nil {
		r.ActionReports[index].FailureReason = "outcome_unknown"
		return runErr
	}
	if run.ExitCode == nil {
		return fmt.Errorf("script exit status unavailable")
	}
	if run.TimedOut {
		return fmt.Errorf("script exceeded its incident deadline")
	}
	if *run.ExitCode != 0 {
		return fmt.Errorf("script failed (exit %d)", *run.ExitCode)
	}
	r.ActionReports[index].Status = "applied"
	r.ActionReports[index].FailureReason = ""
	return nil
}

func scriptApplicationID(application string, index int) string {
	return fmt.Sprintf("%s-script-%d", application, index)
}

func (l *ResponderLoop) recoverInterruptedScripts(ctx context.Context) {
	client, ok := l.Executor.Client.(interface {
		RecoverScript(context.Context, ScriptExecutionRequest) (ScriptExecutionResult, error)
	})
	if !ok {
		return
	}
	for i := range l.archivedApplications {
		app := &l.archivedApplications[i]
		if !hasAppliedScript(app.Result.Plan.Actions) {
			continue
		}
		interrupted := app.Pending && app.CompletedAt.IsZero()
		if !interrupted && !slices.ContainsFunc(app.Result.ActionReports, func(report PlanActionReport) bool {
			return report.Script != nil && report.FailureReason == "outcome_unknown"
		}) {
			continue
		}
		if len(app.Result.ActionReports) != len(app.Result.Plan.Actions) {
			app.Result.ActionReports = newPlanReports(app.Result.Plan)
		}
		for index, action := range app.Result.Plan.Actions {
			script, ok := concreteExternalAction(action).(program.RunScript)
			if !ok || !interrupted && app.Result.ActionReports[index].FailureReason != "outcome_unknown" {
				continue
			}
			run, err := client.RecoverScript(ctx, ScriptExecutionRequest{ApplicationID: scriptApplicationID(app.ID, index), IncidentID: l.Request.IncidentID, Namespace: l.Request.Namespace, Script: script.Script, Timeout: program.MaxScriptTimeout})
			if run.JobName == "" {
				continue
			}
			_ = app.Result.attachScriptResult(l.Request, script, index, run, err)
		}
		if interrupted {
			app.Error = "application interrupted; script effects may be unknown"
			app.Pending = false
			app.CompletedAt = l.clock().UTC()
		}
	}
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
	return append(append([]sdk.ObjectRef(nil), report.Script.Scope...), scriptWriteRefs(report.Script)...)
}

func scriptWriteRefs(run *ScriptExecutionResult) []sdk.ObjectRef {
	var refs []sdk.ObjectRef
	for _, receipt := range run.Receipts {
		refs = append(refs, receipt.Written...)
	}
	for _, object := range run.Objects {
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

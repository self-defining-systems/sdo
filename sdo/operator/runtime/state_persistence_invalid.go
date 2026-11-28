package runtime

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	validate "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// droppedStateNote starts the stage-report note that names what recovery dropped.
const droppedStateNote = "records dropped from invalid controller state: "

const (
	// After this many drops the remaining violations clear whole records.
	invalidStateNarrowDrops = 50
	invalidStateMaxDrops    = 200
	invalidStateNoteBytes   = 2000
)

func clonePersistenceFailure(failure *contracts.StatePersistenceFailure) *contracts.StatePersistenceFailure {
	if failure == nil {
		return nil
	}
	return proto.Clone(failure).(*contracts.StatePersistenceFailure)
}

// lifecycleFields decide what the controller does next. They are never dropped;
// an invalid one comes from the last saved state or recovery refuses.
var lifecycleFields = []protoreflect.Name{"version", "incident_open", "responder_done", "incident_request", "dispatch_state", "closure_state"}

// closureFields are one fact: a closure's phase, payload, receipt and failure.
var closureFields = []protoreflect.Name{"pending_closure", "closure_state", "closure_receipt", "closure_failure"}

type stateRepair struct {
	state    *contracts.ControllerState
	baseline *contracts.ControllerState
	original *contracts.ControllerState
	dropped  []string
}

// recoverInvalidState drops the records the state contract refuses and keeps
// everything else. It never closes an open incident or drops its dispatch.
// Only a closure that already began can be replaced by a failed one.
func (c *Controller) recoverInvalidState(invalid error) (*contracts.ControllerState, error) {
	c.mu.Lock()
	baseline := c.checkpoint
	liveDispatch, liveClosure := c.dispatchState, c.closureState
	observerStarted := c.recoveryObserverStarted
	c.durable = nil // no side effect may run until the replacement is saved
	c.mu.Unlock()
	encoded := map[protoreflect.Name]error{}
	state, err := c.exportState(func(field protoreflect.Name, err error) { encoded[field] = err })
	if err != nil {
		return nil, err
	}
	state, dropped, err := c.repairState(state, baseline, encoded)
	if err != nil {
		return nil, fmt.Errorf("state_persistence_invalid: %w (%w)", err, invalid)
	}
	if err := c.RestoreState(state); err != nil {
		return nil, err
	}
	// Export writes running work as pending for a restart. This process still
	// runs it, so restoring must not start it again.
	c.mu.Lock()
	switch {
	case liveDispatch == contracts.DispatchState_DISPATCH_STATE_RUNNING && c.dispatchState == contracts.DispatchState_DISPATCH_STATE_PENDING,
		liveDispatch == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING && c.dispatchState == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_PENDING:
		c.dispatchState = liveDispatch
	}
	switch {
	case liveClosure == contracts.ClosureState_CLOSURE_STATE_PROCESSING && c.closureState == contracts.ClosureState_CLOSURE_STATE_PENDING,
		liveClosure == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING && c.closureState == contracts.ClosureState_CLOSURE_STATE_COMMITTED:
		c.closureState = liveClosure
	}
	c.recoveryObserverStarted = observerStarted
	c.mu.Unlock()
	c.reportBrokerError("invalid controller state repaired", errors.New(strings.Join(dropped, "; ")))
	return state, nil
}

// repairState is pure apart from reading the configured response stages: it
// serves a live save and a state loaded after a restart.
func (c *Controller) repairState(state, baseline *contracts.ControllerState, encoded map[protoreflect.Name]error) (*contracts.ControllerState, []string, error) {
	r := &stateRepair{state: proto.Clone(state).(*contracts.ControllerState), baseline: baseline, original: state}
	for name, err := range encoded {
		r.note(string(name), err.Error())
		// The record could not be encoded at all, so there is nothing to prune.
		if slices.Contains(closureFields, name) {
			if !r.restoreClosure() {
				return nil, nil, fmt.Errorf("%s: %w", name, err)
			}
		} else if name == "incident_result" {
			r.state.IncidentResult = nil // an older result is not this one
		} else if !r.restore(name) && slices.Contains(lifecycleFields, name) {
			return nil, nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	for {
		err := validateRuntimeState(r.state)
		if err == nil {
			break
		}
		var invalid *protovalidate.ValidationError
		if !errors.As(err, &invalid) || len(invalid.Violations) == 0 || len(r.dropped) >= invalidStateMaxDrops {
			return nil, nil, err
		}
		violation := invalid.Violations[0].Proto
		r.note(protovalidate.FieldPathString(violation.GetField()), violation.GetMessage())
		if !r.drop(c, violation, err) {
			return nil, nil, err
		}
	}
	lost := r.state.GetIncidentResult() == nil && (state.GetIncidentResult() != nil || encoded["incident_result"] != nil)
	if err := c.keepIncidentDispatched(r.state, state, lost); err != nil {
		return nil, nil, err
	}
	// The stage report is the durable, outcome-visible place for the note.
	if report := deterministicStageReport(r.state.GetIncidentRequest().GetPriorStages()); r.state.GetIncidentOpen() && report != nil {
		note := strings.ToValidUTF8(strings.Join(r.dropped, "; "), "")
		if len(note) > invalidStateNoteBytes {
			note = strings.ToValidUTF8(note[:invalidStateNoteBytes], "") + "..."
		}
		report.Error = strings.TrimPrefix(report.GetError()+". ", ". ") + droppedStateNote + note
	}
	if err := validateRuntimeState(r.state); err != nil {
		return nil, nil, err
	}
	return r.state, r.dropped, nil
}

func (r *stateRepair) note(path, message string) {
	if path == "" {
		path = "state"
	}
	r.dropped = append(r.dropped, path+": "+message)
}

func stateField(state *contracts.ControllerState, name protoreflect.Name) protoreflect.FieldDescriptor {
	return state.ProtoReflect().Descriptor().Fields().ByName(name)
}

// restore replaces one field with its last saved value, or clears it. Facts of
// another incident are never restored. It reports whether a saved value was used.
func (r *stateRepair) restore(name protoreflect.Name) bool {
	field := stateField(r.state, name)
	current := r.state.GetIncidentRequest().GetIncidentId()
	perIncident := strings.HasPrefix(string(name), "incident_") || slices.Contains(lifecycleFields, name) && name != "version"
	same := !perIncident || current == "" || r.baseline.GetIncidentRequest().GetIncidentId() == current
	r.state.ProtoReflect().Clear(field)
	if r.baseline == nil || !same || !r.baseline.ProtoReflect().Has(field) {
		return false
	}
	saved := proto.Clone(r.baseline).ProtoReflect()
	r.state.ProtoReflect().Set(field, saved.Get(field))
	return true
}

func (r *stateRepair) restoreClosure() bool {
	if r.baseline == nil || r.baseline.GetPendingClosure().GetRequest().GetIncidentId() != r.state.GetPendingClosure().GetRequest().GetIncidentId() && r.state.GetPendingClosure() != nil {
		return false
	}
	saved := proto.Clone(r.baseline).ProtoReflect()
	for _, name := range closureFields {
		field := stateField(r.state, name)
		if saved.Has(field) {
			r.state.ProtoReflect().Set(field, saved.Get(field))
		} else {
			r.state.ProtoReflect().Clear(field)
		}
	}
	return true
}

func (r *stateRepair) drop(c *Controller, violation *validate.Violation, cause error) bool {
	path := violation.GetField().GetElements()
	if len(path) == 0 {
		switch violation.GetRuleId() {
		case "state.open_incident_has_request":
			return r.restore("incident_request")
		case "state.closure_failure_matches_pending_closure", "state.failed_closure_has_permanent_failure",
			"state.pending_closure_has_payload", "state.committed_closure_has_receipt":
			before := proto.Clone(r.state)
			return r.restoreClosure() && !proto.Equal(before, r.state)
		}
		return false
	}
	top := protoreflect.Name(path[0].GetFieldName())
	if slices.Contains(closureFields, top) {
		return r.dropClosure(c, path, cause)
	}
	if len(r.dropped) <= invalidStateNarrowDrops && dropElement(r.state.ProtoReflect(), path) {
		return true
	}
	if top == "incident_request" {
		if len(r.dropped) <= invalidStateNarrowDrops && len(path) > 1 && clearOptional(r.state.GetIncidentRequest().ProtoReflect(), path[1]) {
			return true
		}
		if r.baseline == nil || proto.Equal(r.baseline.GetIncidentRequest(), r.state.GetIncidentRequest()) {
			return false
		}
		stages := slices.DeleteFunc(r.state.GetIncidentRequest().GetPriorStages(), func(stage *contracts.StageReport) bool { return wire.Validate(stage) != nil })
		if !r.restore(top) {
			return false
		}
		if len(stages) > 0 {
			r.state.IncidentRequest.PriorStages = stages
		}
		return true
	}
	if slices.Contains(lifecycleFields, top) {
		return r.restore(top)
	}
	r.state.ProtoReflect().Clear(stateField(r.state, top))
	return true
}

// dropClosure repairs a closure that already began. A frozen or committed
// payload is the incident's terminal identity, so it is only ever rolled back
// to the saved one.
func (r *stateRepair) dropClosure(c *Controller, path []*validate.FieldPathElement, cause error) bool {
	phase := r.state.GetClosureState()
	fixed := r.state.GetPendingClosure().GetFrozen() || phase == contracts.ClosureState_CLOSURE_STATE_COMMITTED || phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING
	if fixed && r.baseline != nil {
		before := proto.Clone(r.state)
		if r.restoreClosure() && !proto.Equal(before, r.state) {
			return true
		}
	}
	if path[0].GetFieldName() != "pending_closure" || phase == contracts.ClosureState_CLOSURE_STATE_COMMITTED || phase == contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING {
		return false
	}
	if !fixed && len(r.dropped) <= invalidStateNarrowDrops && dropElement(r.state.ProtoReflect(), path) {
		return true
	}
	// Alerts were already judged when this closure began: keep it closing, as
	// failed, without the evidence that cannot be stored.
	if r.state.GetPendingClosure().GetStatePersistenceFailure() != nil {
		return false
	}
	closure := r.failedClosure(c, cause)
	if closure == nil {
		return false
	}
	r.state, _ = lifecyclecore.Step(r.state, lifecyclecore.Event{Kind: lifecyclecore.PersistenceInvalid, Closure: closure})
	return r.state.GetPendingClosure().GetStatePersistenceFailure() != nil
}

func (r *stateRepair) failedClosure(c *Controller, cause error) *contracts.IncidentClosure {
	pending := r.state.GetPendingClosure()
	request := pending.GetRequest()
	if request == nil || wire.Validate(request) != nil {
		request = r.baseline.GetIncidentRequest()
		if request.GetIncidentId() != pending.GetRequest().GetIncidentId() || request == nil {
			return nil
		}
	}
	at := c.clock()
	for _, stamp := range []*timestamppb.Timestamp{pending.GetDetectedAt(), pending.GetDispatchedAt(), pending.GetResponderCompletedAt(), pending.GetVerifiedAt()} {
		if stamp != nil && stamp.IsValid() && stamp.AsTime().After(at) {
			at = stamp.AsTime()
		}
	}
	now := timestamppb.New(at)
	stampOr := func(stamp *timestamppb.Timestamp) *timestamppb.Timestamp {
		if stamp != nil && stamp.IsValid() {
			return stamp
		}
		return now
	}
	failure := &contracts.StatePersistenceFailure{Reason: wire.StringConst(&contracts.StatePersistenceFailure{}, "reason"), Detail: cause.Error()}
	return &contracts.IncidentClosure{Request: proto.Clone(request).(*contracts.IncidentRequest), Result: failedStateResult(request.GetIncidentId(), pending.GetResult(), now),
		DispatchError: proto.String(failure.Reason + ": " + failure.Detail), DetectedAt: stampOr(pending.GetDetectedAt()), DispatchedAt: stampOr(pending.GetDispatchedAt()),
		ResponderCompletedAt: now, VerifiedAt: now, StatePersistenceFailure: failure}
}

// failedStateResult keeps the provider session and usage of a result that
// cannot be stored, and nothing that could authorize credit.
func failedStateResult(incidentID string, result *contracts.IncidentResult, now *timestamppb.Timestamp) *contracts.IncidentResult {
	failed := &contracts.IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentId: incidentID, Status: string(IncidentFailed), Usage: &contracts.UsageMetrics{}, Timing: &contracts.TimingMetrics{StartedAt: now, CompletedAt: now}}
	if result.GetResponderSessionId() != "" {
		failed.ResponderSessionId = proto.String(result.GetResponderSessionId())
	}
	for _, action := range result.GetRepairActions() {
		if wire.Validate(action) == nil {
			failed.RepairActions = append(failed.RepairActions, action)
		}
	}
	for _, application := range result.GetPlanApplications() {
		if wire.Validate(application) == nil {
			failed.PlanApplications = append(failed.PlanApplications, application)
		}
	}
	if usage := result.GetUsage(); usage != nil && wire.Validate(usage) == nil {
		failed.Usage = proto.Clone(usage).(*contracts.UsageMetrics)
	}
	return failed
}

// keepIncidentDispatched restores the owner of an open incident whose stage
// evidence was lost. The ordinary automatic-recovery step hands a stage result
// it can still read to the agent; without one nothing would.
func (c *Controller) keepIncidentDispatched(state, original *contracts.ControllerState, lostResult bool) error {
	if !state.GetIncidentOpen() || !state.GetResponderDone() || state.GetPendingClosure() != nil {
		return nil
	}
	result := original.GetIncidentResult()
	session, path := result.GetResponderSessionId(), result.GetResolutionPath()
	if result == nil && lostResult {
		c.mu.Lock()
		if live := c.currentIncidentResult; live != nil {
			session, path = live.ResponderSessionID, live.ResolutionPath
			result, _ = incidentResultContract(IncidentResult{RepairActions: live.RepairActions, PlanApplications: live.PlanApplications})
		}
		c.mu.Unlock()
	}
	report := deterministicStageReport(state.GetIncidentRequest().GetPriorStages())
	stageOnly := path == "deterministic" && session == "" && !report.GetHandoffSelected() && state.GetIncidentRequest().GetPreparedAgentTask() == ""
	now := timestamppb.New(c.clock())
	if !stageOnly {
		if lostResult {
			// The agent already ran once for this incident; keep its session.
			state.IncidentResult = failedStateResult(state.GetIncidentRequest().GetIncidentId(), result, now)
			if original.GetIncidentResult() == nil && session != "" {
				state.IncidentResult.ResponderSessionId = proto.String(session)
			}
		}
		return nil
	}
	if !lostResult && report != nil {
		return nil
	}
	deadline := state.GetIncidentRequest().GetResponseDeadline()
	agent := c.codePath == nil || slices.Contains(c.codePath.Selection.ResponseStages, "agent")
	if !agent {
		state.IncidentResult = failedStateResult(state.GetIncidentRequest().GetIncidentId(), nil, now)
		return nil
	}
	if deadline != nil && !now.AsTime().Before(deadline.AsTime()) {
		// The lost stage used the response window: the agent's session gets its own.
		state.IncidentRequest.ResponseDeadline = timestamppb.New(now.AsTime().Add(c.config.ResponseTimeout))
	}
	if report == nil {
		report = &contracts.StageReport{Stage: "deterministic", StartedAt: now}
		state.IncidentRequest.PriorStages = append(state.IncidentRequest.PriorStages, report)
	}
	report.HandoffSelected = true
	report.HandoffKeys = alertKeys(incidentRequestState(state.GetIncidentRequest()))
	state.IncidentResult = nil
	state.ResponderDone, state.ResponderCompletedAt = false, nil
	state.DispatchState = contracts.DispatchState_DISPATCH_STATE_PENDING
	state.DispatchError, state.DispatchFailureAttempts = "", 0
	return nil
}

// dropElement removes the deepest list or map entry that contains a violation.
// A plan application is one entry: its reports are indexed by action, so
// removing one of them would attribute the others to the wrong action.
func dropElement(message protoreflect.Message, path []*validate.FieldPathElement) bool {
	var remove func()
	var entry proto.Message // the list entry remove would drop
	for _, element := range path {
		if !message.IsValid() || message.Descriptor().FullName() == (&contracts.PlanApplication{}).ProtoReflect().Descriptor().FullName() && remove != nil {
			break
		}
		field := message.Descriptor().Fields().ByName(protoreflect.Name(element.GetFieldName()))
		if field == nil {
			break
		}
		owner := message
		var next protoreflect.Value
		switch {
		case field.IsList():
			if _, ok := element.GetSubscript().(*validate.FieldPathElement_Index); !ok {
				// The rule is about the list itself (too many entries).
				list := owner.Mutable(field).List()
				if list.Len() == 0 {
					return false
				}
				list.Truncate(list.Len() - 1)
				return true
			}
			list, index := owner.Mutable(field).List(), int(element.GetIndex())
			if index >= list.Len() {
				return false
			}
			remove = func() {
				for i := index; i < list.Len()-1; i++ {
					list.Set(i, list.Get(i+1))
				}
				list.Truncate(list.Len() - 1)
				if list.Len() == 0 {
					owner.Clear(field)
				}
			}
			next, entry = list.Get(index), nil
			if field.Message() != nil {
				entry = next.Message().Interface()
			}
		case field.IsMap():
			if field.MapKey().Kind() != protoreflect.StringKind {
				break
			}
			if _, ok := element.GetSubscript().(*validate.FieldPathElement_StringKey); !ok {
				return false
			}
			key, entries := protoreflect.ValueOfString(element.GetStringKey()).MapKey(), owner.Mutable(field).Map()
			remove, entry = func() { entries.Clear(key) }, nil
			next = entries.Get(key)
		case field.Message() != nil:
			next = owner.Get(field)
		}
		if !next.IsValid() {
			break
		}
		child, ok := next.Interface().(protoreflect.Message)
		if !ok {
			break
		}
		message = child
	}
	if remove == nil {
		return false
	}
	if entry != nil && keepWrite(entry) {
		return true
	}
	remove()
	return true
}

// keepWrite reduces an invalid entry that records an applied write to a valid
// one with an unknown outcome. It reports false when the entry can be dropped,
// or was already reduced and is still invalid.
func keepWrite(entry proto.Message) bool {
	before := proto.Clone(entry)
	switch e := entry.(type) {
	case *contracts.RepairActionReceipt:
		if !e.GetSuccess() && len(e.GetResources()) == 0 {
			return false
		}
		at := timestamppb.New(e.GetStartedAt().AsTime())
		kept := &contracts.RepairActionReceipt{ActionId: e.GetActionId(), Kind: e.GetKind(), Target: e.GetTarget(), Resources: e.GetResources(), Summary: e.GetSummary(), Details: e.GetDetails(), StartedAt: at, CompletedAt: at, FailureReason: "outcome_unknown"}
		for _, text := range []*string{&kept.ActionId, &kept.Kind, &kept.Target, &kept.Summary, &kept.Details} {
			*text = strings.ToValidUTF8(*text, "")
			if *text == "" {
				*text = "unavailable"
			}
		}
		proto.Reset(e)
		proto.Merge(e, kept)
	case *contracts.PlanApplication:
		if !stageReceiptWrote(e) {
			return false
		}
		kept := &contracts.PlanApplication{Plan: &contracts.Plan{}, ApplicationId: e.GetApplicationId(), ProgramIds: e.GetProgramIds(), Applied: true, StartedAt: e.GetStartedAt(), CompletedAt: e.GetCompletedAt(), Error: "receipt reduced: invalid record"}
		proto.Reset(e)
		proto.Merge(e, kept)
	case *contracts.StageReport:
		if e.GetStage() == "" && playbookWrote(e) {
			e.Stage = "deterministic"
		}
	case *contracts.StageMitigation:
		if !stageReceiptWrote(e.GetApplyReceipt()) {
			return false
		}
		kept := &contracts.StageMitigation{PlaybookId: e.GetPlaybookId(), Bindings: e.GetBindings(), PlanId: e.GetPlanId(), Result: "apply_error", Error: e.GetError(), ApplyReceipt: e.GetApplyReceipt()}
		keepWrite(kept.ApplyReceipt)
		proto.Reset(e)
		proto.Merge(e, kept)
	}
	return !proto.Equal(before, entry)
}

// clearOptional clears one set submessage.
func clearOptional(message protoreflect.Message, element *validate.FieldPathElement) bool {
	field := message.Descriptor().Fields().ByName(protoreflect.Name(element.GetFieldName()))
	if field == nil || field.Message() == nil || field.IsList() || field.IsMap() || !message.Has(field) {
		return false
	}
	message.Clear(field)
	return true
}

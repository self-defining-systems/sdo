package runtime

import (
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// These projections join controller state and the shared contract. The
// controller keeps SDK findings and time.Time values; workers see only proto.
func contractTimestamp(at time.Time) *timestamppb.Timestamp { return timestamppb.New(at) }

func optionalContractTimestamp(at *time.Time) *timestamppb.Timestamp {
	if at == nil {
		return nil
	}
	return contractTimestamp(*at)
}

func controllerTime(at *timestamppb.Timestamp) time.Time {
	if at == nil {
		return time.Time{}
	}
	return at.AsTime()
}

func objectRefContract(ref sdk.ObjectRef) *contracts.ObjectRef {
	return &contracts.ObjectRef{ApiVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}
}

func objectRefState(ref *contracts.ObjectRef) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()}
}

func objectRefsContract(refs []sdk.ObjectRef) []*contracts.ObjectRef {
	result := make([]*contracts.ObjectRef, 0, len(refs))
	for _, ref := range refs {
		result = append(result, objectRefContract(ref))
	}
	return result
}

func driftEventsContract(events []sdk.DriftEvent) []*contracts.DriftEvent {
	result := make([]*contracts.DriftEvent, 0, len(events))
	for _, event := range events {
		item := &contracts.DriftEvent{Ref: objectRefContract(event.Ref), Source: event.Source, Manager: event.Manager,
			Operation: event.Operation, ManagerAt: optionalContractTimestamp(event.ManagerAt), ObservedAt: contractTimestamp(event.ObservedAt), Classification: string(event.Classification), ProgramId: event.ProgramID,
			InvestigationStatus: event.InvestigationStatus, InvestigationVerdict: event.InvestigationVerdict, InvestigationReason: event.InvestigationReason, InvestigationCaptureId: event.InvestigationCaptureID}
		for _, field := range event.Fields {
			item.Fields = append(item.Fields, &contracts.DriftField{Path: field.Path, Old: field.Old, New: field.New, Source: field.Source})
		}
		for _, evidence := range event.InvestigationEvidence {
			item.InvestigationEvidence = append(item.InvestigationEvidence, &contracts.DriftEvidence{Kind: evidence.Kind, Observation: evidence.Observation})
		}
		result = append(result, item)
	}
	return result
}

func objectRefsState(refs []*contracts.ObjectRef) []sdk.ObjectRef {
	result := make([]sdk.ObjectRef, 0, len(refs))
	for _, ref := range refs {
		result = append(result, objectRefState(ref))
	}
	return result
}

func bindingsContract(bindings map[string]sdk.ObjectRef) map[string]*contracts.ObjectRef {
	result := make(map[string]*contracts.ObjectRef, len(bindings))
	for name, ref := range bindings {
		result[name] = objectRefContract(ref)
	}
	return result
}

func bindingsState(bindings map[string]*contracts.ObjectRef) map[string]sdk.ObjectRef {
	result := make(map[string]sdk.ObjectRef, len(bindings))
	for name, ref := range bindings {
		result[name] = objectRefState(ref)
	}
	return result
}

func findingContract(finding sdk.Finding) (*contracts.Finding, error) {
	playbooks, err := memory.PlaybookIDs(finding.Playbooks)
	if err != nil {
		return nil, err
	}
	var metadata *structpb.Struct
	if finding.Metadata != nil {
		encoded, err := json.Marshal(finding.Metadata)
		if err != nil {
			return nil, fmt.Errorf("encode finding metadata: %w", err)
		}
		metadata = new(structpb.Struct)
		if err := protojson.Unmarshal(encoded, metadata); err != nil {
			return nil, fmt.Errorf("convert finding metadata: %w", err)
		}
	}
	return &contracts.Finding{
		DetectorId: finding.DetectorID, RuleId: finding.RuleID, Status: string(finding.Status), Severity: string(finding.Severity),
		Summary: finding.Summary, Evidence: finding.Evidence, PrimaryResource: objectRefContract(finding.PrimaryResource),
		RelatedResources: objectRefsContract(finding.RelatedResources), PlaybookIds: playbooks,
		ParameterBindings: bindingsContract(finding.ParameterBindings), Fingerprint: finding.Fingerprint, Metadata: metadata,
	}, nil
}

func findingState(finding *contracts.Finding) sdk.Finding {
	var metadata map[string]any
	if finding.GetMetadata() != nil {
		metadata = finding.GetMetadata().AsMap()
	}
	return sdk.Finding{
		DetectorID: finding.GetDetectorId(), RuleID: finding.GetRuleId(), Status: sdk.FindingStatus(finding.GetStatus()),
		Severity: sdk.FindingSeverity(finding.GetSeverity()), Summary: finding.GetSummary(), Evidence: finding.GetEvidence(),
		PrimaryResource: objectRefState(finding.GetPrimaryResource()), RelatedResources: objectRefsState(finding.GetRelatedResources()),
		Playbooks: append([]string(nil), finding.GetPlaybookIds()...), ParameterBindings: bindingsState(finding.GetParameterBindings()),
		Fingerprint: finding.GetFingerprint(), Metadata: metadata,
	}
}

func evaluationsContract(items []DetectorEvaluation) []*contracts.DetectorEvaluation {
	result := make([]*contracts.DetectorEvaluation, 0, len(items))
	for _, item := range items {
		message := &contracts.DetectorEvaluation{
			DetectorId: item.DetectorID, EvaluatedAt: contractTimestamp(item.EvaluatedAt), Status: string(item.Status),
			Fingerprints: append([]string(nil), item.Fingerprints...),
		}
		if item.Error != "" {
			message.Error = &item.Error
		}
		result = append(result, message)
	}
	return result
}

func evaluationsState(items []*contracts.DetectorEvaluation) []DetectorEvaluation {
	result := make([]DetectorEvaluation, 0, len(items))
	for _, item := range items {
		result = append(result, DetectorEvaluation{
			DetectorID: item.GetDetectorId(), EvaluatedAt: controllerTime(item.GetEvaluatedAt()),
			Status: DetectorEvaluationStatus(item.GetStatus()), Fingerprints: append([]string(nil), item.GetFingerprints()...), Error: item.GetError(),
		})
	}
	return result
}

func stateChangesContract(state *StateChanges) *contracts.StateChanges {
	if state == nil {
		return nil
	}
	result := &contracts.StateChanges{
		BaselineAt: contractTimestamp(state.BaselineAt), ObservedAt: contractTimestamp(state.ObservedAt),
		Changes: make([]*contracts.StateChange, 0, len(state.Changes)), UnobservedKinds: append([]string(nil), state.UnobservedKinds...),
	}
	if state.Omitted != 0 {
		omitted := int32(state.Omitted)
		result.Omitted = &omitted
	}
	for _, change := range state.Changes {
		item := &contracts.StateChange{Kind: change.Kind, Name: change.Name, Change: change.Change, Fields: make([]*contracts.StateFieldChange, 0, len(change.Fields))}
		for _, field := range change.Fields {
			value := &contracts.StateFieldChange{Field: field.Field}
			if field.Before != "" {
				value.Before = &field.Before
			}
			if field.After != "" {
				value.After = &field.After
			}
			item.Fields = append(item.Fields, value)
		}
		result.Changes = append(result.Changes, item)
	}
	return result
}

func incidentRequestContract(request IncidentRequest) (*contracts.IncidentRequest, error) {
	result := &contracts.IncidentRequest{
		SchemaVersion: request.SchemaVersion, Application: request.Application, Namespace: request.Namespace,
		IncidentId: request.IncidentID, Findings: make([]*contracts.Finding, 0, len(request.Findings)),
		DetectorHistory: evaluationsContract(request.DetectorHistory), SurfacedPlaybooks: make([]*contracts.SurfacedPlaybook, 0, len(request.SurfacedPlaybooks)),
		RelevantOutcomes: make([]*contracts.PriorOutcomeEvidence, 0, len(request.RelevantOutcomes)),
		SourceCommit:     request.SourceCommit, DeployedCommit: request.DeployedCommit,
		RepositoryWorktree: request.RepositoryWorktree, RepositoryBaseCommit: request.RepositoryBaseCommit,
		ResponseDeadline: contractTimestamp(request.ResponseDeadline), CancellationToken: request.CancellationToken,
		RepairPolicy: request.RepairPolicy, StateChanges: stateChangesContract(request.StateChanges),
		MaxResponseLoops: int32(request.MaxResponseLoops),
		DriftEvents:      driftEventsContract(request.DriftEvents),
		FastPathSummary:  request.FastPathSummary,
	}
	for _, run := range request.InitialProgramRuns {
		result.InitialProgramRuns = append(result.InitialProgramRuns, programRunContract(run))
	}
	for _, finding := range request.Findings {
		item, err := findingContract(finding)
		if err != nil {
			return nil, fmt.Errorf("finding %s: %w", finding.DetectorID, err)
		}
		result.Findings = append(result.Findings, item)
	}
	for _, surfaced := range request.SurfacedPlaybooks {
		id, err := memory.PlaybookID(surfaced.PlaybookID)
		if err != nil {
			return nil, err
		}
		result.SurfacedPlaybooks = append(result.SurfacedPlaybooks, &contracts.SurfacedPlaybook{PlaybookId: id, ParameterBindings: bindingsContract(surfaced.ParameterBindings)})
	}
	for _, prior := range request.RelevantOutcomes {
		ids, err := memory.PlaybookIDs(prior.AppliedPlaybookIDs)
		if err != nil {
			return nil, err
		}
		result.RelevantOutcomes = append(result.RelevantOutcomes, &contracts.PriorOutcomeEvidence{
			IncidentId: prior.IncidentID, MatchReason: prior.MatchReason, RootCauseSummaries: append([]string(nil), prior.RootCauseSummaries...),
			RepairActionSummaries: append([]string(nil), prior.RepairActionSummaries...), AppliedPlaybookIds: ids,
			SourceCommit: prior.SourceCommit, ExactSourceMatch: prior.ExactSourceMatch,
		})
	}
	if request.FollowUp != nil {
		result.FollowUp = &contracts.FollowUpContext{
			OriginalIncidentId: request.FollowUp.OriginalIncidentID, ParentIncidentId: request.FollowUp.ParentIncidentID,
			Attempt: int32(request.FollowUp.Attempt), MaxFollowUps: int32(request.FollowUp.MaxFollowUps),
			PriorResponderSummary: request.FollowUp.PriorSummary,
		}
	}
	return result, nil
}

func incidentResultContract(result IncidentResult) (*contracts.IncidentResult, error) {
	message := &contracts.IncidentResult{
		SchemaVersion: result.SchemaVersion, IncidentId: result.IncidentID, Status: string(result.Status),
		ConfirmedRootCauses: make([]*contracts.ConfirmedRootCause, 0, len(result.ConfirmedRootCauses)),
		AppliedPlaybooks:    make([]*contracts.AppliedPlaybook, 0, len(result.AppliedPlaybooks)),
		RepairChanges:       append([]string(nil), result.RepairChanges...), RepairActions: make([]*contracts.RepairActionReceipt, 0, len(result.RepairActions)),
		FinalDetectorStates:  evaluationsContract(result.FinalDetectorStates),
		VerificationEvidence: make([]*contracts.VerificationEvidence, 0, len(result.VerificationEvidence)),
		Usage:                proto.Clone(&result.Usage).(*contracts.UsageMetrics),
		Timing:               &contracts.TimingMetrics{StartedAt: contractTimestamp(result.Timing.StartedAt), CompletedAt: contractTimestamp(result.Timing.CompletedAt)},
		ResponseLoopsUsed:    int32(result.ResponseLoopsUsed), ResponseLoopExhausted: result.ResponseLoopExhausted,
		ProgramFailureSignals: append([]string(nil), result.ProgramFailureSignals...),
		ResolutionPath:        result.ResolutionPath, FastPathPrograms: append([]string(nil), result.FastPathPrograms...), FastPathSkippedReason: string(result.FastPathSkippedReason),
	}
	for _, call := range result.ProgramToolCalls {
		message.ProgramToolCalls = append(message.ProgramToolCalls, toolCallContract(call))
	}
	for _, run := range result.ProgramRuns {
		message.ProgramRuns = append(message.ProgramRuns, programRunContract(run))
	}
	for _, application := range result.PlanApplications {
		encoded, err := application.Result.Proto()
		if err != nil {
			return nil, err
		}
		encoded.ApplicationId, encoded.ProgramIds, encoded.Duration, encoded.Error = application.ID, append([]string(nil), application.ProgramIDs...), durationpb.New(application.Result.Duration), application.Error
		encoded.RolledBack, encoded.RollbackError = application.RolledBack, application.RollbackError
		message.PlanApplications = append(message.PlanApplications, encoded)
	}
	for _, cause := range result.ConfirmedRootCauses {
		item := &contracts.ConfirmedRootCause{
			Summary: cause.Summary, Resources: objectRefsContract(cause.Resources),
			Evidence:           make([]*contracts.RootCauseEvidence, 0, len(cause.Evidence)),
			ExplainedDetectors: append([]string(nil), cause.ExplainedDetectors...), StaticContext: append([]string(nil), cause.StaticContext...),
		}
		for _, evidence := range cause.Evidence {
			item.Evidence = append(item.Evidence, &contracts.RootCauseEvidence{Kind: evidence.Kind, Source: evidence.Source, Observation: evidence.Observation})
		}
		message.ConfirmedRootCauses = append(message.ConfirmedRootCauses, item)
	}
	for _, playbook := range result.AppliedPlaybooks {
		id, err := memory.PlaybookID(playbook.PlaybookID)
		if err != nil {
			return nil, err
		}
		item := &contracts.AppliedPlaybook{PlaybookId: id, ParameterBindings: bindingsContract(playbook.ParameterBindings), ScriptNames: make([]string, 0, len(playbook.ScriptNames))}
		for _, script := range playbook.ScriptNames {
			name, err := memory.PlaybookScriptName(id, script)
			if err != nil {
				return nil, err
			}
			item.ScriptNames = append(item.ScriptNames, name)
		}
		message.AppliedPlaybooks = append(message.AppliedPlaybooks, item)
	}
	for _, action := range result.RepairActions {
		message.RepairActions = append(message.RepairActions, &contracts.RepairActionReceipt{
			ActionId: action.ActionID, Kind: action.Kind, Target: action.Target, Resources: objectRefsContract(action.Resources),
			Summary: action.Summary, Details: action.Details, StartedAt: contractTimestamp(action.StartedAt),
			CompletedAt: contractTimestamp(action.CompletedAt), Success: action.Success, Reversible: action.Reversible,
		})
	}
	for _, evidence := range result.VerificationEvidence {
		message.VerificationEvidence = append(message.VerificationEvidence, &contracts.VerificationEvidence{
			Name: evidence.Name, Passed: evidence.Passed, Details: evidence.Details, ObservedAt: contractTimestamp(evidence.ObservedAt),
		})
	}
	for _, reference := range result.ProposedArtifactIDs {
		if reference == ".sdo/outcomes.jsonl" {
			continue
		}
		id, err := memory.ArtifactID(reference)
		if err != nil {
			return nil, err
		}
		message.ProposedArtifactIds = append(message.ProposedArtifactIds, id)
	}
	if result.ResponderSessionID != "" {
		message.ResponderSessionId = &result.ResponderSessionID
	}
	if result.Error != "" {
		message.Error = &result.Error
	}
	if result.LateFindingsPull != nil {
		pull := result.LateFindingsPull
		message.LateFindingsPull = &contracts.LateFindingsPull{
			IncidentId: pull.IncidentID, PullCount: pull.PullCount, DetectorIds: append([]string(nil), pull.DetectorIDs...),
			PlaybookIds: append([]string(nil), pull.PlaybookIDs...), ReportDigest: pull.ReportDigest,
		}
	}
	return message, nil
}

func incidentResultState(message *contracts.IncidentResult) IncidentResult {
	result := IncidentResult{
		SchemaVersion: message.GetSchemaVersion(), IncidentID: message.GetIncidentId(), Status: IncidentStatus(message.GetStatus()),
		ConfirmedRootCauses:  make([]ConfirmedRootCause, 0, len(message.GetConfirmedRootCauses())),
		AppliedPlaybooks:     make([]AppliedPlaybook, 0, len(message.GetAppliedPlaybooks())),
		RepairChanges:        append([]string(nil), message.GetRepairChanges()...),
		RepairActions:        make([]RepairActionReceipt, 0, len(message.GetRepairActions())),
		FinalDetectorStates:  evaluationsState(message.GetFinalDetectorStates()),
		ProposedArtifactIDs:  append([]string(nil), message.GetProposedArtifactIds()...),
		VerificationEvidence: make([]VerificationEvidence, 0, len(message.GetVerificationEvidence())),
		Timing:               TimingMetrics{StartedAt: controllerTime(message.GetTiming().GetStartedAt()), CompletedAt: controllerTime(message.GetTiming().GetCompletedAt())},
		ResponderSessionID:   message.GetResponderSessionId(), Error: message.GetError(),
		ResponseLoopsUsed: int(message.GetResponseLoopsUsed()), ResponseLoopExhausted: message.GetResponseLoopExhausted(),
		ProgramFailureSignals: append([]string(nil), message.GetProgramFailureSignals()...),
		ResolutionPath:        message.GetResolutionPath(), FastPathPrograms: append([]string(nil), message.GetFastPathPrograms()...), FastPathSkippedReason: FastPathSkipReason(message.GetFastPathSkippedReason()),
	}
	for _, call := range message.GetProgramToolCalls() {
		result.ProgramToolCalls = append(result.ProgramToolCalls, toolCallState(call))
	}
	for _, run := range message.GetProgramRuns() {
		result.ProgramRuns = append(result.ProgramRuns, programRunState(run))
	}
	for _, application := range message.GetPlanApplications() {
		result.PlanApplications = append(result.PlanApplications, planApplicationState(application))
	}
	if message.GetUsage() != nil {
		result.Usage = *proto.Clone(message.GetUsage()).(*contracts.UsageMetrics)
	}
	for _, cause := range message.GetConfirmedRootCauses() {
		item := ConfirmedRootCause{
			Summary: cause.GetSummary(), Resources: objectRefsState(cause.GetResources()),
			Evidence:           make([]RootCauseEvidence, 0, len(cause.GetEvidence())),
			ExplainedDetectors: append([]string(nil), cause.GetExplainedDetectors()...), StaticContext: append([]string(nil), cause.GetStaticContext()...),
		}
		for _, evidence := range cause.GetEvidence() {
			item.Evidence = append(item.Evidence, RootCauseEvidence{Kind: evidence.GetKind(), Source: evidence.GetSource(), Observation: evidence.GetObservation()})
		}
		result.ConfirmedRootCauses = append(result.ConfirmedRootCauses, item)
	}
	for _, playbook := range message.GetAppliedPlaybooks() {
		result.AppliedPlaybooks = append(result.AppliedPlaybooks, AppliedPlaybook{
			PlaybookID: playbook.GetPlaybookId(), ParameterBindings: bindingsState(playbook.GetParameterBindings()),
			ScriptNames: append([]string(nil), playbook.GetScriptNames()...),
		})
	}
	for _, action := range message.GetRepairActions() {
		result.RepairActions = append(result.RepairActions, RepairActionReceipt{
			ActionID: action.GetActionId(), Kind: action.GetKind(), Target: action.GetTarget(), Resources: objectRefsState(action.GetResources()),
			Summary: action.GetSummary(), Details: action.GetDetails(), StartedAt: controllerTime(action.GetStartedAt()),
			CompletedAt: controllerTime(action.GetCompletedAt()), Success: action.GetSuccess(), Reversible: action.GetReversible(),
		})
	}
	for _, evidence := range message.GetVerificationEvidence() {
		result.VerificationEvidence = append(result.VerificationEvidence, VerificationEvidence{
			Name: evidence.GetName(), Passed: evidence.GetPassed(), Details: evidence.GetDetails(), ObservedAt: controllerTime(evidence.GetObservedAt()),
		})
	}
	if pull := message.GetLateFindingsPull(); pull != nil {
		result.LateFindingsPull = &LateFindingsPull{
			IncidentID: pull.GetIncidentId(), PullCount: pull.GetPullCount(), DetectorIDs: append([]string(nil), pull.GetDetectorIds()...),
			PlaybookIDs: append([]string(nil), pull.GetPlaybookIds()...), ReportDigest: pull.GetReportDigest(),
		}
	}
	return result
}

func incidentClosureContract(closure IncidentClosure) (*contracts.IncidentClosure, error) {
	request, err := incidentRequestContract(closure.Request)
	if err != nil {
		return nil, fmt.Errorf("closure request: %w", err)
	}
	message := &contracts.IncidentClosure{
		Request: request, FinalDetectorStates: evaluationsContract(closure.FinalDetectorStates),
		IncidentDetectorStates:              evaluationsContract(closure.IncidentDetectorStates),
		DetectorTimeline:                    make([]*contracts.DetectorTimelineEntry, 0, len(closure.DetectorTimeline)),
		IncidentDetectorFiredBeforeDispatch: closure.IncidentDetectorFiredBeforeDispatch,
		IncidentDetectorFiredAfterDispatch:  closure.IncidentDetectorFiredAfterDispatch,
		NoIncidentDetectorFired:             closure.NoIncidentDetectorFired,
		FinalStateChanges:                   stateChangesContract(closure.FinalStateChanges),
		DetectedAt:                          contractTimestamp(closure.DetectedAt), DispatchedAt: contractTimestamp(closure.DispatchedAt),
		ResponderCompletedAt: contractTimestamp(closure.ResponderCompletedAt), VerifiedAt: contractTimestamp(closure.VerifiedAt),
		HealthClearedAt:          optionalContractTimestamp(closure.HealthClearedAt),
		ObservedStateChanges:     make([]*contracts.ObservedStateChange, 0, len(closure.ObservedStateChanges)),
		DetectorReviewRequiredAt: optionalContractTimestamp(closure.DetectorReviewRequiredAt),
		CleanedHelpers:           append([]string(nil), closure.CleanedHelpers...),
	}
	if closure.Result != nil {
		message.Result, err = incidentResultContract(*closure.Result)
		if err != nil {
			return nil, fmt.Errorf("closure result: %w", err)
		}
	}
	if closure.DispatchError != "" {
		message.DispatchError = &closure.DispatchError
	}
	if closure.DetectorReviewReason != "" {
		message.DetectorReviewReason = &closure.DetectorReviewReason
	}
	for _, entry := range closure.DetectorTimeline {
		playbooks, err := memory.PlaybookIDs(entry.SurfacedPlaybookIDs)
		if err != nil {
			return nil, fmt.Errorf("timeline playbooks: %w", err)
		}
		message.DetectorTimeline = append(message.DetectorTimeline, &contracts.DetectorTimelineEntry{
			DetectorId: entry.DetectorID, DetectorClass: entry.DetectorClass, Owner: entry.Owner, RuleId: entry.RuleID,
			Fingerprint: entry.Fingerprint, ParameterBindings: bindingsContract(entry.ParameterBindings),
			SurfacedPlaybookIds: playbooks, FirstActivatedAt: contractTimestamp(entry.FirstActivatedAt),
			LastSeenAt: contractTimestamp(entry.LastSeenAt), ClearedAt: optionalContractTimestamp(entry.ClearedAt),
			Relation: string(entry.Relation), Activations: int32(entry.Activations),
		})
	}
	for _, change := range closure.ObservedStateChanges {
		message.ObservedStateChanges = append(message.ObservedStateChanges, &contracts.ObservedStateChange{
			Kind: change.Kind, Name: change.Name, FirstObservedAt: contractTimestamp(change.FirstObservedAt),
		})
	}
	return message, nil
}

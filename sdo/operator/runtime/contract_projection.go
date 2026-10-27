package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
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

func resourceOwnersContract(owners []ResourceOwners) []*contracts.ResourceOwners {
	var result []*contracts.ResourceOwners
	for _, owner := range owners {
		resource := objectRefsContract([]sdk.ObjectRef{owner.Resource})
		result = append(result, &contracts.ResourceOwners{Resource: resource[0], OwnerChain: objectRefsContract(owner.OwnerChain)})
	}
	return result
}

func resourceOwnersState(owners []*contracts.ResourceOwners) []ResourceOwners {
	var result []ResourceOwners
	for _, owner := range owners {
		result = append(result, ResourceOwners{Resource: objectRefState(owner.GetResource()), OwnerChain: objectRefsState(owner.GetOwnerChain())})
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
		RelatedResources: objectRefsContract(finding.RelatedResources), PlaybookIds: append([]string{}, finding.Playbooks...),
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
		if item.Reason != "" {
			message.Reason = &item.Reason
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
			Status: DetectorEvaluationStatus(item.GetStatus()), Fingerprints: append([]string(nil), item.GetFingerprints()...), Error: item.GetError(), Reason: item.GetReason(),
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
		if change.APIVersion != "" {
			item.ApiVersion = proto.String(change.APIVersion)
			item.Namespace = proto.String(change.Namespace)
		}
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
		ObservationGeneration: request.ObservationGeneration,
		SchemaVersion:         request.SchemaVersion, Application: request.Application, Namespace: request.Namespace,
		IncidentId: request.IncidentID, Findings: make([]*contracts.Finding, 0, len(request.Findings)),
		DetectorHistory: evaluationsContract(request.DetectorHistory), SurfacedPlaybooks: make([]*contracts.SurfacedPlaybook, 0, len(request.SurfacedPlaybooks)),
		RelevantOutcomes: make([]*contracts.PriorOutcomeEvidence, 0, len(request.RelevantOutcomes)),
		SourceCommit:     request.SourceCommit, DeployedCommit: request.DeployedCommit,
		RepositoryWorktree: request.RepositoryWorktree, RepositoryBaseCommit: request.RepositoryBaseCommit,
		ResponseDeadline: optionalTimestamp(request.ResponseDeadline), CancellationToken: request.CancellationToken,
		RepairPolicy: request.RepairPolicy, StateChanges: stateChangesContract(request.StateChanges),
		MaxResponseLoops: int32(request.MaxResponseLoops),
		DriftEvents:      driftEventsContract(request.DriftEvents), PreparedAgentTask: request.PreparedAgentTask,
		PriorResponseProgress:  cloneResponseProgress(request.PriorResponseProgress),
		PlaybookCoverage:       cloneCoverage(request.PlaybookCoverage),
		PriorStages:            cloneStageReports(request.PriorStages),
		NamespaceOnboarding:    cloneNamespaceOnboarding(request.NamespaceOnboarding),
		DisabledSetupDetectors: cloneDisabledSetupDetectors(request.DisabledSetupDetectors),
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
		result.SurfacedPlaybooks = append(result.SurfacedPlaybooks, &contracts.SurfacedPlaybook{PlaybookId: surfaced.PlaybookID, ParameterBindings: bindingsContract(surfaced.ParameterBindings)})
	}
	for _, prior := range request.RelevantOutcomes {
		ids, err := layout.PlaybookIDs(prior.AppliedPlaybookIDs)
		if err != nil {
			return nil, err
		}
		result.RelevantOutcomes = append(result.RelevantOutcomes, &contracts.PriorOutcomeEvidence{
			IncidentId: prior.IncidentID, MatchReason: prior.MatchReason, RootCauseSummaries: append([]string(nil), prior.RootCauseSummaries...),
			RepairActionSummaries: append([]string(nil), prior.RepairActionSummaries...), AppliedPlaybookIds: ids,
			SourceCommit: prior.SourceCommit, ExactSourceMatch: prior.ExactSourceMatch,
		})
	}
	return result, nil
}

func incidentResultContract(result IncidentResult) (*contracts.IncidentResult, error) {
	message := &contracts.IncidentResult{
		DriftReprompt: cloneDriftReprompt(result.DriftReprompt),
		SchemaVersion: result.SchemaVersion, IncidentId: result.IncidentID, Status: string(result.Status),
		ConfirmedRootCauses: make([]*contracts.ConfirmedRootCause, 0, len(result.ConfirmedRootCauses)),
		DriftEvents:         driftEventsContract(result.DriftEvents),
		AppliedPlaybooks:    make([]*contracts.AppliedPlaybook, 0, len(result.AppliedPlaybooks)),
		RepairChanges:       append([]string(nil), result.RepairChanges...), RepairActions: make([]*contracts.RepairActionReceipt, 0, len(result.RepairActions)),
		FinalDetectorStates:  evaluationsContract(result.FinalDetectorStates),
		VerificationEvidence: make([]*contracts.VerificationEvidence, 0, len(result.VerificationEvidence)),
		Usage:                proto.Clone(&result.Usage).(*contracts.UsageMetrics),
		Timing:               &contracts.TimingMetrics{StartedAt: optionalTimestamp(result.Timing.StartedAt), CompletedAt: optionalTimestamp(result.Timing.CompletedAt)},
		ResponseLoopsUsed:    int32(result.ResponseLoopsUsed), ResponseLoopExhausted: result.ResponseLoopExhausted,
		ProgramFailureSignals: append([]string(nil), result.ProgramFailureSignals...),
		ResolutionPath:        result.ResolutionPath,
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
		encoded.StartedAt, encoded.CompletedAt = optionalTimestamp(application.StartedAt), optionalTimestamp(application.CompletedAt)
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
		id, err := layout.PlaybookID(playbook.PlaybookID)
		if err != nil {
			return nil, err
		}
		item := &contracts.AppliedPlaybook{PlaybookId: id, ParameterBindings: bindingsContract(playbook.ParameterBindings), ScriptNames: make([]string, 0, len(playbook.ScriptNames))}
		for _, script := range playbook.ScriptNames {
			name, err := layout.PlaybookScriptName(id, script)
			if err != nil {
				return nil, err
			}
			item.ScriptNames = append(item.ScriptNames, name)
		}
		message.AppliedPlaybooks = append(message.AppliedPlaybooks, item)
	}
	for _, finding := range result.UnresolvedFindings {
		encoded, err := findingContract(finding)
		if err != nil {
			return nil, err
		}
		message.UnresolvedFindings = append(message.UnresolvedFindings, encoded)
	}
	for _, action := range result.RepairActions {
		message.RepairActions = append(message.RepairActions, &contracts.RepairActionReceipt{
			ActionId: action.ActionID, Kind: action.Kind, Target: action.Target, Resources: objectRefsContract(action.Resources),
			Summary: action.Summary, Details: action.Details, StartedAt: optionalTimestamp(action.StartedAt),
			CompletedAt: optionalTimestamp(action.CompletedAt), Success: action.Success, Reversible: action.Reversible, FailureReason: action.FailureReason, ScriptDigest: action.ScriptDigest,
			Owners: resourceOwnersContract(action.Owners),
		})
	}
	for _, evidence := range result.VerificationEvidence {
		message.VerificationEvidence = append(message.VerificationEvidence, &contracts.VerificationEvidence{
			Name: evidence.Name, Passed: evidence.Passed, Details: evidence.Details, ObservedAt: contractTimestamp(evidence.ObservedAt),
		})
	}
	for _, reference := range result.ProposedArtifactIDs {
		if reference == layout.LegacyOutcomesFile {
			continue
		}
		id, err := layout.ArtifactID(reference)
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
		DriftReprompt: cloneDriftReprompt(message.GetDriftReprompt()),
		SchemaVersion: message.GetSchemaVersion(), IncidentID: message.GetIncidentId(), Status: IncidentStatus(message.GetStatus()),
		ConfirmedRootCauses:  make([]ConfirmedRootCause, 0, len(message.GetConfirmedRootCauses())),
		DriftEvents:          driftEventsState(message.GetDriftEvents()),
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
		ResolutionPath:        message.GetResolutionPath(),
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
			CompletedAt: controllerTime(action.GetCompletedAt()), Success: action.GetSuccess(), Reversible: action.GetReversible(), FailureReason: action.GetFailureReason(), ScriptDigest: action.GetScriptDigest(),
			Owners: resourceOwnersState(action.GetOwners()),
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
	for _, finding := range message.GetUnresolvedFindings() {
		result.UnresolvedFindings = append(result.UnresolvedFindings, findingState(finding))
	}
	return result
}

func incidentClosureContract(closure IncidentClosure) (*contracts.IncidentClosure, error) {
	request, err := incidentRequestContract(closure.Request)
	if err != nil {
		return nil, fmt.Errorf("closure request: %w", err)
	}
	// A closed incident's evidence needs the findings it handled, not the frozen task bytes (which carry the
	// whole memory snapshot) that every later reader of the closure would otherwise copy.
	request.PreparedAgentTask = ""
	message := &contracts.IncidentClosure{
		VerifiedWhileResponderRunning: closure.VerifiedWhileResponderRunning,
		ClosureStabilityWindow:        durationpb.New(closure.ClosureStabilityWindow),
		ObservationGeneration:         closure.ObservationGeneration,
		Request:                       request, FinalDetectorStates: evaluationsContract(closure.FinalDetectorStates),
		IncidentDetectorStates:              evaluationsContract(closure.IncidentDetectorStates),
		PartialDetectorStates:               evaluationsContract(closure.PartialDetectorStates),
		IncidentDetectorFiredBeforeDispatch: closure.IncidentDetectorFiredBeforeDispatch,
		IncidentDetectorFiredAfterDispatch:  closure.IncidentDetectorFiredAfterDispatch,
		NoIncidentDetectorFired:             closure.NoIncidentDetectorFired,
		FinalStateChanges:                   stateChangesContract(closure.FinalStateChanges),
		DetectedAt:                          contractTimestamp(closure.DetectedAt), DispatchedAt: contractTimestamp(closure.DispatchedAt),
		ResponderCompletedAt: contractTimestamp(closure.ResponderCompletedAt), VerifiedAt: contractTimestamp(closure.VerifiedAt),
		HealthClearedAt:   optionalContractTimestamp(closure.HealthClearedAt),
		CleanedHelpers:    append([]string(nil), closure.CleanedHelpers...),
		UnconfirmedReason: closure.UnconfirmedReason,
		UnconfirmedDetail: closure.UnconfirmedDetail,
		SourceCheckNotes:  append([]string(nil), closure.SourceCheckNotes...),
	}
	if closure.ClosureStabilityWindow == 0 {
		message.ClosureStabilityWindow = nil
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
	message.DetectorTimeline = detectorTimelineContract(closure.DetectorTimeline)
	return message, nil
}

func optionalTimestamp(at time.Time) *timestamppb.Timestamp {
	if at.IsZero() {
		return nil
	}
	return contractTimestamp(at)
}

func timestampsContract(times map[string]time.Time) map[string]*timestamppb.Timestamp {
	if len(times) == 0 {
		return nil
	}
	result := make(map[string]*timestamppb.Timestamp, len(times))
	for key, at := range times {
		result[key] = contractTimestamp(at)
	}
	return result
}

func timestampsState(times map[string]*timestamppb.Timestamp) map[string]time.Time {
	if len(times) == 0 {
		return nil
	}
	result := make(map[string]time.Time, len(times))
	for key, at := range times {
		result[key] = controllerTime(at)
	}
	return result
}

func findingsContract(findings []sdk.Finding) ([]*contracts.Finding, error) {
	result := make([]*contracts.Finding, 0, len(findings))
	for _, finding := range findings {
		item, err := findingContract(finding)
		if err != nil {
			return nil, fmt.Errorf("finding %s: %w", finding.DetectorID, err)
		}
		result = append(result, item)
	}
	return result, nil
}

func findingsState(findings []*contracts.Finding) []sdk.Finding {
	result := make([]sdk.Finding, 0, len(findings))
	for _, finding := range findings {
		result = append(result, findingState(finding))
	}
	return result
}

func stateChangesState(message *contracts.StateChanges) *StateChanges {
	if message == nil {
		return nil
	}
	result := &StateChanges{
		BaselineAt: controllerTime(message.GetBaselineAt()), ObservedAt: controllerTime(message.GetObservedAt()),
		Changes: make([]StateChange, 0, len(message.GetChanges())), Omitted: int(message.GetOmitted()),
		UnobservedKinds: append([]string(nil), message.GetUnobservedKinds()...),
	}
	for _, change := range message.GetChanges() {
		item := StateChange{APIVersion: change.GetApiVersion(), Namespace: change.GetNamespace(), Kind: change.GetKind(), Name: change.GetName(), Change: change.GetChange()}
		for _, field := range change.GetFields() {
			item.Fields = append(item.Fields, StateFieldChange{Field: field.GetField(), Before: field.GetBefore(), After: field.GetAfter()})
		}
		result.Changes = append(result.Changes, item)
	}
	return result
}

func incidentRequestState(message *contracts.IncidentRequest) *IncidentRequest {
	if message == nil {
		return nil
	}
	result := &IncidentRequest{
		PriorResponseProgress:  cloneResponseProgress(message.GetPriorResponseProgress()),
		ObservationGeneration:  message.GetObservationGeneration(),
		NamespaceOnboarding:    cloneNamespaceOnboarding(message.GetNamespaceOnboarding()),
		DisabledSetupDetectors: cloneDisabledSetupDetectors(message.GetDisabledSetupDetectors()),
		PriorStages:            cloneStageReports(message.GetPriorStages()),
		SchemaVersion:          message.GetSchemaVersion(), Application: message.GetApplication(), Namespace: message.GetNamespace(),
		IncidentID: message.GetIncidentId(), Findings: findingsState(message.GetFindings()),
		DetectorHistory:   evaluationsState(message.GetDetectorHistory()),
		SurfacedPlaybooks: make([]SurfacedPlaybook, 0, len(message.GetSurfacedPlaybooks())),
		RelevantOutcomes:  make([]PriorOutcomeEvidence, 0, len(message.GetRelevantOutcomes())),
		SourceCommit:      message.GetSourceCommit(), DeployedCommit: message.GetDeployedCommit(),
		RepositoryWorktree: message.GetRepositoryWorktree(), RepositoryBaseCommit: message.GetRepositoryBaseCommit(),
		ResponseDeadline: controllerTime(message.GetResponseDeadline()), CancellationToken: message.GetCancellationToken(),
		RepairPolicy: message.GetRepairPolicy(), StateChanges: stateChangesState(message.GetStateChanges()),
		DriftEvents: driftEventsState(message.GetDriftEvents()), PlaybookCoverage: cloneCoverage(message.GetPlaybookCoverage()), MaxResponseLoops: int(message.GetMaxResponseLoops()), PreparedAgentTask: message.GetPreparedAgentTask(),
	}
	for _, run := range message.GetInitialProgramRuns() {
		result.InitialProgramRuns = append(result.InitialProgramRuns, programRunState(run))
	}
	for _, surfaced := range message.GetSurfacedPlaybooks() {
		result.SurfacedPlaybooks = append(result.SurfacedPlaybooks, SurfacedPlaybook{
			PlaybookID: surfaced.GetPlaybookId(), ParameterBindings: bindingsState(surfaced.GetParameterBindings()),
		})
	}
	for _, prior := range message.GetRelevantOutcomes() {
		result.RelevantOutcomes = append(result.RelevantOutcomes, PriorOutcomeEvidence{
			IncidentID: prior.GetIncidentId(), MatchReason: prior.GetMatchReason(),
			RootCauseSummaries:    append([]string(nil), prior.GetRootCauseSummaries()...),
			RepairActionSummaries: append([]string(nil), prior.GetRepairActionSummaries()...),
			AppliedPlaybookIDs:    append([]string(nil), prior.GetAppliedPlaybookIds()...),
			SourceCommit:          prior.GetSourceCommit(), ExactSourceMatch: prior.GetExactSourceMatch(),
		})
	}
	return result
}

func detectorTimelineContract(timeline []DetectorTimelineEntry) []*contracts.DetectorTimelineEntry {
	result := make([]*contracts.DetectorTimelineEntry, 0, len(timeline))
	for _, entry := range timeline {
		result = append(result, &contracts.DetectorTimelineEntry{
			DetectorId: entry.DetectorID, DetectorClass: entry.DetectorClass, Owner: entry.Owner, RuleId: entry.RuleID,
			Fingerprint: entry.Fingerprint, ParameterBindings: bindingsContract(entry.ParameterBindings),
			SurfacedPlaybookIds: append([]string{}, entry.SurfacedPlaybookIDs...), FirstActivatedAt: contractTimestamp(entry.FirstActivatedAt),
			LastSeenAt: contractTimestamp(entry.LastSeenAt), ClearedAt: optionalContractTimestamp(entry.ClearedAt),
			Relation: string(entry.Relation), Activations: int32(entry.Activations),
		})
	}
	return result
}

func detectorTimelineState(timeline []*contracts.DetectorTimelineEntry) []DetectorTimelineEntry {
	result := make([]DetectorTimelineEntry, 0, len(timeline))
	for _, entry := range timeline {
		item := DetectorTimelineEntry{
			DetectorID: entry.GetDetectorId(), DetectorClass: entry.GetDetectorClass(), Owner: entry.GetOwner(), RuleID: entry.GetRuleId(),
			Fingerprint: entry.GetFingerprint(), ParameterBindings: bindingsState(entry.GetParameterBindings()),
			SurfacedPlaybookIDs: append([]string(nil), entry.GetSurfacedPlaybookIds()...),
			FirstActivatedAt:    controllerTime(entry.GetFirstActivatedAt()), LastSeenAt: controllerTime(entry.GetLastSeenAt()),
			Relation: FiringRelation(entry.GetRelation()), Activations: int(entry.GetActivations()),
		}
		if entry.ClearedAt != nil {
			cleared := controllerTime(entry.GetClearedAt())
			item.ClearedAt = &cleared
		}
		result = append(result, item)
	}
	return result
}

func optionalTimeState(at *timestamppb.Timestamp) *time.Time {
	if at == nil {
		return nil
	}
	value := controllerTime(at)
	return &value
}

func incidentClosureState(message *contracts.IncidentClosure) IncidentClosure {
	closure := IncidentClosure{
		VerifiedWhileResponderRunning: message.GetVerifiedWhileResponderRunning(),
		ClosureStabilityWindow:        message.GetClosureStabilityWindow().AsDuration(),
		ObservationGeneration:         message.GetObservationGeneration(),
		DispatchError:                 message.GetDispatchError(), FinalDetectorStates: evaluationsState(message.GetFinalDetectorStates()),
		IncidentDetectorStates:              evaluationsState(message.GetIncidentDetectorStates()),
		PartialDetectorStates:               evaluationsState(message.GetPartialDetectorStates()),
		DetectorTimeline:                    detectorTimelineState(message.GetDetectorTimeline()),
		IncidentDetectorFiredBeforeDispatch: message.GetIncidentDetectorFiredBeforeDispatch(),
		IncidentDetectorFiredAfterDispatch:  message.GetIncidentDetectorFiredAfterDispatch(),
		NoIncidentDetectorFired:             message.GetNoIncidentDetectorFired(),
		FinalStateChanges:                   stateChangesState(message.GetFinalStateChanges()),
		DetectedAt:                          controllerTime(message.GetDetectedAt()), DispatchedAt: controllerTime(message.GetDispatchedAt()),
		ResponderCompletedAt: controllerTime(message.GetResponderCompletedAt()), VerifiedAt: controllerTime(message.GetVerifiedAt()),
		HealthClearedAt:   optionalTimeState(message.HealthClearedAt),
		CleanedHelpers:    append([]string(nil), message.GetCleanedHelpers()...),
		UnconfirmedReason: message.GetUnconfirmedReason(),
		UnconfirmedDetail: message.GetUnconfirmedDetail(),
		SourceCheckNotes:  append([]string(nil), message.GetSourceCheckNotes()...),
	}
	if request := incidentRequestState(message.GetRequest()); request != nil {
		closure.Request = *request
	}
	if message.GetResult() != nil {
		result := incidentResultState(message.GetResult())
		closure.Result = &result
	}
	return closure
}

func closureReceiptContract(receipt *ClosureReceipt) *contracts.ClosureReceipt {
	if receipt == nil {
		return nil
	}
	return &contracts.ClosureReceipt{
		IncidentId: receipt.IncidentID, Worktree: receipt.Worktree, BaseCommit: receipt.BaseCommit, ProposalCommit: receipt.ProposalCommit,
		OutcomeCommit: receipt.OutcomeCommit, ReflectionCommit: receipt.ReflectionCommit, AckToken: receipt.AckToken,
	}
}

func closureReceiptState(message *contracts.ClosureReceipt) *ClosureReceipt {
	if message == nil {
		return nil
	}
	return &ClosureReceipt{
		IncidentID: message.GetIncidentId(), Worktree: message.GetWorktree(), BaseCommit: message.GetBaseCommit(),
		ProposalCommit: message.GetProposalCommit(), OutcomeCommit: message.GetOutcomeCommit(),
		ReflectionCommit: message.GetReflectionCommit(), AckToken: message.GetAckToken(),
	}
}

func closureFailureContract(failure *ClosureFailure) *contracts.ClosureFailure {
	if failure == nil {
		return nil
	}
	return &contracts.ClosureFailure{
		IncidentId: failure.IncidentID, Attempts: int32(failure.Attempts), MaxAttempts: int32(failure.MaxAttempts),
		LastError: failure.LastError, FirstFailedAt: contractTimestamp(failure.FirstFailedAt),
		LastFailedAt: contractTimestamp(failure.LastFailedAt), NextAttemptAt: optionalTimestamp(failure.NextAttemptAt),
		Permanent: failure.Permanent, Action: failure.Action, Reason: failure.Reason,
	}
}

func closureFailureState(message *contracts.ClosureFailure) *ClosureFailure {
	if message == nil {
		return nil
	}
	return &ClosureFailure{
		IncidentID: message.GetIncidentId(), Attempts: int(message.GetAttempts()), MaxAttempts: int(message.GetMaxAttempts()),
		LastError: message.GetLastError(), FirstFailedAt: controllerTime(message.GetFirstFailedAt()),
		LastFailedAt: controllerTime(message.GetLastFailedAt()), NextAttemptAt: controllerTime(message.GetNextAttemptAt()),
		Permanent: message.GetPermanent(), Action: message.GetAction(), Reason: message.GetReason(),
	}
}

func incidentViewContract(view *IncidentView) (*contracts.IncidentView, error) {
	if view == nil {
		return nil, nil
	}
	blocking, err := findingsContract(view.BlockingFindings)
	if err != nil {
		return nil, fmt.Errorf("blocking findings: %w", err)
	}
	clearing, err := findingsContract(view.ClearingFindings)
	if err != nil {
		return nil, fmt.Errorf("clearing findings: %w", err)
	}
	return &contracts.IncidentView{
		IncidentId: view.IncidentID, ObservedAt: contractTimestamp(view.ObservedAt),
		BlockingDetectors: append([]string(nil), view.BlockingDetectors...), BlockingFindings: blocking,
		ClearingFindings: clearing, StateChanges: stateChangesContract(view.StateChanges), DriftEvents: driftEventsContract(view.DriftEvents),
	}, nil
}

func findingStatesContract(states map[string]FindingState) (map[string]*contracts.FindingState, error) {
	if len(states) == 0 {
		return nil, nil
	}
	result := make(map[string]*contracts.FindingState, len(states))
	for key, state := range states {
		finding, err := findingContract(state.Finding)
		if err != nil {
			return nil, fmt.Errorf("finding state %s: %w", key, err)
		}
		result[key] = &contracts.FindingState{
			DetectorId: state.DetectorID, Finding: finding, FiringCount: int32(state.FiringCount), ClearCount: int32(state.ClearCount),
			Active: state.Active, FirstSeenAt: optionalTimestamp(state.FirstSeenAt), Activations: int32(state.Activations),
			Clears: int32(state.Clears), NotPersisted: int32(state.NotPersisted),
			FirstClearAt: optionalTimestamp(state.FirstClearAt), LastObservedAt: optionalTimestamp(state.LastObservedAt),
		}
	}
	return result, nil
}

func findingStatesState(states map[string]*contracts.FindingState) map[string]FindingState {
	result := make(map[string]FindingState, len(states))
	for key, state := range states {
		result[key] = FindingState{
			DetectorID: state.GetDetectorId(), Finding: findingState(state.GetFinding()), FiringCount: int(state.GetFiringCount()),
			ClearCount: int(state.GetClearCount()), Active: state.GetActive(), FirstSeenAt: controllerTime(state.GetFirstSeenAt()),
			Activations: int(state.GetActivations()), Clears: int(state.GetClears()), NotPersisted: int(state.GetNotPersisted()),
			FirstClearAt: controllerTime(state.GetFirstClearAt()), LastObservedAt: controllerTime(state.GetLastObservedAt()),
		}
	}
	return result
}

// isPreCutoverFormat reports whether a strict ProtoJSON decode failed on a
// field the generated message does not define. Documents written before the
// ProtoJSON cutover used the Go structs' ad hoc encoding, whose extra and
// renamed keys surface this way; the check is a substring test on the decode
// error, not a legacy parser.
func isPreCutoverFormat(err error) bool {
	return err != nil && strings.Contains(err.Error(), "unknown field")
}

func rolloutExpectationContract(expectation *ControllerRolloutExpectation) *contracts.ControllerRolloutExpectation {
	if expectation == nil {
		return nil
	}
	return &contracts.ControllerRolloutExpectation{
		IncidentId: expectation.IncidentID, ReflectionCommit: expectation.ReflectionCommit,
		BeforeDetectorFingerprint: expectation.BeforeDetectorFingerprint, AfterDetectorFingerprint: expectation.AfterDetectorFingerprint,
	}
}

func rolloutExpectationState(message *contracts.ControllerRolloutExpectation) *ControllerRolloutExpectation {
	if message == nil {
		return nil
	}
	return &ControllerRolloutExpectation{
		IncidentID: message.GetIncidentId(), ReflectionCommit: message.GetReflectionCommit(),
		BeforeDetectorFingerprint: message.GetBeforeDetectorFingerprint(), AfterDetectorFingerprint: message.GetAfterDetectorFingerprint(),
	}
}

func rolloutReceiptContract(receipt ControllerRolloutReceipt) *contracts.ControllerRolloutReceipt {
	return &contracts.ControllerRolloutReceipt{
		Expectation: rolloutExpectationContract(&receipt.Expectation), BuildArtifact: receipt.BuildArtifact,
		ControllerRevision: receipt.ControllerRevision, ObservedDetectorFingerprint: receipt.ObservedDetectorFingerprint,
		Success: receipt.Success, VerifiedAt: contractTimestamp(receipt.VerifiedAt),
	}
}

func rolloutReceiptsState(receipts []*contracts.ControllerRolloutReceipt) []ControllerRolloutReceipt {
	result := make([]ControllerRolloutReceipt, 0, len(receipts))
	for _, receipt := range receipts {
		item := ControllerRolloutReceipt{
			BuildArtifact: receipt.GetBuildArtifact(), ControllerRevision: receipt.GetControllerRevision(),
			ObservedDetectorFingerprint: receipt.GetObservedDetectorFingerprint(), Success: receipt.GetSuccess(),
			VerifiedAt: controllerTime(receipt.GetVerifiedAt()),
		}
		if expectation := rolloutExpectationState(receipt.GetExpectation()); expectation != nil {
			item.Expectation = *expectation
		}
		result = append(result, item)
	}
	return result
}

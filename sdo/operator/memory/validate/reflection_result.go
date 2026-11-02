package validate

import (
	"context"
	"errors"
	"os"

	"fmt"
	"google.golang.org/protobuf/encoding/protojson"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// DecisionShape is the expected decision JSON. A malformed or missing decision
// error carries it, so one correction turn can fix the file.
const DecisionShape = `{"learning_decision": "updated | no_change | sdk_gap", "no_change_reason": "why nothing changed (no_change and sdk_gap only)", "proposed_artifact_ids": ["playbook-id for each changed playbook"]}`

// LoadReflectionResult preserves the full typed decision input, including
// fields which must be rejected before converting to publication metadata.
func LoadReflectionResult(ctx context.Context, root, path string) (*contracts.ReflectionResult, error) {
	if path == "" {
		var err error
		path, err = metadataPath(ctx, root, DecisionFile)
		if err != nil {
			return nil, err
		}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := new(contracts.ReflectionResult)
	if err := protojson.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("reflection decision is not valid JSON: %w; expected %s", err, DecisionShape)
	}
	return result, nil
}

// LoadCheckedReflectionDecision rejects fields that would be lost during
// projection, then leaves decision/file acceptance to the complete Check.
func LoadCheckedReflectionDecision(ctx context.Context, root, path string) (*contracts.ReflectionDecision, error) {
	result, err := LoadReflectionResult(ctx, root, path)
	if err != nil {
		return nil, err
	}
	if result != nil && result.GetArtifactProposal() != nil {
		return nil, Err(ReflectionResultDiagnostics(result))
	}
	return ReflectionDecisionFromResult(result), nil
}

// ReflectionDecisionFromResult is the single projection used after checking
// fields that cannot appear in the broker's publication decision.
func ReflectionDecisionFromResult(result *contracts.ReflectionResult) *contracts.ReflectionDecision {
	if result == nil {
		return nil
	}
	return &contracts.ReflectionDecision{
		LearningDecision: result.GetLearningDecision(), NoChangeReason: result.GetNoChangeReason(),
		PlaybookIds: result.GetProposedArtifactIds(), SdkGaps: result.GetSdkGaps(), DeclineReason: result.GetDeclineReason(),
	}
}

func reflectionResultFailure(message string) []Diagnostic {
	return []Diagnostic{{Rule: RuleReflectionDecision, Message: message, Hint: "return the same valid typed reflection decision checked locally"}}
}

// ReflectionDecisionContractDiagnostics keeps result consistency rules in the
// complete acceptance check, rather than only in the registered-worker adapter.
func ReflectionDecisionContractDiagnostics(decision *contracts.ReflectionDecision) []Diagnostic {
	if decision == nil {
		return nil
	} // The complete check diagnoses a missing decision.
	result := &contracts.ReflectionResult{
		LearningDecision: decision.GetLearningDecision(), NoChangeReason: decision.GetNoChangeReason(),
		ProposedArtifactIds: decision.GetPlaybookIds(), SdkGaps: decision.GetSdkGaps(), DeclineReason: decision.GetDeclineReason(),
	}
	if err := wire.Validate(result); err != nil {
		return reflectionResultFailure(fmt.Sprintf("reflection result contract: %v", err))
	}
	if result.GetLearningDecision() == "sdk_gap" && len(result.GetSdkGaps()) == 0 {
		return reflectionResultFailure("sdk_gap reflection requires a typed SDK gap")
	}
	if result.GetLearningDecision() == "no_change" && len(result.GetSdkGaps()) > 0 {
		return reflectionResultFailure("no-change reflection cannot report SDK gaps")
	}
	return nil
}

// ReflectionResultDiagnostics also checks fields lost when projecting the
// worker result to a publication decision. Both the CLI loader and worker edge
// must reject the retired content payload before that projection.
func ReflectionResultDiagnostics(result *contracts.ReflectionResult) []Diagnostic {
	if result == nil {
		return reflectionResultFailure("reflection requires a typed result")
	}
	diagnostics := ReflectionDecisionContractDiagnostics(ReflectionDecisionFromResult(result))
	if len(diagnostics) > 0 {
		return diagnostics
	}
	if result.GetArtifactProposal() != nil {
		if result.GetLearningDecision() == "sdk_gap" {
			return reflectionResultFailure("sdk_gap reflection cannot propose artifacts")
		}
		return reflectionResultFailure("direct-edit reflection rejects deprecated artifact_proposal")
	}
	return nil
}

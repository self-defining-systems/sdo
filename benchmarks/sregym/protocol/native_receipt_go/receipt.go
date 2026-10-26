// Package receipt owns benchmark receipt decisions. Production code does not
// import this module: the Python adapter invokes its CLI as a byte transport.
package receipt

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

// Evaluate converts either wire corruption or rejected evidence into data for
// exactly one incident. Build derives the flat summary before checking it;
// validate checks an existing receipt without repairing a conflicting summary.
func Evaluate(data []byte, build bool) *c.NativeReceiptResult {
	receipt := &c.NativeProductionReceipt{}
	result := &c.NativeReceiptResult{Receipt: receipt}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, receipt); err != nil {
		result.Failure = &c.ReceiptFailure{IncidentId: receipt.GetIncidentId(), Code: c.ReceiptFailure_CODE_INVALID_WIRE, Message: "native receipt is not valid ProtoJSON: " + err.Error()}
		return result
	}
	if build {
		summarize(receipt)
	}
	if err := validate(receipt); err != nil {
		result.Failure = &c.ReceiptFailure{IncidentId: receipt.GetIncidentId(), Code: c.ReceiptFailure_CODE_REJECTED_EVIDENCE, Message: err.Error()}
	}
	return result
}

// resolution labels how the incident closed. Only sdo_mitigated and
// cleared_without_sdo_action belong to a valid receipt; the SDO-owned labels of
// rejected closures keep analysis rows from counting them as external recovery.
func resolution(classification string) string {
	switch classification {
	case "success":
		return "sdo_mitigated"
	case "cleared_without_action":
		return "cleared_without_sdo_action"
	case "unconfirmed":
		return "sdo_unconfirmed"
	case "failed":
		return "sdo_failed"
	default:
		return "external_recovery"
	}
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func summarize(r *c.NativeProductionReceipt) {
	ledger := r.GetNativeLedger()
	r.ProposalCommit = optional(ledger.GetProposalCommit())
	r.OutcomeCommit = ledger.GetOutcomeCommit()
	r.ReflectionCommit = optional(ledger.GetReflectionCommit())
	r.ReflectionUsage = ledger.GetReflectionDecision().GetUsage()
	r.ReflectionSkippedReason = optional(ledger.GetReflectionSkippedReason())
	r.OutcomeClassification = ledger.GetOutcomeDocument().GetClassification()
	r.Resolution = resolution(r.OutcomeClassification)
}

func nonempty(value, name string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("native %s is missing", name)
	}
	return nil
}

func commit(value, name string, optional bool) error {
	if optional && value == "" {
		return nil
	}
	if !commitPattern.MatchString(value) {
		return fmt.Errorf("native %s must be a full Git commit", name)
	}
	return nil
}

func validate(r *c.NativeProductionReceipt) error {
	if r.GetSchemaVersion() != wire.StringConst(r, "schema_version") {
		return errors.New("invalid native production receipt schema_version")
	}
	if r.GetBrokerMode() != "native" || r.GetWorkerMountMode() != "proposal-only" {
		return errors.New("native receipt requires Go broker and proposal-only worker mounts")
	}
	for _, field := range []struct{ value, name string }{
		{r.GetIncidentId(), "incident ID"}, {r.GetNamespace(), "namespace"}, {r.GetControlNamespace(), "control namespace"},
		{r.GetControllerImage(), "controller_image"}, {r.GetResponderImage(), "responder_image"}, {r.GetValidatorImage(), "validator_image"}, {r.GetControllerPodUid(), "controller_pod_uid"},
	} {
		if err := nonempty(field.value, field.name); err != nil {
			return err
		}
	}
	id := r.GetIncidentId()
	l := r.GetNativeLedger()
	if l == nil {
		return errors.New("native broker ledger must be a JSON object")
	}
	if l.GetIncidentId() != id || !l.GetAcknowledged() {
		return errors.New("native broker ledger is not acknowledged for this incident")
	}
	closure := l.GetClosure()
	if closure == nil {
		return errors.New("native closure must be a JSON object")
	}
	request, response, outcome, accepted := closure.GetRequest(), closure.GetResult(), l.GetOutcomeDocument(), l.GetReceipt()
	if request == nil || response == nil || outcome == nil || accepted == nil {
		return errors.New("native closure request, responder result, outcome document and closure receipt must be JSON objects")
	}
	if request.GetIncidentId() != id || response.GetIncidentId() != id || outcome.GetIncidentId() != id || accepted.GetIncidentId() != id {
		return errors.New("native incident identities disagree")
	}
	if request.GetNamespace() != r.GetNamespace() {
		return errors.New("native closure namespace disagrees with receipt")
	}
	if request.GetRepairPolicy() != "commit" && request.GetRepairPolicy() != "recorded-actions" {
		return errors.New("native closure lacks valid repair policy")
	}
	classification := outcome.GetClassification()
	if outcome.GetSchemaVersion() != 1 || !slices.Contains([]string{"success", "cleared_without_action"}, classification) {
		return errors.New("native outcome is not a verified closure classification")
	}
	for _, field := range []struct {
		value, name string
		optional    bool
	}{
		{l.GetProposalCommit(), "proposal commit", true}, {l.GetOutcomeCommit(), "outcome commit", false}, {l.GetReflectionCommit(), "reflection commit", true}, {outcome.GetRepairCommit(), "outcome repair commit", true},
		{accepted.GetProposalCommit(), "closure receipt proposal_commit", true}, {accepted.GetOutcomeCommit(), "closure receipt outcome_commit", false}, {accepted.GetReflectionCommit(), "closure receipt reflection_commit", true},
	} {
		if err := commit(field.value, field.name, field.optional); err != nil {
			return err
		}
	}
	if outcome.GetRepairCommit() != l.GetProposalCommit() {
		return errors.New("native outcome repair commit disagrees with broker proposal")
	}
	if accepted.GetProposalCommit() != l.GetProposalCommit() || accepted.GetOutcomeCommit() != l.GetOutcomeCommit() || accepted.GetReflectionCommit() != l.GetReflectionCommit() {
		return errors.New("native closure receipt commits disagree with broker ledger")
	}
	if err := nonempty(accepted.GetAckToken(), "acknowledgment token"); err != nil {
		return err
	}
	if len(closure.GetFinalDetectorStates()) == 0 {
		return errors.New("native closure lacks independently clear health detectors")
	}
	for _, state := range closure.GetFinalDetectorStates() {
		if state.GetStatus() != "clear" || len(state.GetFingerprints()) != 0 {
			return errors.New("native closure lacks independently clear health detectors")
		}
	}
	if closure.GetVerifiedAt() == nil || closure.GetVerifiedAt().CheckValid() != nil {
		return errors.New("native verified_at must be an ISO timestamp")
	}
	decision := l.GetReflectionDecision()
	if l.GetReflectionSkippedReason() != "" && (strings.TrimSpace(l.GetReflectionSkippedReason()) == "" || l.GetRequiresReflection() || decision != nil) {
		return errors.New("native reflection skip conflicts with broker decision")
	}
	if l.GetRequiresReflection() && (!l.GetReflectionProcessed() || decision == nil || (decision.GetLearningDecision() == "updated" && l.GetReflectionCommit() == "")) {
		return errors.New("native required reflection is incomplete")
	}
	if decision.GetLearningDecision() == "failed" {
		if err := nonempty(decision.GetError(), "reflection failure error"); err != nil {
			return err
		}
		if l.GetReflectionCommit() != "" {
			return errors.New("native failed reflection cannot have a reflection commit")
		}
	}
	if expected := l.GetRolloutExpectation(); expected != nil {
		if expected.GetIncidentId() != id || expected.GetReflectionCommit() != l.GetReflectionCommit() {
			return errors.New("native rollout expectation is not bound to the incident")
		}
		if err := nonempty(expected.GetBeforeDetectorFingerprint(), "prior detector fingerprint"); err != nil {
			return err
		}
		if err := nonempty(expected.GetAfterDetectorFingerprint(), "new detector fingerprint"); err != nil {
			return err
		}
		if expected.GetBeforeDetectorFingerprint() == expected.GetAfterDetectorFingerprint() {
			return errors.New("native rollout did not change detector fingerprint")
		}
		verified := []*c.ControllerRolloutReceipt{}
		for _, attempt := range l.GetRolloutReceipts() {
			if attempt.GetSuccess() {
				verified = append(verified, attempt)
			}
		}
		if len(verified) != 1 || !proto.Equal(verified[0].GetExpectation(), expected) || verified[0].GetObservedDetectorFingerprint() != expected.GetAfterDetectorFingerprint() {
			return errors.New("native rollout lacks one matching verified receipt")
		}
		for _, field := range []struct{ value, name string }{{verified[0].GetBuildArtifact(), "rollout build_artifact"}, {verified[0].GetControllerRevision(), "rollout controller_revision"}} {
			if err := nonempty(field.value, field.name); err != nil {
				return err
			}
		}
		if verified[0].GetVerifiedAt() == nil || verified[0].GetVerifiedAt().CheckValid() != nil {
			return errors.New("native rollout verified_at must be an ISO timestamp")
		}
	}
	jobs := r.GetResponderJobEvidence()
	if len(jobs) == 0 {
		// A controller may intentionally retire the Job after an early report.
		// Validate its durable lifecycle, frozen request/result and broker ACK,
		// rather than inventing a successful Kubernetes Job.
		dispatched, completed := closure.GetDispatchedAt(), closure.GetResponderCompletedAt()
		if dispatched == nil || dispatched.CheckValid() != nil || completed == nil || completed.CheckValid() != nil || completed.AsTime().Before(dispatched.AsTime()) || response.GetStatus() != "completed" {
			return errors.New("native receipt lacks durable responder dispatch and completion")
		}
	} else {
		if len(jobs) != 1 || jobs[0].GetIncidentId() != id || !jobs[0].GetSucceeded() {
			return errors.New("native receipt requires one correlated successful responder Job")
		}
		if err := nonempty(jobs[0].GetJobUid(), "responder Job UID"); err != nil {
			return err
		}
		if err := nonempty(jobs[0].GetJobName(), "responder Job name"); err != nil {
			return err
		}
	}
	if r.GetOutcomeCommit() != l.GetOutcomeCommit() || r.GetReflectionCommit() != l.GetReflectionCommit() || r.GetProposalCommit() != l.GetProposalCommit() {
		return errors.New("native flat commit summary disagrees with ledger")
	}
	if r.GetResolution() != resolution(classification) {
		return errors.New("native resolution disagrees with independently derived outcome")
	}
	return nil
}

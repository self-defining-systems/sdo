package outcome_derivation

import (
	"fmt"
	"sort"
	"strings"
	"time"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func verifyDiagnosis(closure *contractsv1alpha1.IncidentClosure) []*contractsv1alpha1.DiagnosisVerification {
	result := closure.GetResult()
	if result == nil {
		return nil
	}
	request := closure.GetRequest()
	fired := map[string]bool{}
	findings := map[string]bool{}
	scenarios := map[string]bool{}
	for _, finding := range request.GetFindings() {
		if finding.GetDetectorId() != "" {
			fired[finding.GetDetectorId()] = fired[finding.GetDetectorId()] || finding.GetStatus() == "active"
		}
		if finding.GetFingerprint() != "" {
			findings[finding.GetFingerprint()] = findings[finding.GetFingerprint()] || finding.GetStatus() == "active"
		}
		if ref := finding.GetPrimaryResource(); ref.GetName() != "" {
			qualified := strings.Join([]string{finding.GetDetectorId(), finding.GetRuleId(), ref.GetNamespace(), ref.GetName()}, "/")
			findings[qualified] = findings[qualified] || finding.GetStatus() == "active"
		}
		if strings.HasPrefix(finding.GetRuleId(), "scenario-slo.") {
			name := strings.TrimPrefix(finding.GetRuleId(), "scenario-slo.")
			scenarios[name] = scenarios[name] || finding.GetStatus() == "active"
		}
	}
	for _, evaluation := range request.GetDetectorHistory() {
		for _, fingerprint := range evaluation.GetFingerprints() {
			findings[fingerprint] = findings[fingerprint] || evaluation.GetStatus() == "firing"
		}
		if evaluation.GetStatus() == "firing" {
			fired[evaluation.GetDetectorId()] = true
		} else if evaluation.GetDetectorId() != "" {
			if _, ok := fired[evaluation.GetDetectorId()]; !ok {
				fired[evaluation.GetDetectorId()] = false
			}
		}
	}
	// A detector that activates while the responder is still receiving its
	// settled diagnosis observed the incident before any repair could begin.
	if closure.GetDispatchedAt() != nil {
		cutoff := closure.GetDispatchedAt().AsTime().Add(15 * time.Second)
		for _, call := range result.GetProgramToolCalls() {
			if call.GetAt() != nil && call.GetAt().AsTime().After(cutoff) {
				cutoff = call.GetAt().AsTime()
			}
			break
		}
		for _, action := range result.GetRepairActions() {
			if action.GetStartedAt() != nil && action.GetStartedAt().AsTime().Before(cutoff) {
				cutoff = action.GetStartedAt().AsTime()
			}
		}
		for _, entry := range closure.GetDetectorTimeline() {
			if entry.GetFirstActivatedAt() == nil || entry.GetFirstActivatedAt().AsTime().After(cutoff) {
				continue
			}
			if entry.GetDetectorId() != "" {
				fired[entry.GetDetectorId()] = true
			}
			if entry.GetFingerprint() != "" {
				findings[entry.GetFingerprint()] = true
			}
		}
	}
	latest := map[string]*contractsv1alpha1.DetectorEvaluation{}
	evaluations := append(append([]*contractsv1alpha1.DetectorEvaluation{}, closure.GetFinalDetectorStates()...), closure.GetIncidentDetectorStates()...)
	sort.SliceStable(evaluations, func(i, j int) bool {
		return evaluations[i].GetEvaluatedAt().AsTime().Before(evaluations[j].GetEvaluatedAt().AsTime())
	})
	for _, evaluation := range evaluations {
		latest[evaluation.GetDetectorId()] = evaluation
	}
	changes := newStateEvidence(closure)
	repairs := newRepairs(closure)
	verifications := make([]*contractsv1alpha1.DiagnosisVerification, 0, len(result.GetConfirmedRootCauses()))
	for _, cause := range result.GetConfirmedRootCauses() {
		checks := make([]*contractsv1alpha1.OutcomeEvidenceCheck, 0, len(cause.GetEvidence()))
		for _, evidence := range cause.GetEvidence() {
			check := &contractsv1alpha1.OutcomeEvidenceCheck{Kind: evidence.GetKind(), Source: evidence.GetSource()}
			switch evidence.GetKind() {
			case "detector-finding":
				if fired[evidence.GetSource()] || findings[evidence.GetSource()] {
					check.Verified = boolPointer(true)
				} else if _, ok := fired[evidence.GetSource()]; ok {
					check.Verified = boolPointer(false)
				} else if _, ok := findings[evidence.GetSource()]; ok {
					check.Verified = boolPointer(false)
				}
			case "synthetic-traffic":
				if observed, ok := scenarios[strings.TrimPrefix(evidence.GetSource(), "scenario-slo.")]; ok {
					check.Verified = boolPointer(observed)
				}
			case "state-change":
				check.Verified, check.Reason = changes.check(evidence.GetSource())
			}
			checks = append(checks, check)
		}
		flips := make([]*contractsv1alpha1.OutcomeDetectorFlip, 0, len(cause.GetExplainedDetectors()))
		for _, detectorID := range cause.GetExplainedDetectors() {
			var clear *bool
			if evaluation, ok := latest[detectorID]; ok {
				clear = boolPointer(evaluation.GetStatus() == "clear")
			}
			flips = append(flips, &contractsv1alpha1.OutcomeDetectorFlip{DetectorId: detectorID, FiredAtDispatch: fired[detectorID], ClearedAfterFix: clear, Flipped: fired[detectorID] && clear != nil && *clear})
		}
		repair := repairs.attribute(cause)
		programCheck := programFlipCheck(closure, cause)
		fieldCheck := restoredFieldCheck(closure, cause, repair)
		if programCheck != nil {
			checks = append(checks, programCheck)
		}
		if fieldCheck != nil {
			checks = append(checks, fieldCheck)
		}
		verdict := "unverified"
		switch {
		case len(cause.GetEvidence()) == 0:
			verdict = "no-evidence"
		case anyContradicted(checks):
			verdict = "contradicted"
		case anyFlipped(flips) || programCheck != nil || fieldCheck != nil:
			if repair.Attributed {
				verdict = "confirmed"
			} else {
				verdict = "unattributed"
			}
		}
		verifications = append(verifications, &contractsv1alpha1.DiagnosisVerification{Summary: cause.GetSummary(), Verdict: verdict, Evidence: checks, Detectors: flips, Repair: repair})
	}
	return verifications
}

func programFlipCheck(closure *contractsv1alpha1.IncidentClosure, cause *contractsv1alpha1.ConfirmedRootCause) *contractsv1alpha1.OutcomeEvidenceCheck {
	if closure.GetVerifiedAt() == nil || !allFinalDetectorsClear(closure) {
		return nil
	}
	for _, before := range closure.GetRequest().GetInitialProgramRuns() {
		if before.GetVerdict().GetResult() != contractsv1alpha1.Verdict_RESULT_MATCH || !bindingsTouchCause(before.GetBindings(), cause) {
			continue
		}
		for _, after := range closure.GetResult().GetProgramRuns() {
			if after.GetProgramId() != before.GetProgramId() || after.GetVerdict().GetResult() != contractsv1alpha1.Verdict_RESULT_NO_MATCH ||
				before.GetProgramRevision() != after.GetProgramRevision() || !sameBindings(before.GetBindings(), after.GetBindings()) {
				continue
			}
			return &contractsv1alpha1.OutcomeEvidenceCheck{
				Kind: "program-verdict", Source: before.GetProgramId(), Verified: boolPointer(true),
				Reason: stringPointer("the same program and binding diagnosed Match before repair and NoMatch after repair; the controller verified the fixed state"),
			}
		}
	}
	return nil
}

func sameBindings(left, right []*contractsv1alpha1.Binding) bool {
	if len(left) != len(right) {
		return false
	}
	byName := make(map[string]objectKey, len(left))
	for _, binding := range left {
		byName[binding.GetName()] = keyOf(binding.GetObject())
	}
	if len(byName) != len(left) {
		return false
	}
	for _, binding := range right {
		if byName[binding.GetName()] != keyOf(binding.GetObject()) {
			return false
		}
	}
	return true
}

func bindingsTouchCause(bindings []*contractsv1alpha1.Binding, cause *contractsv1alpha1.ConfirmedRootCause) bool {
	for _, binding := range bindings {
		for _, resource := range cause.GetResources() {
			if keyOf(binding.GetObject()).matches(keyOf(resource)) {
				return true
			}
		}
	}
	return false
}

func allFinalDetectorsClear(closure *contractsv1alpha1.IncidentClosure) bool {
	if len(closure.GetFinalDetectorStates()) == 0 || closure.GetDetectorReviewRequiredAt() != nil {
		return false
	}
	for _, evaluation := range closure.GetFinalDetectorStates() {
		if evaluation.GetStatus() != "clear" {
			return false
		}
	}
	return true
}

func restoredFieldCheck(closure *contractsv1alpha1.IncidentClosure, cause *contractsv1alpha1.ConfirmedRootCause, repair *contractsv1alpha1.OutcomeRepairAttribution) *contractsv1alpha1.OutcomeEvidenceCheck {
	if !repair.GetAttributed() || !allFinalDetectorsClear(closure) || closure.GetVerifiedAt() == nil ||
		closure.GetRequest().GetStateChanges() == nil || closure.GetFinalStateChanges() == nil || closure.GetFinalStateChanges().GetObservedAt() == nil {
		return nil
	}
	if closure.GetFinalStateChanges().GetObservedAt().AsTime().After(closure.GetVerifiedAt().AsTime()) {
		return nil
	}
	for _, before := range closure.GetRequest().GetStateChanges().GetChanges() {
		if before.GetChange() != "modified" || len(before.GetFields()) == 0 {
			continue
		}
		changedField := false
		for _, field := range before.GetFields() {
			if field.Before != nil && field.After != nil && field.GetBefore() != field.GetAfter() {
				changedField = true
				break
			}
		}
		if !changedField {
			continue
		}
		key := objectKey{kind: canonicalKind(before.GetKind()), name: before.GetName()}
		cited := false
		for _, evidence := range cause.GetEvidence() {
			if evidence.GetKind() == "state-change" {
				if source, ok := parseKey(evidence.GetSource()); ok && source.matches(key) {
					cited = true
				}
			}
		}
		if !cited {
			continue
		}
		for _, after := range closure.GetFinalStateChanges().GetChanges() {
			if canonicalKind(after.GetKind()) == key.kind && after.GetName() == key.name {
				return nil
			}
		}
		for _, action := range closure.GetResult().GetRepairActions() {
			if !action.GetSuccess() || action.GetCompletedAt() == nil || action.GetCompletedAt().AsTime().After(closure.GetFinalStateChanges().GetObservedAt().AsTime()) {
				continue
			}
			for _, touched := range actionKeys(action) {
				if touched.matches(key) {
					return &contractsv1alpha1.OutcomeEvidenceCheck{
						Kind: "baseline-restoration", Source: before.GetKind() + "/" + before.GetName(), Verified: boolPointer(true),
						Reason: stringPointer("the controller recorded changed fields before repair and observed the repaired object back at its healthy baseline after the successful action"),
					}
				}
			}
		}
	}
	return nil
}

func boolPointer(value bool) *bool       { return &value }
func stringPointer(value string) *string { return &value }
func anyContradicted(items []*contractsv1alpha1.OutcomeEvidenceCheck) bool {
	for _, item := range items {
		if item.Verified != nil && !*item.Verified {
			return true
		}
	}
	return false
}
func anyFlipped(items []*contractsv1alpha1.OutcomeDetectorFlip) bool {
	for _, item := range items {
		if item.Flipped {
			return true
		}
	}
	return false
}

type objectKey struct{ kind, name, namespace, label string }

var aliases = map[string]string{
	"cm": "configmap", "configmaps": "configmap", "cronjobs": "cronjob", "cj": "cronjob", "daemonsets": "daemonset", "deploy": "deployment", "deployments": "deployment", "ds": "daemonset", "hpa": "horizontalpodautoscaler", "horizontalpodautoscalers": "horizontalpodautoscaler", "ing": "ingress", "ingresses": "ingress", "jobs": "job", "netpol": "networkpolicy", "networkpolicies": "networkpolicy", "persistentvolumeclaims": "persistentvolumeclaim", "po": "pod", "pods": "pod", "pvc": "persistentvolumeclaim", "replicasets": "replicaset", "rolebindings": "rolebinding", "roles": "role", "rs": "replicaset", "sa": "serviceaccount", "secrets": "secret", "serviceaccounts": "serviceaccount", "services": "service", "statefulsets": "statefulset", "sts": "statefulset", "svc": "service",
}
var knownKinds = map[string]bool{"service": true, "secret": true, "role": true, "rolebinding": true}
var podOwners = map[string]bool{"deployment": true, "statefulset": true, "daemonset": true, "replicaset": true, "job": true}

func canonicalKind(raw string) string {
	kind := strings.SplitN(strings.ToLower(strings.TrimSpace(raw)), ".", 2)[0]
	if alias, ok := aliases[kind]; ok {
		return alias
	}
	return kind
}
func knownKind(raw string) bool {
	kind := canonicalKind(raw)
	if knownKinds[kind] {
		return true
	}
	for _, v := range aliases {
		if v == kind {
			return true
		}
	}
	return false
}
func keyOf(ref *contractsv1alpha1.ObjectRef) objectKey {
	return objectKey{canonicalKind(ref.GetKind()), ref.GetName(), ref.GetNamespace(), ref.GetKind() + "/" + ref.GetName()}
}
func parseKey(source string) (objectKey, bool) {
	// A compound citation such as "Service/a; Service/b" names several objects,
	// so it is no object reference rather than a namespace containing punctuation.
	if strings.ContainsAny(source, ";,") || len(strings.Fields(source)) > 1 {
		return objectKey{}, false
	}
	parts := []string{}
	for _, part := range strings.Split(strings.TrimSpace(source), "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	if len(parts) == 2 {
		return objectKey{canonicalKind(parts[0]), parts[1], "", parts[0] + "/" + parts[1]}, true
	}
	if len(parts) == 3 {
		if knownKind(parts[0]) {
			return objectKey{canonicalKind(parts[0]), parts[2], parts[1], parts[0] + "/" + parts[2]}, true
		}
		if knownKind(parts[1]) {
			return objectKey{canonicalKind(parts[1]), parts[2], parts[0], parts[1] + "/" + parts[2]}, true
		}
	}
	return objectKey{}, false
}
func (key objectKey) matches(other objectKey) bool {
	if key.namespace != "" && other.namespace != "" && key.namespace != other.namespace {
		return false
	}
	if key.kind == other.kind {
		return key.name == other.name
	}
	return owns(key, other) || owns(other, key)
}
func owns(owner, owned objectKey) bool {
	return podOwners[owner.kind] && (owned.kind == "pod" || owned.kind == "replicaset") && strings.HasPrefix(owned.name, owner.name+"-")
}
func actionKeys(action *contractsv1alpha1.RepairActionReceipt) []objectKey {
	if len(action.GetResources()) > 0 {
		keys := make([]objectKey, 0, len(action.GetResources()))
		for _, ref := range action.GetResources() {
			keys = append(keys, keyOf(ref))
		}
		return keys
	}
	keys := []objectKey{}
	for _, token := range strings.Fields(strings.ReplaceAll(action.GetTarget(), ",", " ")) {
		if key, ok := parseKey(token); ok {
			keys = append(keys, key)
		}
	}
	return keys
}
func diffKeys(changes *contractsv1alpha1.StateChanges) []objectKey {
	keys := []objectKey{}
	if changes == nil {
		return keys
	}
	for _, change := range changes.GetChanges() {
		keys = append(keys, objectKey{canonicalKind(change.GetKind()), change.GetName(), "", change.GetKind() + "/" + change.GetName()})
	}
	return keys
}
func keyLabels(keys []objectKey) map[string]bool {
	labels := map[string]bool{}
	for _, key := range keys {
		labels[key.label] = true
	}
	return labels
}
func keyID(key objectKey) string { return key.kind + "/" + key.name }
func sortedUnique(items []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, item := range items {
		if !seen[item] {
			out = append(out, item)
			seen[item] = true
		}
	}
	sort.Strings(out)
	return out
}

type actionWithKeys struct {
	receipt *contractsv1alpha1.RepairActionReceipt
	keys    []objectKey
}
type stateEvidence struct {
	dispatch  map[string]bool
	changed   map[string]bool
	observed  map[string]time.Time
	actions   []actionWithKeys
	checkable bool
}

func newStateEvidence(closure *contractsv1alpha1.IncidentClosure) stateEvidence {
	dispatch := keyLabels(diffKeys(closure.GetRequest().GetStateChanges()))
	changed := keyLabels(diffKeys(closure.GetFinalStateChanges()))
	for label := range dispatch {
		changed[label] = true
	}
	var observed map[string]time.Time
	if closure.GetObservedStateChanges() != nil {
		observed = map[string]time.Time{}
		for _, item := range closure.GetObservedStateChanges() {
			label := item.GetKind() + "/" + item.GetName()
			seen := item.GetFirstObservedAt().AsTime()
			if prior, ok := observed[label]; !ok || seen.Before(prior) {
				observed[label] = seen
			}
		}
	}
	actions := []actionWithKeys{}
	for _, receipt := range closure.GetResult().GetRepairActions() {
		actions = append(actions, actionWithKeys{receipt, actionKeys(receipt)})
	}
	return stateEvidence{dispatch, changed, observed, actions, closure.GetRequest().GetStateChanges() != nil || closure.GetFinalStateChanges() != nil || observed != nil}
}
func (e stateEvidence) check(source string) (*bool, *string) {
	if !e.checkable {
		return nil, nil
	}
	if e.dispatch[source] {
		return boolPointer(true), nil
	}
	if e.observed == nil {
		return boolPointer(e.changed[source]), nil
	}
	first, ok := e.observed[source]
	if !ok {
		return boolPointer(false), stringPointer("the controller never observed this object change while the incident was open")
	}
	cited, valid := parseKey(source)
	type start struct {
		at time.Time
		id string
	}
	starts := []start{}
	for _, action := range e.actions {
		for _, key := range action.keys {
			if valid && cited.matches(key) {
				starts = append(starts, start{action.receipt.GetStartedAt().AsTime(), action.receipt.GetActionId()})
				break
			}
		}
	}
	sort.Slice(starts, func(i, j int) bool {
		if starts[i].at.Equal(starts[j].at) {
			return starts[i].id < starts[j].id
		}
		return starts[i].at.Before(starts[j].at)
	})
	if len(starts) > 0 && !first.Before(starts[0].at) {
		return boolPointer(false), stringPointer(fmt.Sprintf("first observed at %s, after the responder's own repair %s of it started at %s: the change is that repair, not the cause", first.Format(time.RFC3339Nano), starts[0].id, starts[0].at.Format(time.RFC3339Nano)))
	}
	return boolPointer(true), nil
}

type repairs struct {
	actions         []actionWithKeys
	dispatch        []objectKey
	reverted        []string
	healthClearedAt *time.Time
}

func repairClearAt(closure *contractsv1alpha1.IncidentClosure) *time.Time {
	var cleared *time.Time
	if closure.GetHealthClearedAt() != nil {
		value := closure.GetHealthClearedAt().AsTime()
		cleared = &value
	}
	earlierHealthClear := cleared != nil && closure.GetDispatchedAt() != nil && cleared.Before(closure.GetDispatchedAt().AsTime())
	if earlierHealthClear {
		cleared = nil
	}
	// A latent fault can outlast symptom health even when health clears during
	// response. Its own independently observed clear bounds repair attribution.
	for _, state := range closure.GetIncidentDetectorStates() {
		if earlierHealthClear || strings.HasPrefix(state.GetDetectorId(), "drift.") {
			if state.GetStatus() != "clear" || state.GetEvaluatedAt() == nil {
				continue
			}
			at := state.GetEvaluatedAt().AsTime()
			if closure.GetDispatchedAt() != nil && at.Before(closure.GetDispatchedAt().AsTime()) {
				continue
			}
			if cleared == nil || at.After(*cleared) {
				cleared = &at
			}
		}
	}
	if cleared == nil && closure.GetVerifiedAt() != nil {
		at := closure.GetVerifiedAt().AsTime()
		cleared = &at
	}
	return cleared
}

func newRepairs(closure *contractsv1alpha1.IncidentClosure) repairs {
	cleared := repairClearAt(closure)
	actions := []actionWithKeys{}
	for _, receipt := range closure.GetResult().GetRepairActions() {
		if receipt.GetSuccess() && (cleared == nil || !receipt.GetStartedAt().AsTime().After(*cleared)) {
			actions = append(actions, actionWithKeys{receipt, actionKeys(receipt)})
		}
	}
	dispatch := diffKeys(closure.GetRequest().GetStateChanges())
	reverted := []string{}
	if closure.GetRequest().GetStateChanges() != nil && closure.GetFinalStateChanges() != nil {
		remaining := map[string]bool{}
		for _, key := range diffKeys(closure.GetFinalStateChanges()) {
			remaining[keyID(key)] = true
		}
		for _, change := range dispatch {
			if remaining[keyID(change)] {
				continue
			}
			touched := false
			for _, action := range actions {
				for _, key := range action.keys {
					if key.matches(change) {
						touched = true
						break
					}
				}
			}
			if !touched {
				reverted = append(reverted, change.label)
			}
		}
	}
	return repairs{actions, dispatch, sortedUnique(reverted), cleared}
}
func (r repairs) attribute(cause *contractsv1alpha1.ConfirmedRootCause) *contractsv1alpha1.OutcomeRepairAttribution {
	blamed := []objectKey{}
	for _, ref := range cause.GetResources() {
		blamed = append(blamed, keyOf(ref))
	}
	for _, item := range cause.GetEvidence() {
		if item.GetKind() == "state-change" {
			if key, ok := parseKey(item.GetSource()); ok {
				blamed = append(blamed, key)
			}
		}
	}
	actions := []string{}
	repaired := []objectKey{}
	for _, action := range r.actions {
		hit := false
		for _, key := range blamed {
			for _, touched := range action.keys {
				if key.matches(touched) {
					hit = true
					unique := true
					for _, prior := range repaired {
						if prior == key {
							unique = false
							break
						}
					}
					if unique {
						repaired = append(repaired, key)
					}
					break
				}
			}
		}
		if hit {
			actions = append(actions, action.receipt.GetActionId())
		}
	}
	labels := []string{}
	for _, key := range repaired {
		labels = append(labels, key.label)
	}
	labels = sortedUnique(labels)
	attribution := &contractsv1alpha1.OutcomeRepairAttribution{Attributed: false, Actions: actions, RepairedResources: labels, ExternallyReverted: r.reverted}
	if len(actions) == 0 {
		names := []string{}
		for _, key := range blamed {
			names = append(names, key.label)
		}
		names = sortedUnique(names)
		named := strings.Join(names, ", ")
		if named == "" {
			named = "none recorded"
		}
		timing := ""
		if r.healthClearedAt != nil {
			timing = " that started before health cleared at " + r.healthClearedAt.Format(time.RFC3339Nano)
		}
		attribution.Reason = fmt.Sprintf("the responder's successful repair actions%s touched none of the cause's resources (%s)", timing, named)
		return attribution
	}
	changed := []string{}
	for _, key := range repaired {
		for _, change := range r.dispatch {
			if key.kind == change.kind && key.name == change.name {
				changed = append(changed, key.label)
				break
			}
		}
	}
	changed = sortedUnique(changed)
	if len(changed) > 0 {
		attribution.Attributed = true
		attribution.Reason = "the responder repaired " + strings.Join(changed, ", ") + ", changed since the healthy baseline"
		return attribution
	}
	if len(r.reverted) > 0 {
		attribution.Reason = "health recovered while baseline changes no responder repair touched were reverted: " + strings.Join(r.reverted, ", ") + "; the repair of " + strings.Join(labels, ", ") + " does not explain the recovery"
		return attribution
	}
	attribution.Attributed = true
	attribution.Reason = "the responder repaired " + strings.Join(labels, ", ") + " before health cleared"
	return attribution
}

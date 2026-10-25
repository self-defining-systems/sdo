package outcome_derivation

import (
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
)

// CheckedDriftAssessments overlays dispositions only onto controller-owned
// observations. Old values and timestamps do not define observation identity.
func CheckedDriftAssessments(observed, labels []*contracts.DriftEvent) []*contracts.DriftEvent {
	checked := []*contracts.DriftEvent{}
	for _, label := range labels {
		if label == nil || strings.TrimSpace(label.GetInvestigationReason()) == "" {
			continue
		}
		verdict := label.GetInvestigationVerdict()
		if verdict != "bug" && verdict != "benign" && verdict != "unsure" {
			continue
		}
		source, relation := false, false
		valid := len(label.GetInvestigationEvidence()) > 0 && len(label.GetInvestigationEvidence()) <= 8
		for _, e := range label.GetInvestigationEvidence() {
			if strings.TrimSpace(e.GetObservation()) == "" {
				valid = false
			}
			switch e.GetKind() {
			case "source_contradiction":
				source = true
			case "violated_relation", "observed_impact":
				relation = true
			default:
				valid = false
			}
		}
		if !valid || verdict == "bug" && (!source || !relation) {
			continue
		}
		for _, event := range observed {
			if !sameDriftObservation(event, label) {
				continue
			}
			copy := proto.Clone(event).(*contracts.DriftEvent)
			copy.InvestigationStatus = "completed"
			copy.InvestigationVerdict = verdict
			copy.InvestigationReason = label.GetInvestigationReason()
			copy.InvestigationEvidence = cloneMessages(label.GetInvestigationEvidence())
			checked = append(checked, copy)
			break
		}
	}
	return checked
}

func sameDriftObservation(a, b *contracts.DriftEvent) bool {
	if a == nil || b == nil || !proto.Equal(a.GetRef(), b.GetRef()) || a.GetManager() != b.GetManager() || len(a.GetFields()) != len(b.GetFields()) {
		return false
	}
	fields := map[string]*contracts.DriftField{}
	for _, f := range a.GetFields() {
		fields[f.GetPath()] = f
	}
	if len(fields) != len(a.GetFields()) {
		return false
	}
	seen := map[string]bool{}
	for _, f := range b.GetFields() {
		old, ok := fields[f.GetPath()]
		if !ok || seen[f.GetPath()] || old.GetNew() != f.GetNew() || old.GetSource() != f.GetSource() {
			return false
		}
		seen[f.GetPath()] = true
	}
	return true
}

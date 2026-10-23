package replay

import (
	"slices"

	"sdo.dev/operator/sdk"
)

func healthImpact(c Case) bool {
	return c.Corpus.HealthImpact == nil || *c.Corpus.HealthImpact
}

func detectorFamilyMatches(d sdk.Detector, c Case) bool {
	for _, id := range d.Spec().Playbooks {
		if slices.Contains(c.Corpus.RootCauseProgramIDs, id) {
			return true
		}
	}
	return false
}

func (r *Result) incidentFP(trigger string) {
	r.IncidentFalsePositives = append(r.IncidentFalsePositives, trigger)
	r.FalsePositives = append(r.FalsePositives, trigger)
}

func (r *Result) hasFailures() bool {
	return len(r.Errors)+len(r.FalsePositives)+len(r.HealthFalseNegatives)+len(r.IncidentFalseNegatives) > 0
}

// Class is the existing manifest-backed role checked by the builder's generated
// registration contracts. Family links govern incident detectors, not health.
func (r *Result) scoreDetector(phase string, row DetectorResult, familyMatch bool, c Case) {
	trigger := phase + ":detector:" + row.ID
	if row.Class != sdk.DetectorClassHealth && row.Class != sdk.DetectorClassIncident {
		r.Errors = append(r.Errors, trigger+": invalid detector class")
		return
	}
	if row.Error != "" {
		r.Errors = append(r.Errors, trigger+": "+row.Error)
		return
	}
	if row.Class == sdk.DetectorClassHealth {
		if phase == "healthy" && row.Active > 0 {
			r.HealthFalsePositives = append(r.HealthFalsePositives, trigger)
			r.FalsePositives = append(r.FalsePositives, trigger)
		}
		if phase == "faulted" {
			if row.Active == 0 && healthImpact(c) {
				r.HealthFalseNegatives = append(r.HealthFalseNegatives, trigger)
			}
			if row.Active > 0 && !familyMatch {
				r.Overlap = append(r.Overlap, trigger)
			}
		}
		return
	}
	if row.Active > 0 && (phase == "healthy" || !familyMatch) {
		r.incidentFP(trigger)
	}
	if phase == "faulted" && row.Active == 0 && familyMatch {
		r.IncidentFalseNegatives = append(r.IncidentFalseNegatives, trigger)
	}
}

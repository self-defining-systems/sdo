package runtime

import (
	"context"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

// creditLearnedPrograms gives the first replayed repair its origin credit even
// though that program did not exist during the scratch repair. Only broker-
// validated reflection commits and controller-verified cause resources count.
func creditLearnedPrograms(ctx context.Context, request IncidentRequest, outcomes []*contracts.OutcomeRecord, stats map[string]ProgramTrust) error {
	evidence, err := memory.ReadProgramLearningEvidence(ctx, request.RepositoryWorktree, request.SourceCommit)
	if err != nil {
		return err
	}
	byID := map[string]*contracts.OutcomeRecord{}
	for _, o := range outcomes {
		byID[o.GetIncidentId()] = o
	}
	credited := map[string]bool{}
	for _, learned := range evidence {
		origin := byID[learned.IncidentID]
		if origin == nil {
			continue
		}
		var causes [][]*contracts.ObjectRef
		if origin.GetClassification() == "success" {
			for _, cause := range origin.GetConfirmedRootCauses() {
				causes = append(causes, cause.GetResources())
			}
		} else {
			for _, cause := range origin.GetReflectionEligibility().GetCauses() {
				causes = append(causes, cause.GetResources())
			}
		}
		verified := false
		for _, resources := range causes {
			covered := len(resources) > 0
			observed := validate.RepairedCauseObserved(origin, resources)
			for _, resource := range resources {
				found := false
				for _, ref := range learned.Bindings.Objects {
					found = found || sameResource(ref, objectRefState(resource))
				}
				// The broker already validated replay. An independently observed
				// recovered symptom needs no program parameter; every recorded write does.
				written := false
				for _, action := range origin.GetRepairActions() {
					if !action.GetSuccess() {
						continue
					}
					for _, ref := range action.GetResources() {
						written = written || sameResource(objectRefState(ref), objectRefState(resource))
					}
				}
				covered = covered && (found || (observed && !written))
			}
			verified = verified || covered
		}
		if !verified {
			continue
		}
		id := learned.ProgramID
		if !programIDPath.MatchString(id) || credited[id] {
			continue
		}
		// A program already executed at its origin has ordinary settlement credit.
		ran := false
		for _, run := range origin.GetProgramRuns() {
			ran = ran || run.GetProgramId() == id
		}
		if ran {
			continue
		}
		state := stats[id]
		if state.Failures > 0 {
			continue
		}

		revision, err := programDependencyRevision(ctx, request.RepositoryWorktree, request.SourceCommit, id)
		if err != nil {
			return err
		}
		state.VerifiedSuccesses++
		state.LastChange = revision
		stats[id] = state
		credited[id] = true
	}
	return nil
}

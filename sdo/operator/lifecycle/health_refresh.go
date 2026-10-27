package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// RefreshHealth reuses judge publication without redeployment, architecture
// authorship, or bootstrap. The broker's proposal/acceptance and rejection log
// use an input-bound ID, so interruption resumes the same publication.
func (c Coordinator) RefreshHealth(ctx context.Context, cfg Config, revision memory.HealthRevision) (broker.Receipt, error) {
	if c.Broker == nil || c.Bootstrap == nil || c.Registry == nil || c.Workers == nil || cfg.Application == "" || cfg.Namespace == "" || revision.Commit == "" || revision.SourceInputs == "" || revision.DetectorRevision == "" || revision.Objective == "" {
		return broker.Receipt{}, errors.New("health refresh requires broker, judge, application and accepted health inputs")
	}
	if !revision.NeedsRefresh {
		return broker.Receipt{Commit: revision.Commit}, nil
	}
	cfg.HealthObjective = revision.Objective
	// The logical input ID ignores operational-memory commits. The concrete
	// proposal/task must also bind its immutable accepted base: a retained Job
	// must never receive a different request or write into a recreated checkout.
	publicationID, err := c.refreshGeneration(revision.RefreshID() + "-base-" + revision.Commit)
	if err != nil {
		return broker.Receipt{}, fmt.Errorf("refresh health requirements: %w", err)
	}
	receipt, ready, feedback, err := c.judge(ctx, publicationID, cfg, 1, revision.Commit, revision.Commit, "")
	if err != nil {
		return broker.Receipt{}, fmt.Errorf("refresh health requirements: %w", err)
	}
	if !ready {
		return broker.Receipt{}, fmt.Errorf("health unverified: %s", feedback)
	}
	return receipt, nil
}

// refreshGeneration returns the publication ID whose rejection log still has
// attempts left. A log made terminal only by lost workers says nothing about
// the judge's work, so the next controller start gets a fresh bounded
// generation instead of failing on that log forever.
func (c Coordinator) refreshGeneration(base string) (string, error) {
	for generation := 0; ; generation++ {
		id := base
		if generation > 0 {
			id = fmt.Sprintf("%s-g%d", base, generation)
		}
		log, err := c.Broker.Rejections(id)
		if err != nil {
			return "", err
		}
		if !log.GetTerminal() || !onlyLostWorkers(log) {
			return id, nil
		}
	}
}

func onlyLostWorkers(log *contracts.ProposalRejectionLog) bool {
	for _, rejection := range log.GetRejections() {
		diagnostics := rejection.GetDiagnostics()
		if len(diagnostics) == 0 || diagnostics[0].GetRule() != vanishedWorkerRule {
			return false
		}
	}
	return true
}

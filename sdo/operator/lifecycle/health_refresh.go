package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"sdo.dev/operator/broker"
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
	receipt, ready, feedback, err := c.judge(ctx, revision.RefreshID(), cfg, 1, revision.Commit, revision.Commit, "")
	if err != nil {
		return broker.Receipt{}, fmt.Errorf("refresh health requirements: %w", err)
	}
	if !ready {
		return broker.Receipt{}, fmt.Errorf("health unverified: %s", feedback)
	}
	return receipt, nil
}

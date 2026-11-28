package prober

import (
	"context"
	"time"

	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/sdk/traffic"
)

// JourneyCheck runs the full journey checks (journeycheck.Run with altered
// answers) of every health probe the prober schedules, with the prober's own
// transport, identities and clock: the checks an onboarding session no longer
// runs itself, done once on the installed application.
func (p *Prober) JourneyCheck(ctx context.Context) journeycheck.Report {
	var workloads []traffic.Workload
	for _, workload := range p.config.Workloads {
		if workload.Purpose == traffic.PurposeHealthProbe {
			workloads = append(workloads, workload.WithDefaults())
		}
	}
	clock := p.config.Clock
	return journeycheck.Run(ctx, journeycheck.Config{
		Catalog: p.config.Catalog, Workloads: workloads, Alter: true,
		Client: p.config.Client, BaseURL: p.config.BaseURL, Passwords: p.config.Passwords, Exec: p.config.Exec, Clock: clock,
		Wait: func(ctx context.Context, d time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-clock.After(d):
				return nil
			}
		},
	})
}

package runtime

import (
	"context"
	"fmt"
	"io"
	"sort"

	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

// maxWriteSoak caps the rounds per pass: the soak adds real writes to the application.
const maxWriteSoak = 20

// soakWrites re-runs, rounds times, every steady workload's write scenarios
// (the ones that are not plain reads) now, recording each sample in the steady
// window like a scheduled one. A probe that reads a list other users change
// then sees it change; a probe that only passes on a static dataset fails and
// the traffic detector names its scenario and step. A failed burst is reported
// and the soak goes on: the detectors judge the samples, not the soak.
func soakWrites(ctx context.Context, api ProberAPI, rounds int, stderr io.Writer) error {
	if rounds <= 0 {
		return nil
	}
	windows, err := api.Windows(ctx)
	if err != nil {
		return fmt.Errorf("read the probes' scenarios: %w", err)
	}
	names := make([]string, 0, len(windows))
	for name, window := range windows {
		if len(window.Links) > 0 {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var writes []string
		for _, observed := range windows[name].Scenarios {
			if effect := observed.Scenario.SideEffect; effect != "" && effect != traffic.SideEffectRead {
				writes = append(writes, observed.Scenario.ID)
			}
		}
		sort.Strings(writes)
		if len(writes) == 0 {
			continue
		}
		for round := 0; round < rounds; round++ {
			result, err := api.Burst(ctx, prober.BurstRequest{Workload: name, Scenarios: writes})
			if err == nil && !result.Healthy {
				// The detectors already see the failed samples; more rounds
				// would only slow the judgement down.
				break
			}
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				fmt.Fprintf(stderr, "write soak of %s: %v\n", name, err)
				break
			}
		}
	}
	return nil
}

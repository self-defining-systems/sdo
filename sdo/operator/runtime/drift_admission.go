package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"strings"
	"time"
)

// admitDriftBatch applies limits only to repeat, drift-only incidents. The
// ordinary batcher owns pending observations and the responder owns execution.
func (c *Controller) admitDriftBatch(now time.Time, batch []sdk.Finding) []sdk.Finding {
	if c.drift == nil {
		return batch
	}
	for _, f := range batch {
		if f.DetectorID != "drift.unexplained" {
			return batch
		}
	}
	if c.driftAdmission == nil {
		c.driftAdmission = &contracts.DriftAdmissionState{}
	}
	state := c.driftAdmission
	cutoff := now.Add(-time.Hour)
	dispatches := state.DispatchAt[:0]
	for _, at := range state.DispatchAt {
		if controllerTime(at).After(cutoff) {
			dispatches = append(dispatches, at)
		}
	}
	state.DispatchAt = dispatches
	uses := state.TokenUsage[:0]
	var tokens int64
	for _, use := range state.TokenUsage {
		if controllerTime(use.At).After(cutoff) {
			uses = append(uses, use)
			tokens += use.Tokens
		}
	}
	state.TokenUsage = uses
	seen := map[string]bool{}
	for _, key := range state.SeenKeys {
		seen[key] = true
	}
	allowed := make([]sdk.Finding, 0, 8)
	retryAt := now
	if c.drift.DispatchesPerHour > 0 && len(dispatches) >= c.drift.DispatchesPerHour {
		retryAt = controllerTime(dispatches[0]).Add(time.Hour)
	}
	if c.drift.TokensPerHour > 0 && tokens >= c.drift.TokensPerHour && len(uses) > 0 {
		at := controllerTime(uses[0].At).Add(time.Hour)
		if at.After(retryAt) {
			retryAt = at
		}
	}
	// New keys have priority even after a retry budget has been spent.
	for _, newOnly := range []bool{true, false} {
		for _, f := range batch {
			events := driftEventsForFindings(nil, []sdk.Finding{f})
			key := FindingFingerprint(f)
			if len(events) > 0 {
				key = driftEventKey(events[0])
			}
			fresh := !seen[key]
			if fresh != newOnly {
				continue
			}
			if len(allowed) >= 8 || !fresh && retryAt.After(now) {
				delay := time.Second
				if retryAt.After(now) {
					delay = retryAt.Sub(now)
				}
				c.batcher.AddAtWithDebounce(f, now, delay)
				continue
			}
			allowed = append(allowed, f)
			if fresh {
				state.SeenKeys = append(state.SeenKeys, key)
			}
		}
	}
	if len(allowed) > 0 {
		state.DispatchAt = append(state.DispatchAt, contractTimestamp(now))
	}
	return allowed
}

func (c *Controller) chargeDriftResponse(now time.Time, result IncidentResult) {
	if c.driftAdmission == nil || c.currentIncidentRequest == nil {
		return
	}
	for _, f := range c.currentIncidentRequest.Findings {
		if !strings.HasPrefix(f.DetectorID, "drift.") {
			return
		}
	}
	c.driftAdmission.TokenUsage = append(c.driftAdmission.TokenUsage, &contracts.DriftAdmissionUse{At: contractTimestamp(now), Tokens: result.Usage.GetInputTokens() + result.Usage.GetOutputTokens()})
}

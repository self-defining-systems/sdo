package journeycheck

import (
	"fmt"
	"net/http"
	"strings"

	"sdo.dev/operator/sdk/traffic"
)

// cleanupGaps reports why a write-with-cleanup scenario's cleanup could leave
// its data behind unseen, from the steps alone. An application may answer a
// delete with success and only schedule the removal (delayed deletion, a
// trash, a soft delete), so only a read after the last cleanup write shows
// the data is gone; and a cleanup write that accepts a refusal (a 4xx other
// than not-found) counts a delete the application refused as done. Probe data
// left behind every iteration grows without bound and crowds what users see.
func cleanupGaps(s traffic.Scenario) []string {
	if s.SideEffect != traffic.SideEffectWriteWithCleanup {
		return nil
	}
	// Only the writes after the last read matter: a read that follows a
	// write shows its result, whatever status the write accepted.
	var unverified []traffic.Step
	for _, step := range s.Cleanup {
		endpoint, ok := step.Endpoint.(*traffic.HTTPEndpoint)
		if ok && (endpoint.Method == http.MethodGet || endpoint.Method == http.MethodHead) {
			unverified = nil
			continue
		}
		// Other protocols count as writes, so a later HTTP read must show
		// their result.
		unverified = append(unverified, step)
	}
	if len(unverified) == 0 {
		return nil
	}
	var gaps []string
	for _, step := range unverified {
		endpoint, ok := step.Endpoint.(*traffic.HTTPEndpoint)
		if !ok {
			continue
		}
		var refused []string
		for _, code := range endpoint.ExpectStatus {
			if code >= 400 && code != http.StatusNotFound && code != http.StatusGone {
				refused = append(refused, fmt.Sprint(code))
			}
		}
		if len(refused) > 0 {
			gaps = append(gaps, fmt.Sprintf("step %s accepts %s, so a %s the application refused counts as done", step.Name, strings.Join(refused, ", "), endpoint.Method))
		}
	}
	return append(gaps, "no step reads the data back gone after the last cleanup write: an application that defers deletion keeps it, one more object every iteration")
}

func cleanupResult(s traffic.Scenario) (Result, bool) {
	gaps := cleanupGaps(s)
	if len(gaps) == 0 {
		return Result{}, false
	}
	return Result{ItemCleanup, s.ID, Red, "cleanup cannot see the data it leaves: " + strings.Join(gaps, "; ")}, true
}

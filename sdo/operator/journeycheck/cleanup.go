package journeycheck

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"sort"
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
	if s.SideEffect == traffic.SideEffectIdempotentWrite {
		return idempotentGaps(s)
	}
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
	return append(gaps, "no step reads the data back gone after the last cleanup write: end Cleanup with a GET or HEAD expecting 404/410, or a list assertion that checks the deleted data is absent")
}

func cleanupResult(s traffic.Scenario) (Result, bool) {
	gaps := cleanupGaps(s)
	if len(gaps) == 0 {
		return Result{}, false
	}
	lead := "cleanup cannot see the data it leaves: "
	if s.SideEffect == traffic.SideEffectIdempotentWrite {
		lead = "probe data piles up: "
	}
	return Result{ItemCleanup, s.ID, Red, lead + strings.Join(gaps, "; ")}, true
}

// uniqueToken matches the token traffic.Unique appends.
var uniqueToken = regexp.MustCompile(fmt.Sprintf(`[0-9a-f]{%d}`, traffic.UniqueWidth))

// idempotentGaps reports the write steps of an idempotent-write scenario that
// name a new object every iteration (a Unique token in the request): an
// idempotent write has no cleanup, so each iteration leaves one more object.
// Each write step is built for two iterations that differ only in their
// random source; a step that needs a value an earlier step saved cannot be
// built alone and is not judged.
func idempotentGaps(s traffic.Scenario) []string {
	var gaps []string
	for _, step := range s.Steps {
		if endpoint, ok := step.Endpoint.(*traffic.HTTPEndpoint); ok && (endpoint.Method == http.MethodGet || endpoint.Method == http.MethodHead) {
			continue
		}
		first, err := buildText(step.Endpoint, 1)
		if err != nil {
			continue
		}
		second, err := buildText(step.Endpoint, 2)
		if err != nil {
			continue
		}
		seen := map[string]bool{}
		for _, token := range uniqueToken.FindAllString(second, -1) {
			seen[token] = true
		}
		for _, token := range uniqueToken.FindAllString(first, -1) {
			if !seen[token] {
				gaps = append(gaps, fmt.Sprintf("step %s of an idempotent write names a new object each iteration (a Unique value) and nothing removes it: write one fixed object, or declare %s and delete it in Cleanup", step.Name, traffic.SideEffectWriteWithCleanup))
				break
			}
		}
	}
	return gaps
}

// buildText is the request an endpoint builds with the random source seed,
// as one string.
func buildText(endpoint traffic.Endpoint, seed uint64) (string, error) {
	request, err := endpoint.Build(context.Background(), rand.New(rand.NewPCG(seed, seed)), traffic.State{traffic.StateNow: "2026-01-01T00:00:00Z"})
	if err != nil {
		return "", err
	}
	parts := []string{request.Method, request.Path, request.Body}
	for _, values := range []map[string]string{request.Query, request.Headers} {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			parts = append(parts, key+"="+values[key])
		}
	}
	return strings.Join(parts, "\n"), nil
}

package traffic

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
)

// Values of Sample.ExistingRecords: what a ReadsExistingRecords step saw.
const (
	// ExistingRecordsExercised: it returned at least one record the probe did
	// not create.
	ExistingRecordsExercised = "exercised"
	// ExistingRecordsOwnOnly: it returned records, all carrying the scenario's
	// Marker.
	ExistingRecordsOwnOnly = "own-only"
	// ExistingRecordsEmpty: it returned none. The step passes: a fresh install
	// has no user data.
	ExistingRecordsEmpty = "empty"
)

// existingRecordsOutcome classifies the members a marked step read. A member
// is the probe's own when it contains the scenario's Marker.
func existingRecordsOutcome(saved string, marker string) string {
	var members []string
	if json.Unmarshal([]byte(saved), &members) != nil || len(members) == 0 {
		return ExistingRecordsEmpty
	}
	for _, member := range members {
		if marker == "" || !strings.Contains(member, marker) {
			return ExistingRecordsExercised
		}
	}
	return ExistingRecordsOwnOnly
}

// ExistingRecordsStep is the compiled fact about one step built with
// ReadsExistingRecords.
type ExistingRecordsStep struct {
	Name string
	// ChecksContent is true when the step's member pattern is valid and
	// non-empty.
	ChecksContent bool
	// OwnKeys are the State keys the step templates (in its path, query,
	// headers or body) that a write step of the scenario saved, sorted: the
	// step then reads what the scenario itself wrote.
	OwnKeys []string
}

// ExistingRecordsSteps reports the scenario's steps marked with
// ReadsExistingRecords. To find the keys a marked step templates, it builds
// the step's request with every key a step saves set to a distinct sentinel
// and looks for the sentinels in the request, so it holds for any parameter
// generator.
func (s Scenario) ExistingRecordsSteps() []ExistingRecordsStep {
	var marked []Step
	for _, step := range s.Steps {
		if endpoint, ok := step.Endpoint.(*HTTPEndpoint); ok && endpoint.MarksExistingRecords() {
			marked = append(marked, step)
		}
	}
	if len(marked) == 0 {
		return nil
	}
	written := map[string]bool{}
	sentinels := State{}
	for _, phase := range [][]Step{s.Setup, s.Steps} {
		for _, step := range phase {
			endpoint, ok := step.Endpoint.(*HTTPEndpoint)
			if !ok {
				continue
			}
			isWrite := endpoint.Method != http.MethodGet && endpoint.Method != http.MethodHead
			var keys []string
			for key := range endpoint.Saves {
				keys = append(keys, key)
			}
			for key := range endpoint.cookies {
				keys = append(keys, key)
			}
			for _, key := range keys {
				sentinels[key] = sentinel(key)
				if isWrite {
					written[key] = true
				}
			}
		}
	}
	for _, user := range s.Users {
		sentinels[user.As] = sentinel(user.As)
		if user.SSHKey != "" {
			sentinels[user.SSHKey] = sentinel(user.SSHKey)
		}
	}
	var out []ExistingRecordsStep
	for _, step := range marked {
		endpoint := step.Endpoint.(*HTTPEndpoint)
		fact := ExistingRecordsStep{Name: step.Name, ChecksContent: endpoint.existingRecords.compileErr == nil}
		request, err := endpoint.Build(context.Background(), rand.New(rand.NewPCG(1, 2)), sentinels.clone())
		if err == nil {
			text := request.Path + "\x00" + request.Body
			for key, value := range request.Query {
				text += "\x00" + key + "=" + value
			}
			for name, value := range request.Headers {
				text += "\x00" + name + "=" + value
			}
			for key := range written {
				if strings.Contains(text, sentinel(key)) {
					fact.OwnKeys = append(fact.OwnKeys, key)
				}
			}
			sort.Strings(fact.OwnKeys)
		}
		out = append(out, fact)
	}
	return out
}

func sentinel(key string) string { return "SDOSENTINEL" + key + "END" }

// ExistingRecordsExercised counts the samples in which a ReadsExistingRecords
// step returned at least one record the probe did not write.
func (w Window) ExistingRecordsExercised() int {
	n := 0
	for _, scenario := range w.Scenarios {
		for _, sample := range scenario.Samples {
			if sample.ExistingRecords == ExistingRecordsExercised {
				n++
			}
		}
	}
	return n
}

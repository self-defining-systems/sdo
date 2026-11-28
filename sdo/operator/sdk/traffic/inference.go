package traffic

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// CompleteCatalog fills what Go derives instead of the author: an empty
// SideEffect from the steps' methods (InferSideEffect), and an empty
// DependsOn from paths, the Services of each component on the operations'
// paths of the application map's probe line for the scenario. DependsOn gets
// one Service per component: the one the scenario's steps call, else the
// component's first. Scenarios whose side effect cannot be inferred keep it
// empty, and Validate says what to declare. The generated catalog runs every
// scenario through it, so the prober, the checks and the controller see the
// same values.
func CompleteCatalog(catalog Catalog, paths map[string][][]string) Catalog {
	out := make(Catalog, len(catalog))
	for index, scenario := range catalog {
		if scenario.SideEffect == "" {
			if inferred, err := InferSideEffect(scenario); err == nil {
				scenario.SideEffect = inferred
			}
		}
		if len(scenario.DependsOn) == 0 {
			scenario.DependsOn = dependsOnFromPath(scenario, paths[scenario.ID])
		}
		out[index] = scenario
	}
	return out
}

func dependsOnFromPath(s Scenario, components [][]string) []string {
	called := map[string]bool{s.Target.Service: true}
	for _, step := range append(append(append([]Step(nil), s.Setup...), s.Steps...), s.Cleanup...) {
		if step.Target != nil {
			called[step.Target.Service] = true
		}
	}
	var out []string
	for _, services := range components {
		if len(services) == 0 {
			continue
		}
		chosen := services[0]
		for _, service := range services {
			if called[service] {
				chosen = service
				break
			}
		}
		out = append(out, chosen)
	}
	return out
}

// InferSideEffect derives a scenario's side effect from its steps: read when
// every step sends GET or HEAD (or is marked Reads), write-with-cleanup when a
// step writes and the scenario has Cleanup. A write without Cleanup, or a step
// whose method only its own Build knows (a custom Endpoint), must be declared:
// only the author knows whether such a write leaves data behind.
func InferSideEffect(s Scenario) (SideEffect, error) {
	writes := false
	for _, step := range s.Steps {
		if s.stepTarget(step).Scheme == "ssh" {
			writes = true
			continue
		}
		endpoint, ok := declaredHTTP(step.Endpoint)
		if !ok {
			return "", fmt.Errorf("step %s is a custom Endpoint, so Go cannot tell whether it writes: declare SideEffect (%s, %s or %s)", step.Name, SideEffectRead, SideEffectIdempotentWrite, SideEffectWriteWithCleanup)
		}
		writes = writes || (Request{Method: endpoint.Method, readOnly: endpoint.readOnly}).writes()
	}
	switch {
	case !writes:
		return SideEffectRead, nil
	case len(s.Cleanup) > 0:
		return SideEffectWriteWithCleanup, nil
	}
	return "", fmt.Errorf("a step writes and the scenario has no Cleanup: delete what it wrote in Cleanup, or declare SideEffect: traffic.SideEffectIdempotentWrite when it rewrites the same bounded synthetic data every time")
}

// declaration is promoted to a type that embeds *HTTPEndpoint (a wrapper
// that refines Check), so Go can read the method such a step sends.
func (e *HTTPEndpoint) declaration() *HTTPEndpoint { return e }

// declaredHTTP is the declarative endpoint behind a step: the step itself, the
// read an Eventually wraps, or the *HTTPEndpoint a wrapper type embeds.
func declaredHTTP(endpoint Endpoint) (*HTTPEndpoint, bool) {
	if eventual, ok := endpoint.(*EventualEndpoint); ok {
		endpoint = eventual.Endpoint
	}
	if declared, ok := endpoint.(interface{ declaration() *HTTPEndpoint }); ok && declared.declaration() != nil {
		return declared.declaration(), true
	}
	return nil, false
}

// stateOwn is the State key listing the State keys whose values a marked
// write of this scenario saved (SaveJSON on a write that carried the marker):
// the identities of records the probe itself created.
const stateOwn = "sdo.own"

// creditOwn records the keys endpoint saved from the answer to a marked write.
func creditOwn(step Step, request Request, scenario Scenario, state State) {
	endpoint, ok := declaredHTTP(step.Endpoint)
	if !ok || len(endpoint.Saves) == 0 || !request.writes() {
		return
	}
	if !requestMentions(request, scenario.Marker) && !namesOwnValue(request, state) {
		return
	}
	keys := ownKeys(state)
	for key := range endpoint.Saves {
		keys[key] = true
	}
	names := make([]string, 0, len(keys))
	for key := range keys {
		names = append(names, key)
	}
	sort.Strings(names)
	state[stateOwn] = strings.Join(names, ",")
}

func ownKeys(state State) map[string]bool {
	keys := map[string]bool{}
	for _, key := range strings.Split(state[stateOwn], ",") {
		if key != "" {
			keys[key] = true
		}
	}
	return keys
}

// namesOwnValue reports whether the request addresses a record the probe
// created: a whole path segment, query value, form value or JSON body value
// equals a value a marked write of this scenario saved.
func namesOwnValue(request Request, state State) bool {
	values := map[string]bool{}
	for key := range ownKeys(state) {
		if value := state[key]; value != "" {
			values[value] = true
		}
	}
	if len(values) == 0 {
		return false
	}
	for _, segment := range strings.Split(request.Path, "/") {
		if unescaped, err := url.PathUnescape(segment); err == nil && values[unescaped] {
			return true
		}
	}
	for _, value := range request.Query {
		if values[value] {
			return true
		}
	}
	if request.Body == "" {
		return false
	}
	if form, err := url.ParseQuery(request.Body); err == nil {
		for _, list := range form {
			for _, value := range list {
				if values[value] {
					return true
				}
			}
		}
	}
	var document any
	if json.Unmarshal([]byte(request.Body), &document) == nil {
		return jsonHasValue(document, values)
	}
	return false
}

func jsonHasValue(document any, values map[string]bool) bool {
	switch typed := document.(type) {
	case map[string]any:
		for _, member := range typed {
			if jsonHasValue(member, values) {
				return true
			}
		}
	case []any:
		for _, member := range typed {
			if jsonHasValue(member, values) {
				return true
			}
		}
	default:
		if scalar, ok := jsonScalar(typed); ok {
			return values[scalar]
		}
	}
	return false
}

// DeleteReadBack is the read SDO sends after Cleanup to see the data gone:
// when the last cleanup write is a declarative DELETE, no read follows it,
// and a step of the scenario reads the same path (so the path has a GET route
// that answers while the record exists), SDO GETs that path with the
// DELETE's query and headers and requires 404 or 410. A scenario whose
// application answers a deleted path otherwise (a trash, a soft delete) reads
// it back itself; that read replaces this one.
func (s Scenario) DeleteReadBack() (Step, bool) {
	var last *HTTPEndpoint
	name := ""
	for _, step := range s.Cleanup {
		endpoint, ok := step.Endpoint.(*HTTPEndpoint)
		if !ok {
			last = nil
			continue
		}
		switch {
		case endpoint.Method == http.MethodGet || endpoint.Method == http.MethodHead:
			last = nil
		case endpoint.Method == http.MethodDelete && step.Target == nil:
			last, name = endpoint, step.Name
		default:
			last = nil
		}
	}
	if last == nil {
		return Step{}, false
	}
	read := false
	for _, step := range append(append([]Step(nil), s.Setup...), s.Steps...) {
		if endpoint, ok := declaredHTTP(step.Endpoint); ok && endpoint.Method == http.MethodGet && endpoint.Path == last.Path && step.Target == nil {
			read = true
		}
	}
	if !read {
		return Step{}, false
	}
	gone := &HTTPEndpoint{Method: http.MethodGet, Path: last.Path, Query: last.Query, Headers: last.Headers,
		basicUser: last.basicUser, basicPassword: last.basicPassword, ExpectStatus: []int{http.StatusNotFound, http.StatusGone}}
	return Step{Name: name + "-gone", Endpoint: gone}, true
}

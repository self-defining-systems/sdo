// Package traffic is SDO's app-agnostic synthetic-traffic health machinery.
//
// The health judge writes, in the application's operational memory, Go
// generators (.sdo/diagnostics/traffic/generators/) that describe user
// journeys as Scenarios: ordered Steps whose Endpoints build requests from a
// seeded random source and shared per-iteration State, and check responses.
// Workload profiles (.sdo/diagnostics/traffic/workloads/<name>.yaml) say
// which scenarios run, how often, and against which SLO.
//
// The Engine owns everything about when and how much: scheduling, arrival,
// rate and concurrency caps, timeouts, latency measurement and the random
// seed of every iteration, so a failing probe replays exactly. Generators only
// build requests and judge responses. A separate prober process runs the
// engine; the controller runtime hands its observations to detectors built
// with NewDetector, which judge each scenario against a sliding-window SLO.
// Nothing here knows about a particular application, and nothing calls an LLM.
package traffic

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strings"
)

// SideEffect classifies what a scenario does to application state.
type SideEffect string

const (
	// SideEffectRead scenarios send only GET and HEAD requests.
	SideEffectRead SideEffect = "read"
	// SideEffectIdempotentWrite scenarios write the same dedicated synthetic
	// data every time, so repeating them changes nothing further.
	SideEffectIdempotentWrite SideEffect = "idempotent-write"
	// SideEffectWriteWithCleanup scenarios create synthetic data and remove
	// it again with their Cleanup steps.
	SideEffectWriteWithCleanup SideEffect = "write-with-cleanup"
)

// FaultClass is a failure a scenario must detect. Every scenario must detect
// FaultUnreachable and FaultErrorStatus; the others are declared when the
// scenario's checks are strong enough to catch them.
type FaultClass string

const (
	// FaultUnreachable: the target refuses connections or does not resolve.
	FaultUnreachable FaultClass = "unreachable"
	// FaultErrorStatus: the target answers with a 5xx status.
	FaultErrorStatus FaultClass = "error-status"
	// FaultWrongBody: the target answers 200 with an empty or wrong body.
	FaultWrongBody FaultClass = "wrong-body"
	// FaultSlow: the target does not answer within the request timeout.
	FaultSlow FaultClass = "slow"
)

// RequiredFaultClasses are detected by every valid scenario.
var RequiredFaultClasses = []FaultClass{FaultUnreachable, FaultErrorStatus}

var knownFaultClasses = map[FaultClass]bool{
	FaultUnreachable: true, FaultErrorStatus: true, FaultWrongBody: true, FaultSlow: true,
}

const (
	// SyntheticHeader marks every synthetic request so application logs and
	// workload analysis can tell it apart from users.
	SyntheticHeader = "X-SDO-Synthetic"
	// MaxSteps bounds the steps (and, separately, cleanup steps) of a scenario.
	MaxSteps = 8
)

var (
	namePattern     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	scenarioPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,62}$`)
	methods         = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true}
)

// State carries values between the steps of one iteration, for example an ID
// a create step saved for the read and cleanup steps that follow. Every
// iteration starts with an empty State.
type State map[string]string

// Request is one HTTP request relative to a scenario's target Service.
type Request struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   map[string]string `json:"query,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
}

// Response is what the engine received for a Request. Body is bounded.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// Verdict is an Endpoint's judgement of one response.
type Verdict struct {
	OK     bool
	Reason string
}

// Pass accepts a response.
func Pass() Verdict { return Verdict{OK: true} }

// Fail rejects a response with a reason that names what was wrong.
func Fail(format string, args ...any) Verdict {
	return Verdict{Reason: fmt.Sprintf(format, args...)}
}

// Endpoint builds one request and judges its response. Build must derive
// every varying value from rng and state; the engine seeds rng per iteration,
// so the same iteration always builds the same requests. Check may save values
// into state for later steps. Neither may measure time or send requests.
type Endpoint interface {
	Build(ctx context.Context, rng *rand.Rand, state State) (Request, error)
	Check(response Response, state State) Verdict
}

// Step is one named request of a scenario.
type Step struct {
	Name     string
	Endpoint Endpoint
}

// Target is the Service, in the application namespace, a scenario calls.
type Target struct {
	Service string `json:"service"`
	Port    int    `json:"port"`
	Scheme  string `json:"scheme,omitempty"`
}

// Scenario is one user journey. DependsOn names the Services on its request
// path, as described in .sdo/arch.md, so a failing scenario localizes the
// fault. Writes must use dedicated synthetic data whose Marker appears in
// every write request.
type Scenario struct {
	ID          string
	Description string
	Target      Target
	DependsOn   []string
	SideEffect  SideEffect
	Marker      string
	Steps       []Step
	Cleanup     []Step
	Detects     []FaultClass
}

// Descriptor is the serializable description of a scenario that travels with
// its observations, so detectors can explain and localize failures without
// the generator code.
type Descriptor struct {
	ID          string       `json:"id"`
	Description string       `json:"description,omitempty"`
	Target      Target       `json:"target"`
	DependsOn   []string     `json:"dependsOn,omitempty"`
	SideEffect  SideEffect   `json:"sideEffect"`
	Steps       []string     `json:"steps"`
	Detects     []FaultClass `json:"detects"`
}

// Describe returns the scenario's descriptor with its effective fault classes.
func (s Scenario) Describe() Descriptor {
	steps := make([]string, 0, len(s.Steps))
	for _, step := range s.Steps {
		steps = append(steps, step.Name)
	}
	return Descriptor{
		ID: s.ID, Description: s.Description, Target: s.normalizedTarget(),
		DependsOn: append([]string(nil), s.DependsOn...), SideEffect: s.SideEffect,
		Steps: steps, Detects: s.FaultClasses(),
	}
}

// FaultClasses returns the required classes followed by the declared ones.
func (s Scenario) FaultClasses() []FaultClass {
	classes := append([]FaultClass(nil), RequiredFaultClasses...)
	for _, class := range s.Detects {
		seen := false
		for _, existing := range classes {
			seen = seen || existing == class
		}
		if !seen {
			classes = append(classes, class)
		}
	}
	return classes
}

func (s Scenario) normalizedTarget() Target {
	target := s.Target
	if target.Scheme == "" {
		target.Scheme = "http"
	}
	return target
}

// Validate reports the first problem that makes the scenario unsafe or
// ambiguous. Rules that depend on built requests (read scenarios send only
// GET/HEAD; writes carry the marker) are enforced by the engine per request.
func (s Scenario) Validate() error {
	if !scenarioPattern.MatchString(s.ID) {
		return fmt.Errorf("scenario id %q must be lowercase letters, digits, '.', '_', or '-'", s.ID)
	}
	target := s.normalizedTarget()
	if !namePattern.MatchString(target.Service) {
		return fmt.Errorf("scenario %s: target service %q must be a Service name", s.ID, target.Service)
	}
	if target.Port < 1 || target.Port > 65535 {
		return fmt.Errorf("scenario %s: target port %d is out of range", s.ID, target.Port)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return fmt.Errorf("scenario %s: target scheme must be http or https", s.ID)
	}
	for _, service := range s.DependsOn {
		if !namePattern.MatchString(service) {
			return fmt.Errorf("scenario %s: dependency %q must be a Service name", s.ID, service)
		}
	}
	if len(s.Steps) == 0 || len(s.Steps) > MaxSteps {
		return fmt.Errorf("scenario %s: needs 1 to %d steps", s.ID, MaxSteps)
	}
	if len(s.Cleanup) > MaxSteps {
		return fmt.Errorf("scenario %s: at most %d cleanup steps", s.ID, MaxSteps)
	}
	for _, step := range append(append([]Step(nil), s.Steps...), s.Cleanup...) {
		if !scenarioPattern.MatchString(step.Name) {
			return fmt.Errorf("scenario %s: step name %q must be lowercase letters, digits, '.', '_', or '-'", s.ID, step.Name)
		}
		if step.Endpoint == nil {
			return fmt.Errorf("scenario %s: step %s has no endpoint", s.ID, step.Name)
		}
	}
	for _, class := range s.Detects {
		if !knownFaultClasses[class] {
			return fmt.Errorf("scenario %s: unknown fault class %q", s.ID, class)
		}
	}
	switch s.SideEffect {
	case SideEffectRead:
		if s.Marker != "" || len(s.Cleanup) > 0 {
			return fmt.Errorf("scenario %s: marker and cleanup apply only to write scenarios", s.ID)
		}
	case SideEffectIdempotentWrite, SideEffectWriteWithCleanup:
		if strings.TrimSpace(s.Marker) == "" || len(s.Marker) < 6 {
			return fmt.Errorf("scenario %s: a write scenario needs a Marker of at least 6 characters naming its synthetic data", s.ID)
		}
		if s.SideEffect == SideEffectWriteWithCleanup && len(s.Cleanup) == 0 {
			return fmt.Errorf("scenario %s: a write-with-cleanup scenario needs cleanup steps", s.ID)
		}
		if s.SideEffect == SideEffectIdempotentWrite && len(s.Cleanup) > 0 {
			return fmt.Errorf("scenario %s: cleanup applies only to write-with-cleanup scenarios", s.ID)
		}
	default:
		return fmt.Errorf("scenario %s: sideEffect must be %q, %q, or %q",
			s.ID, SideEffectRead, SideEffectIdempotentWrite, SideEffectWriteWithCleanup)
	}
	return nil
}

// Catalog is the set of scenarios an application's generators provide.
type Catalog []Scenario

// Validate validates every scenario and rejects duplicate IDs.
func (c Catalog) Validate() error {
	if len(c) == 0 {
		return fmt.Errorf("traffic catalog has no scenarios")
	}
	seen := make(map[string]bool, len(c))
	for _, scenario := range c {
		if err := scenario.Validate(); err != nil {
			return err
		}
		if seen[scenario.ID] {
			return fmt.Errorf("duplicate scenario id %q", scenario.ID)
		}
		seen[scenario.ID] = true
	}
	return nil
}

// Scenario returns the scenario with the given ID.
func (c Catalog) Scenario(id string) (Scenario, bool) {
	for _, scenario := range c {
		if scenario.ID == id {
			return scenario, true
		}
	}
	return Scenario{}, false
}

// checkRequest enforces the per-request safety rules the engine applies to
// every built request.
func checkRequest(scenario Scenario, request Request, cleanup bool) error {
	if !methods[request.Method] {
		return fmt.Errorf("method %q is not one of GET, HEAD, POST, PUT, PATCH, DELETE", request.Method)
	}
	if !strings.HasPrefix(request.Path, "/") || strings.HasPrefix(request.Path, "//") ||
		strings.Contains(request.Path, "://") || strings.ContainsAny(request.Path, "?# \t\r\n") {
		return fmt.Errorf("path %q must be an absolute path on the target, without a host, query, or fragment", request.Path)
	}
	for name := range request.Headers {
		if strings.EqualFold(name, "Host") || strings.EqualFold(name, SyntheticHeader) {
			return fmt.Errorf("headers may not set %s", name)
		}
	}
	write := request.Method != "GET" && request.Method != "HEAD"
	if write && scenario.SideEffect == SideEffectRead {
		return fmt.Errorf("read scenario sent %s; declare a write side effect", request.Method)
	}
	if (write || cleanup) && scenario.SideEffect != SideEffectRead && !requestMentions(request, scenario.Marker) {
		return fmt.Errorf("write request does not carry the synthetic marker %q", scenario.Marker)
	}
	return nil
}

func requestMentions(request Request, marker string) bool {
	if marker == "" {
		return false
	}
	if strings.Contains(request.Body, marker) || strings.Contains(request.Path, marker) {
		return true
	}
	for _, values := range []map[string]string{request.Query, request.Headers} {
		for key, value := range values {
			if strings.Contains(key, marker) || strings.Contains(value, marker) {
				return true
			}
		}
	}
	return false
}

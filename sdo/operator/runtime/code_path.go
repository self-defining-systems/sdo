package runtime

import (
	"context"
	"fmt"
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// CodePathConfig chooses a coherent plugin set, with explicit optional seam overrides.
// The zero value retains the Go programming model. Registries are assembled before use.
type CodePathConfig struct {
	Name      string            `json:"name,omitempty"`
	Overrides CodePathOverrides `json:"overrides,omitempty"`
}
type CodePathOverrides struct {
	DetectorSource   string   `json:"detector_source,omitempty"`
	Coverage         string   `json:"coverage,omitempty"`
	ResponseStages   []string `json:"response_stages,omitempty"`
	MemoryValidation string   `json:"memory_validation,omitempty"`
	Activation       string   `json:"activation,omitempty"`
	AgentGuidance    string   `json:"agent_guidance,omitempty"`
}
type CodePathSelection struct {
	Name             string   `json:"name"`
	DetectorSource   string   `json:"detector_source"`
	Coverage         string   `json:"coverage"`
	ResponseStages   []string `json:"response_stages"`
	MemoryValidation string   `json:"memory_validation"`
	Activation       string   `json:"activation"`
	AgentGuidance    string   `json:"agent_guidance"`
}
type CodePathConfigError struct{ Seam, Value, Reason string }

func (e *CodePathConfigError) Error() string {
	return fmt.Sprintf("code path %s %q: %s", e.Seam, e.Value, e.Reason)
}

// DetectorSource supplies detector adapters; scheduling, findings and closure stay shared.
type DetectorSource interface {
	Detectors(ctx context.Context, request DetectorSourceRequest) ([]sdk.Detector, error)
}
type DetectorSourceRequest struct {
	RepositoryRoot    string
	Namespace         string
	CompiledDetectors []sdk.Detector
}

type CoverageProvider interface {
	Coverage(findings []sdk.Finding, runs []ProgramRun, snapshot sdk.DetectionContext, statistics map[string]ProgramTrust) *contracts.PlaybookCoverage
}

// MemoryValidation owns a mechanism whose dependencies are bound at construction.
type MemoryValidation interface {
	Validate(ctx context.Context, root string, paths []string) error
}

// PublishActivation selects the activation mechanism after broker acceptance.
type PublishActivation interface {
	Activate(ctx context.Context, commit string) error
}
type AgentGuidance interface {
	Variant(role string) (string, error)
}

type CodePath struct {
	Selection  CodePathSelection
	Detectors  DetectorSource
	Coverage   CoverageProvider
	Memory     MemoryValidation
	Activation PublishActivation
	Guidance   AgentGuidance
}
type CodePathRegistry struct {
	paths      map[string]CodePathSelection
	detectors  map[string]DetectorSource
	coverage   map[string]CoverageProvider
	memory     map[string]MemoryValidation
	activation map[string]PublishActivation
	guidance   map[string]AgentGuidance
	stages     map[string]bool
}

// CodePathDependencies bind each mechanism once, when its plugin is constructed.
// Additional plugins own their own dependencies and keep the same seam interface.
type CodePathDependencies struct {
	ValidateGo         func(context.Context, string, []string) error
	RebuildController  func(context.Context, string) error
	PullAcceptedCommit func(context.Context, string) error
}

func NewCodePathRegistry() *CodePathRegistry {
	return NewCodePathRegistryWithDependencies(CodePathDependencies{})
}
func NewCodePathRegistryWithDependencies(bindings CodePathDependencies) *CodePathRegistry {

	return &CodePathRegistry{
		paths: map[string]CodePathSelection{
			"go":    {Name: "go", DetectorSource: "go", Coverage: "diagnose", ResponseStages: []string{"deterministic", "agent"}, MemoryValidation: "go", Activation: "rollout", AgentGuidance: "go"},
			"shell": {Name: "shell", DetectorSource: "shell", Coverage: "none", ResponseStages: []string{"agent"}, MemoryValidation: "hygiene", Activation: "pull", AgentGuidance: "shell"},
		},
		detectors:  map[string]DetectorSource{"go": compiledCodePathDetectors{}, "shell": ShellDetectorSource{}},
		coverage:   map[string]CoverageProvider{"diagnose": goCodePathCoverage{}, "none": noCodePathCoverage{}},
		memory:     map[string]MemoryValidation{"go": goCodePathValidation{check: bindings.ValidateGo}, "hygiene": shellCodePathValidation{}},
		activation: map[string]PublishActivation{"rollout": configuredCodePathActivation{name: "rollout", activate: bindings.RebuildController}, "pull": configuredCodePathActivation{name: "pull", activate: bindings.PullAcceptedCommit}},
		guidance:   map[string]AgentGuidance{"go": codePathGuidance("go"), "shell": codePathGuidance("shell")},
		stages:     map[string]bool{"deterministic": true, "agent": true},
	}
}
func registerCodePathPlugin[T any](items map[string]T, name string, value T) error {
	if name == "" {
		return &CodePathConfigError{Seam: "registration", Reason: "name is required"}
	}
	if _, exists := items[name]; exists {
		return &CodePathConfigError{Seam: "registration", Value: name, Reason: "already registered"}
	}
	items[name] = value
	return nil
}
func (r *CodePathRegistry) RegisterDetectorSource(n string, v DetectorSource) error {
	if v == nil {
		return fmt.Errorf("detector source is nil")
	}
	return registerCodePathPlugin(r.detectors, n, v)
}
func (r *CodePathRegistry) RegisterCoverage(n string, v CoverageProvider) error {
	if v == nil {
		return fmt.Errorf("coverage provider is nil")
	}
	return registerCodePathPlugin(r.coverage, n, v)
}
func (r *CodePathRegistry) RegisterMemoryValidation(n string, v MemoryValidation) error {
	if v == nil {
		return fmt.Errorf("memory validation is nil")
	}
	return registerCodePathPlugin(r.memory, n, v)
}
func (r *CodePathRegistry) RegisterActivation(n string, v PublishActivation) error {
	if v == nil {
		return fmt.Errorf("activation is nil")
	}
	return registerCodePathPlugin(r.activation, n, v)
}
func (r *CodePathRegistry) RegisterGuidance(n string, v AgentGuidance) error {
	if v == nil {
		return fmt.Errorf("guidance is nil")
	}
	return registerCodePathPlugin(r.guidance, n, v)
}
func (r *CodePathRegistry) RegisterResponseStage(n string) error {
	return registerCodePathPlugin(r.stages, n, true)
}
func (r *CodePathRegistry) RegisterCodePath(n string, v CodePathSelection) error {
	v.Name = n
	v.ResponseStages = slices.Clone(v.ResponseStages)
	return registerCodePathPlugin(r.paths, n, v)
}
func (r *CodePathRegistry) Resolve(config CodePathConfig) (*CodePath, error) {
	name := config.Name
	if name == "" {
		name = "go"
	}
	s, ok := r.paths[name]
	if !ok {
		return nil, &CodePathConfigError{Seam: "name", Value: name, Reason: "not registered"}
	}
	o := config.Overrides
	for _, item := range []struct {
		dst      *string
		override string
	}{{&s.DetectorSource, o.DetectorSource}, {&s.Coverage, o.Coverage}, {&s.MemoryValidation, o.MemoryValidation}, {&s.Activation, o.Activation}, {&s.AgentGuidance, o.AgentGuidance}} {
		if item.override != "" {
			*item.dst = item.override
		}
	}
	if o.ResponseStages != nil {
		s.ResponseStages = slices.Clone(o.ResponseStages)
	} else {
		s.ResponseStages = slices.Clone(s.ResponseStages)
	}
	p := &CodePath{Selection: s, Detectors: r.detectors[s.DetectorSource], Coverage: r.coverage[s.Coverage], Memory: r.memory[s.MemoryValidation], Activation: r.activation[s.Activation], Guidance: r.guidance[s.AgentGuidance]}
	for _, item := range []struct {
		seam, value string
		missing     bool
	}{{"detector_source", s.DetectorSource, p.Detectors == nil}, {"coverage", s.Coverage, p.Coverage == nil}, {"memory_validation", s.MemoryValidation, p.Memory == nil}, {"activation", s.Activation, p.Activation == nil}, {"agent_guidance", s.AgentGuidance, p.Guidance == nil}} {
		if item.missing {
			return nil, &CodePathConfigError{Seam: item.seam, Value: item.value, Reason: "not registered"}
		}
	}
	if len(s.ResponseStages) == 0 {
		return nil, &CodePathConfigError{Seam: "response_stages", Reason: "at least one stage is required"}
	}
	seen := map[string]bool{}
	for _, stage := range s.ResponseStages {
		if !r.stages[stage] || seen[stage] {
			return nil, &CodePathConfigError{Seam: "response_stages", Value: stage, Reason: "unknown or repeated stage"}
		}
		seen[stage] = true
	}
	if seen["deterministic"] && s.Coverage == "none" {
		return nil, &CodePathConfigError{Seam: "response_stages", Value: "deterministic", Reason: "requires a coverage provider other than none"}
	}
	return p, nil
}

type compiledCodePathDetectors struct{}

func (compiledCodePathDetectors) Detectors(_ context.Context, request DetectorSourceRequest) ([]sdk.Detector, error) {
	return request.CompiledDetectors, nil
}

type goCodePathCoverage struct{}

func (goCodePathCoverage) Coverage(f []sdk.Finding, r []ProgramRun, v sdk.DetectionContext, s map[string]ProgramTrust) *contracts.PlaybookCoverage {
	return buildPlaybookCoverage(f, r, v, s)
}

type noCodePathCoverage struct{}

func (noCodePathCoverage) Coverage([]sdk.Finding, []ProgramRun, sdk.DetectionContext, map[string]ProgramTrust) *contracts.PlaybookCoverage {
	return &contracts.PlaybookCoverage{}
}

type goCodePathValidation struct {
	check func(context.Context, string, []string) error
}

func (v goCodePathValidation) Validate(ctx context.Context, root string, paths []string) error {
	if v.check == nil {
		return fmt.Errorf("Go memory validation is not configured")
	}
	return v.check(ctx, root, paths)
}

type configuredCodePathActivation struct {
	name     string
	activate func(context.Context, string) error
}

func (a configuredCodePathActivation) Activate(ctx context.Context, commit string) error {
	if a.activate == nil {
		return fmt.Errorf("%s activation is not configured", a.name)
	}
	return a.activate(ctx, commit)
}

type codePathGuidance string

func (v codePathGuidance) Variant(role string) (string, error) {
	switch role {
	case "responder", "reflection", "health_judge":
		return string(v), nil
	default:
		return "", fmt.Errorf("unsupported guidance role %q", role)
	}
}

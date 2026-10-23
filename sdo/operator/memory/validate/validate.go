// Package validate is the single validator for operational memory. The commit
// broker, the Kubernetes validation Job, and agent self-checks all call
// Validate, so a candidate is accepted or rejected for the same reasons
// everywhere.
//
// Validation failures are Diagnostics. The error result is reserved for
// infrastructure failures: unreadable files, a missing Git or Go toolchain, a
// cancelled context, or an invalid Request.
package validate

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Actor is the party that authored a candidate change.
type Actor string

const (
	Human       Actor = "human"
	Deployer    Actor = "deployer"
	HealthJudge Actor = "health_judge"
	Responder   Actor = "responder"
	Upkeep      Actor = "upkeep"
)

// Phase is the lifecycle stage in which a candidate change is proposed.
type Phase string

const (
	ProposalPhase   Phase = "proposal"
	ReflectionPhase Phase = "reflection"
	InitialPhase    Phase = "initial"
	OutcomePhase    Phase = "outcome"
)

// Layer selects a group of rules.
type Layer uint8

const (
	// Structural: every .sdo/ path classifies; no symlinks, secrets, or
	// oversized files; YAML, front matter, and Go syntax parse; schema-version.
	Structural Layer = 1 << iota
	// Semantic: ID patterns, unique IDs, cross-references, executable and
	// referenced scripts, no physical memory paths in authored text.
	Semantic
	// Ownership: the diff against Base must be allowed for the actor and phase.
	Ownership
	// Compile: detectors build against the SDK, pass go vet, and pass their
	// own tests. It executes detector code and so belongs in an isolated Job.
	Compile

	// Static is every layer that only reads files.
	Static = Structural | Semantic | Ownership
	// All is every layer.
	All = Static | Compile
)

// Request describes one validation run.
type Request struct {
	// ObservationCeiling is caller-owned installer policy, never loaded from candidate memory.
	ObservationCeiling   sdk.ObservationScope
	ApplicationNamespace string
	// MitigationReplayNotes optionally receives informational corpus replay results.
	MitigationReplayNotes *[]sdktest.MitigationReplayResult
	// StageTimings optionally receives the Go toolchain stage durations of the compile layer.
	StageTimings *[]builder.StageTiming
	// Root is the checkout to validate.
	Root string
	// Publication is broker-derived context. CLI callers load its provisioned
	// copy from the proposal's private Git directory; the broker never trusts it.
	Publication *PublicationContext
	// Decision is the intended reflection result, checked against the actual diff.
	Decision *contracts.ReflectionDecision
	// Base is the Git revision, resolvable inside Root, that the candidate
	// changes. Ownership needs it; with it, memory is validated only when the
	// diff touches it. Without it, the whole of Root is validated.
	Base string
	// Actor and Phase say who proposes the change and when. Ownership needs
	// Actor. Phase also decides whether memory must be complete: the
	// bootstrap stages before the health judge accept partial memory.
	Actor Actor
	Phase Phase
	// Layers selects the rules to run. Zero means All.
	Layers Layer
	// OperatorRoot holds the operator Go modules and is required for Compile.
	OperatorRoot string
	// HealthyBaseline optionally names recorded healthy snapshots that
	// incident detectors must stay quiet on during Compile.
	HealthyBaseline string
	// CaptureRoot and IncidentID bind proposed corpus to controller-owned captures.
	CaptureRoot      string
	CaptureDirectory string
	IncidentID       string
}

// Diagnostic is one validation finding. Path is repository-relative and Line is
// zero when the finding is not tied to a line.
type Diagnostic struct {
	Rule    string
	Path    string
	Line    int
	Message string
	Hint    string
}

func (d Diagnostic) String() string {
	var b strings.Builder
	if d.Path != "" {
		b.WriteString(d.Path)
		if d.Line > 0 {
			fmt.Fprintf(&b, ":%d", d.Line)
		}
		b.WriteString(": ")
	}
	fmt.Fprintf(&b, "[%s] %s", d.Rule, d.Message)
	if d.Hint != "" {
		fmt.Fprintf(&b, " (hint: %s)", d.Hint)
	}
	return b.String()
}

// Failure is the error form of a non-empty diagnostic list.
type Failure struct{ Diagnostics []Diagnostic }

// Err returns nil for an empty list and a *Failure otherwise.
func Err(diagnostics []Diagnostic) error {
	if len(diagnostics) == 0 {
		return nil
	}
	return &Failure{Diagnostics: diagnostics}
}

func (f *Failure) Error() string {
	lines := make([]string, 0, len(f.Diagnostics)+1)
	lines = append(lines, fmt.Sprintf("%d validation diagnostic(s)", len(f.Diagnostics)))
	for _, d := range f.Diagnostics {
		lines = append(lines, "  "+d.String())
	}
	return strings.Join(lines, "\n")
}

func (r Request) effectiveLayers() Layer {
	if r.Layers == 0 {
		return All
	}
	return r.Layers
}

func (r Request) check() error {
	if r.Root == "" {
		return errors.New("validate: root is required")
	}
	layers := r.effectiveLayers()
	if layers&^All != 0 {
		return fmt.Errorf("validate: unknown layers %#b", layers)
	}
	if layers&Ownership != 0 && (r.Base == "" || r.Actor == "") {
		return errors.New("validate: the ownership layer requires base and actor")
	}
	if layers&Compile != 0 && r.OperatorRoot == "" {
		return errors.New("validate: the compile layer requires the operator root")
	}
	return nil
}

// Validate runs the selected layers and returns every diagnostic found, sorted
// by path, line, and rule. Compile runs only when no earlier layer reported a
// diagnostic, since it would only repeat their findings.
func Validate(ctx context.Context, request Request) ([]Diagnostic, error) {
	if err := request.check(); err != nil {
		return nil, err
	}
	layers := request.effectiveLayers()
	s := &run{req: request}
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	if layers&Semantic != 0 {
		if err := s.semantic(ctx); err != nil {
			return nil, err
		}
	}
	if layers&Ownership != 0 {
		if err := s.ownership(ctx); err != nil {
			return nil, err
		}
	}
	if layers&Compile != 0 && len(s.diags) == 0 {
		if err := s.compile(ctx); err != nil {
			return nil, err
		}
	}
	sort.SliceStable(s.diags, func(i, j int) bool {
		a, b := s.diags[i], s.diags[j]
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Rule < b.Rule
	})
	return s.diags, nil
}

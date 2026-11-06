package operability

// Cause is the closed set of reasons a check can fail. Adapters set it from
// typed errors; routing is by Cause alone.
type Cause string

const (
	CausePatch                  Cause = "patch"
	CauseRender                 Cause = "render"
	CauseRenderNondeterministic Cause = "render_nondeterministic"
	CauseBuild                  Cause = "build"
	CauseRollout                Cause = "rollout"
	CauseAttribution            Cause = "attribution"
	CauseScope                  Cause = "scope"
	CauseAllowlist              Cause = "allowlist"
	CauseBudget                 Cause = "budget"
	CauseBinding                Cause = "binding"
	CauseCoverage               Cause = "coverage"
	CauseMemoryInvalid          Cause = "memory_invalid"
	CauseDetectorFault          Cause = "detector_fault"
	CauseSourceFault            Cause = "source_fault"
	CauseProbeUnavailable       Cause = "probe_unavailable"
	CauseInfrastructure         Cause = "infrastructure"
	CauseEnvironment            Cause = "environment"
	CauseInternal               Cause = "internal"
)

// Causes lists every Cause constant. A test keeps it equal to the constants
// declared in this package.
func Causes() []Cause {
	return []Cause{
		CausePatch, CauseRender, CauseRenderNondeterministic, CauseBuild, CauseRollout,
		CauseAttribution, CauseScope, CauseAllowlist, CauseBudget, CauseBinding,
		CauseCoverage, CauseMemoryInvalid, CauseDetectorFault, CauseSourceFault,
		CauseProbeUnavailable, CauseInfrastructure, CauseEnvironment, CauseInternal,
	}
}

func (c Cause) known() bool {
	_, ok := route(c)
	return ok
}

// Owner is who must act on a failing check.
type Owner string

const (
	// OwnerDeployer corrects deployment files, plan and patches.
	OwnerDeployer Owner = "deployer"
	// OwnerDesigner corrects detectors, probes and operational memory.
	OwnerDesigner Owner = "designer"
	// OwnerResponder handles the failure as an incident.
	OwnerResponder Owner = "responder"
	// OwnerHuman is terminal: the failure needs a person.
	OwnerHuman Owner = "human"
	// OwnerOperator is a defect in SDO's own Go path: fail the run, no agent turn.
	OwnerOperator Owner = "operator"
	// OwnerRetry is infrastructure: retry without costing an attempt.
	OwnerRetry Owner = "retry"
)

// Mode is the situation a failure is routed in.
type Mode string

const (
	// ModeFresh is onboarding where the deployer authored the deployment.
	ModeFresh Mode = "fresh"
	// ModeProvided is an install of a human-provided deployment plan.
	ModeProvided Mode = "provided"
	// ModeAdvance is an upstream advance of an installed deployment.
	ModeAdvance Mode = "advance"
)

type routing struct {
	fresh, provided, advance Owner
	// rank orders worst-cause precedence, lowest first: infrastructure (a flaky
	// cluster is never blamed on an agent), SDO's own defects, deployer
	// causes, then designer causes.
	rank int
}

const (
	rankInfrastructure = iota
	rankInternal
	rankDeployer
	rankDesigner
)

// route is the one cause-to-owner table.
func route(c Cause) (routing, bool) {
	switch c {
	case CausePatch, CauseRender, CauseRenderNondeterministic, CauseBuild:
		return routing{OwnerDeployer, OwnerHuman, OwnerDeployer, rankDeployer}, true
	case CauseRollout:
		return routing{OwnerDeployer, OwnerHuman, OwnerResponder, rankDeployer}, true
	case CauseScope, CauseAllowlist, CauseBudget:
		return routing{OwnerDeployer, OwnerHuman, OwnerHuman, rankDeployer}, true
	case CauseSourceFault:
		return routing{OwnerDeployer, OwnerHuman, OwnerResponder, rankDeployer}, true
	case CauseAttribution:
		return routing{OwnerOperator, OwnerOperator, OwnerOperator, rankInternal}, true
	case CauseBinding, CauseCoverage, CauseMemoryInvalid, CauseProbeUnavailable, CauseDetectorFault:
		return routing{OwnerDesigner, OwnerDesigner, OwnerDesigner, rankDesigner}, true
	case CauseInfrastructure:
		return routing{OwnerRetry, OwnerRetry, OwnerRetry, rankInfrastructure}, true
	case CauseEnvironment, CauseInternal:
		return routing{OwnerHuman, OwnerHuman, OwnerHuman, rankInternal}, true
	}
	return routing{}, false
}

// Route returns the owner of a cause in fresh onboarding.
func Route(c Cause) Owner { return RouteIn(ModeFresh, c) }

// RouteIn returns the owner of a cause in the mode. An unknown cause or mode
// fails closed to the human.
func RouteIn(mode Mode, c Cause) Owner {
	r, ok := route(c)
	if !ok {
		return OwnerHuman
	}
	switch mode {
	case ModeFresh:
		return r.fresh
	case ModeProvided:
		return r.provided
	case ModeAdvance:
		return r.advance
	}
	return OwnerHuman
}

func rankOf(c Cause) int {
	if r, ok := route(c); ok {
		return r.rank
	}
	return rankInternal
}

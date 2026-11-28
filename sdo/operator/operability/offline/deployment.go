// Package offline computes the operability checks that need no cluster:
// the effective tree renders, images are declared or derived consistently,
// scope and registry limits hold. It only adapts results the owning packages
// already compute (rendered, imagesrc) into
// operability.Check values. Renderers are pure by construction (render's
// purity tests), so a tree renders once.
package offline

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/rendered"
)

// maxListed bounds how many names a check detail lists.
const maxListed = 5

// Policy carries the install-time limits the offline checks compare the desired
// set with. The zero value grants nothing and configures no registry allowlist.
type Policy struct {
	// ScopeBlocking makes foreign-namespace and cluster-scoped objects fail
	// O6.scope. Off by default: they are recorded as notes until the apps that
	// rely on them are inventoried (design decision D5).
	ScopeBlocking bool
	// GrantForeign and GrantClusterScoped are install grants that permit those
	// objects even when ScopeBlocking is on.
	GrantForeign, GrantClusterScoped bool
	// GrantedNamespaces permit foreign-namespace objects in those namespaces,
	// and GrantedClusterKinds permit cluster-scoped objects of those Kinds
	// (the cluster-scope ceiling), even when ScopeBlocking is on.
	GrantedNamespaces, GrantedClusterKinds []string
	// AllowedRegistries are the registry hosts images may be pulled from. Empty
	// means no allowlist is configured.
	AllowedRegistries []string
}

// Deployment is everything the offline deployment checks read.
type Deployment struct {
	// TreeErr is the error of computing the effective tree
	// (rendered.Effective); nil when it was computed.
	TreeErr error
	// Render renders the effective tree with the one render environment.
	Render func(context.Context) (rendered.DesiredSet, error)
	// ServedAPIs are the recorded discovery's group/version and
	// group/version/Kind entries; empty is unknown and O1.apis is reported skipped.
	ServedAPIs []string
	// Declare returns the image declarations for a rendered set: the plan's,
	// plus a build for every image the tree has a Dockerfile for
	// (imagesrc.Catalog.Derive). Nil declares nothing.
	Declare func(rendered.DesiredSet) ([]imagesrc.Declared, error)
	Policy  Policy
}

// Result is the checks of O1, O2 and O6 plus the desired set of the first
// render (zero when the tree or render failed), from which callers take the
// render digests for the report subject.
type Result struct {
	Checks []operability.Check
	Set    rendered.DesiredSet
	// UnusedImages are the plan's repository-form image declarations that no
	// rendered container uses (see imagesrc.UnusedRepositories).
	UnusedImages []string
}

// CheckDeployment runs the cheap-first offline deployment checks. A tree or
// render failure stops the sequence: later checks would only repeat it.
func CheckDeployment(ctx context.Context, d Deployment) Result {
	var res Result
	add := func(c operability.Check) { res.Checks = append(res.Checks, c) }
	// stop records the checks a failure kept from running as skipped, so every
	// required item still has checks and the failure alone decides the report.
	stop := func(after string, ids ...string) Result {
		for _, id := range ids {
			add(operability.Check{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusSkipped, Detail: "not run: " + after + " failed"})
		}
		return res
	}

	if d.TreeErr != nil {
		add(failure("O1.render", classifyRender(d.TreeErr, operability.CauseInternal), d.TreeErr, nil))
		return stop("O1.render", afterRender()...)
	}
	if d.Render == nil {
		add(operability.Check{ID: "O1.render", Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: operability.CauseInternal, Detail: "no render function was supplied"})
		return stop("O1.render", afterRender()...)
	}
	first, err := d.Render(ctx)
	if err != nil {
		add(failure("O1.render", classifyRender(err, operability.CauseRender), err, nil))
		return stop("O1.render", afterRender()...)
	}
	res.Set = first
	add(pass("O1.render", fmt.Sprintf("renders %d objects", len(first.Objects()))))

	switch {
	case len(d.ServedAPIs) == 0:
		add(operability.Check{ID: "O1.apis", Phase: operability.PhaseOffline, Status: operability.StatusSkipped, Detail: "not run: no cluster discovery was recorded"})
	default:
		if err := rendered.CheckServedAPIs(first, d.ServedAPIs); err != nil {
			add(failure("O1.apis", operability.CauseRender, err, nil))
		} else {
			add(pass("O1.apis", "rendered objects use API versions the cluster serves"))
		}
	}

	var declared []imagesrc.Declared
	if d.Declare != nil {
		declared, err = d.Declare(first)
	}
	if err != nil {
		add(failure("O2.images", operability.CauseBuild, err, nil))
	} else {
		add(images(first, declared))
		res.UnusedImages = imagesrc.UnusedRepositories(rendered.ImageSites(first), declared)
	}
	for _, c := range CheckPolicy(first, d.Policy) {
		add(c)
	}
	return res
}

// CheckPolicy runs the install-policy checks (O6 scope and allowlist) over a
// desired set. The deployer runs the same function before it applies, so the
// advisory report and the enforcement cannot differ.
func CheckPolicy(set rendered.DesiredSet, p Policy) []operability.Check {
	return []operability.Check{scope(set, p), allowlist(set, p)}
}

// DeploymentCheckIDs lists every check CheckDeployment reports when nothing
// fails, in order. Adapters that stand in for the offline gate use it so they
// cannot drift from the real producer.
func DeploymentCheckIDs() []string {
	return append([]string{"O1.render"}, afterRender()...)
}

// Checks that depend on an earlier one, in the order CheckDeployment runs them.
func afterRender() []string {
	return []string{"O1.apis", "O2.images", "O6.scope", "O6.allowlist"}
}

func pass(id, detail string) operability.Check {
	return operability.Check{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusPass, Detail: operability.Clip(detail)}
}

func note(id, detail string, paths ...string) operability.Check {
	return operability.Check{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusNote, Detail: operability.Clip(detail), Paths: paths}
}

func failure(id string, cause operability.Cause, err error, paths []string) operability.Check {
	return operability.Check{ID: id, Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: cause, Detail: operability.Clip(err.Error()), Paths: paths}
}

// ClassifyRender maps a render or source error to a cause for adapters outside
// this package (the live deployment checks use the same table). Cancellation and
// unavailable I/O are infrastructure; an invalid or unconfigured source is
// invalid; an error of an unknown shape is internal.
func ClassifyRender(err error, invalid operability.Cause) operability.Cause {
	return classifyRender(err, invalid)
}

// classifyRender maps a render or source error: cancellation and unavailable
// I/O are infrastructure, an invalid or unconfigured source is the given cause,
// and an error of an unknown shape is an internal defect.
func classifyRender(err error, invalid operability.Cause) operability.Cause {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return operability.CauseInfrastructure
	}
	var source *rendered.SourceError
	if errors.As(err, &source) {
		switch source.Kind {
		case rendered.SourceUnavailable:
			return operability.CauseInfrastructure
		case rendered.SourceInvalid, rendered.SourceUnconfigured:
			return invalid
		}
	}
	return operability.CauseInternal
}

// images applies the declaration rules ResolveAll enforces, before any
// cluster effect, and notes how many sites rely on the default resolver.
func images(set rendered.DesiredSet, declared []imagesrc.Declared) operability.Check {
	sites := rendered.ImageSites(set)
	if err := imagesrc.CheckDeclared(sites, declared); err != nil {
		return failure("O2.images", operability.CauseBuild, err, nil)
	}
	var undeclared []string
	seen := map[string]bool{}
	for _, image := range sites {
		if _, ok := imagesrc.Match(declared, image); !ok && !strings.Contains(image, "@sha256:") && !seen[image] {
			seen[image] = true
			undeclared = append(undeclared, image)
		}
	}
	if len(undeclared) > 0 {
		sort.Strings(undeclared)
		return note("O2.images", fmt.Sprintf("%d of %d image sites have no declaration and resolve through the default source: %s", len(undeclared), len(sites), list(undeclared)))
	}
	return pass("O2.images", fmt.Sprintf("%d image sites are declared or digest-pinned", len(sites)))
}

// scope counts objects outside the application namespace. Without a grant they
// are notes (decision D5) or, with Policy.ScopeBlocking, failures.
func scope(set rendered.DesiredSet, p Policy) operability.Check {
	var foreign, cluster []string
	for _, o := range set.Objects() {
		name := o.Ref.Kind + "/" + o.Ref.Name
		switch {
		case o.Foreign:
			if !slices.Contains(p.GrantedNamespaces, o.Ref.Namespace) {
				foreign = append(foreign, o.Ref.Namespace+"/"+name)
			}
		case o.Ref.Namespace == "" && rendered.ClusterScopedKind(o.Ref.Kind):
			if !slices.Contains(p.GrantedClusterKinds, o.Ref.Kind) {
				cluster = append(cluster, name)
			}
		}
	}
	sort.Strings(foreign)
	sort.Strings(cluster)
	var out []string
	if len(foreign) > 0 && !p.GrantForeign {
		out = append(out, fmt.Sprintf("%d foreign-namespace objects: %s", len(foreign), list(foreign)))
	}
	if len(cluster) > 0 && !p.GrantClusterScoped {
		out = append(out, fmt.Sprintf("%d cluster-scoped objects: %s", len(cluster), list(cluster)))
	}
	if len(out) == 0 {
		return pass("O6.scope", "every object is in the application namespace or granted")
	}
	detail := strings.Join(out, "; ")
	if p.ScopeBlocking {
		return operability.Check{ID: "O6.scope", Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: operability.CauseScope, Detail: operability.Clip(detail)}
	}
	return note("O6.scope", detail)
}

// allowlist compares the registry host of every image site with the install
// allowlist. With none configured the check is skipped, never passed.
func allowlist(set rendered.DesiredSet, p Policy) operability.Check {
	if len(p.AllowedRegistries) == 0 {
		return operability.Check{ID: "O6.allowlist", Phase: operability.PhaseOffline, Status: operability.StatusSkipped, Detail: "no image registry allowlist is configured"}
	}
	allowed := map[string]bool{}
	for _, host := range p.AllowedRegistries {
		allowed[strings.ToLower(host)] = true
	}
	var bad []string
	seen := map[string]bool{}
	for _, image := range rendered.ImageSites(set) {
		if host := RegistryHost(image); !allowed[host] && !seen[image] {
			seen[image] = true
			bad = append(bad, image)
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		return operability.Check{
			ID: "O6.allowlist", Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: operability.CauseAllowlist,
			Detail: fmt.Sprintf("%d images are outside the registry allowlist: %s", len(bad), list(bad)),
		}
	}
	return pass("O6.allowlist", "every image registry is allowlisted")
}

// RegistryHost returns the registry host of an image reference, lowercased:
// the first path segment when it names a host (contains "." or ":" or is
// localhost), docker.io otherwise.
func RegistryHost(image string) string {
	first, _, hasPath := strings.Cut(image, "/")
	if hasPath && (strings.ContainsAny(first, ".:") || first == "localhost") {
		return strings.ToLower(first)
	}
	return "docker.io"
}

func list(names []string) string {
	if len(names) <= maxListed {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:maxListed], ", ") + fmt.Sprintf(" and %d more", len(names)-maxListed)
}

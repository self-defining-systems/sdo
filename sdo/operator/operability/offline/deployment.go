// Package offline computes the operability checks that need no cluster:
// patches apply, the effective tree renders deterministically, expected
// workloads match, images are declared consistently, scope and registry limits
// hold. It only adapts results the owning packages already compute (rendered,
// patchset, imagesrc) into operability.Check values; it renders nothing a
// second time except the one determinism comparison.
package offline

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/patchset"
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
	// AllowedRegistries are the registry hosts images may be pulled from. Empty
	// means no allowlist is configured.
	AllowedRegistries []string
}

// Deployment is everything the offline deployment checks read.
type Deployment struct {
	// PatchesErr is the error of applying the patch series
	// (rendered.Effective); nil when the series applied.
	PatchesErr error
	// Render renders the effective tree with the one render environment. It is
	// called twice to check determinism, so it must not memoize.
	Render func(context.Context) (rendered.DesiredSet, error)
	// ExpectedWorkloads are the plan's expected workload IDs.
	ExpectedWorkloads []string
	// Declared are the plan's image declarations.
	Declared []imagesrc.Declared
	Policy   Policy
}

// Result is the checks of O1, O2 and O6 plus the desired set of the first
// render (zero when patches or render failed), from which callers take the
// render digests for the report subject.
type Result struct {
	Checks []operability.Check
	Set    rendered.DesiredSet
}

// CheckDeployment runs the cheap-first offline deployment checks. A patch or
// render failure stops the sequence: later checks would only repeat it.
func CheckDeployment(ctx context.Context, d Deployment) Result {
	var res Result
	add := func(c operability.Check) { res.Checks = append(res.Checks, c) }

	if d.PatchesErr != nil {
		add(failure("O1.patches", classifyPatches(d.PatchesErr), d.PatchesErr, patchPaths(d.PatchesErr)))
		return res
	}
	add(pass("O1.patches", "patch series applies"))

	if d.Render == nil {
		add(operability.Check{ID: "O1.render", Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: operability.CauseInternal, Detail: "no render function was supplied"})
		return res
	}
	first, err := d.Render(ctx)
	if err != nil {
		add(failure("O1.render", classifyRender(err, operability.CauseRender), err, nil))
		return res
	}
	res.Set = first
	add(pass("O1.render", fmt.Sprintf("renders %d objects", len(first.Objects()))))

	add(determinism(ctx, d, first))

	if err := rendered.CheckExpectedWorkloads(first, d.ExpectedWorkloads); err != nil {
		add(failure("O1.workloads", operability.CauseRender, err, nil))
	} else {
		add(pass("O1.workloads", "expected workloads match the rendered workloads"))
	}

	add(images(first, d.Declared))
	add(scope(first, d.Policy))
	add(allowlist(first, d.Policy))
	return res
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

// classifyPatches maps the result of applying the series. A patch error is the
// deployer's; unavailable git I/O is infrastructure; anything else is a defect.
func classifyPatches(err error) operability.Cause {
	var patchErr *patchset.PatchError
	if errors.As(err, &patchErr) {
		return operability.CausePatch
	}
	return classifyRender(err, operability.CauseInternal)
}

func patchPaths(err error) []string {
	var patchErr *patchset.PatchError
	if errors.As(err, &patchErr) {
		paths := append([]string(nil), patchErr.Paths...)
		if patchErr.Patch != "" {
			paths = append([]string{patchset.Directory + "/" + patchErr.Patch}, paths...)
		}
		return paths
	}
	return nil
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

// determinism renders again and requires equal digests: a render whose bytes
// depend on anything but the tree and the environment cannot be verified on
// restart (VerifyAccepted compares digests).
func determinism(ctx context.Context, d Deployment, first rendered.DesiredSet) operability.Check {
	second, err := d.Render(ctx)
	if err != nil {
		if cause := classifyRender(err, operability.CauseRenderNondeterministic); cause != operability.CauseInternal {
			return failure("O1.deterministic", cause, fmt.Errorf("second render of the same tree failed: %w", err), nil)
		}
		return failure("O1.deterministic", operability.CauseInternal, err, nil)
	}
	var differing []string
	for _, f := range []struct{ name, a, b string }{
		{"render", first.RenderDigest, second.RenderDigest},
		{"shape", first.ShapeDigest, second.ShapeDigest},
		{"env", first.EnvDigest, second.EnvDigest},
	} {
		if f.a != f.b {
			differing = append(differing, f.name)
		}
	}
	if len(differing) > 0 {
		return operability.Check{
			ID: "O1.deterministic", Phase: operability.PhaseOffline, Status: operability.StatusFail,
			Cause:  operability.CauseRenderNondeterministic,
			Detail: "two renders of the same tree and environment differ in digest: " + strings.Join(differing, ", "),
		}
	}
	return pass("O1.deterministic", "two renders of the same tree agree")
}

// images applies the declaration rules ResolveAll enforces, before any
// cluster effect, and notes how many sites rely on the default resolver.
func images(set rendered.DesiredSet, declared []imagesrc.Declared) operability.Check {
	sites := rendered.ImageSites(set)
	if err := imagesrc.CheckDeclared(sites, declared); err != nil {
		return failure("O2.images", operability.CauseBuild, err, nil)
	}
	declaredSet := map[string]bool{}
	for _, d := range declared {
		declaredSet[d.Image] = true
	}
	var undeclared []string
	seen := map[string]bool{}
	for _, image := range sites {
		if !declaredSet[image] && !strings.Contains(image, "@sha256:") && !seen[image] {
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
			foreign = append(foreign, o.Ref.Namespace+"/"+name)
		case o.Ref.Namespace == "" && rendered.ClusterScopedKind(o.Ref.Kind):
			cluster = append(cluster, name)
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

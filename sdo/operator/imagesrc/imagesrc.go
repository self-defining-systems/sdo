// Package imagesrc is the image-resolution seam of deployment. A Source turns
// one container image reference named by a rendered object into an immutable
// digest reference; the Catalog owns everything around that call: dedupe, a
// time budget per source, and verification of what the source returned.
//
// The package imports only the standard library and protobuf. It does not
// import render: Request carries the tree as stdlib fs.FS. Implementations
// (prebuilt, buildkit) live in subpackages and are wired in
// operatorcli/catalog.
package imagesrc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Request is one container image to resolve.
type Request struct {
	Image    string // reference exactly as the rendered object names it
	Tree     string // effective tree the objects came from
	FS       fs.FS  // the same tracked-only tree view (stdlib type; no render import)
	Checkout string // materialized checkout of Tree, for builders that need a directory
}

// Inputs is what produced the bytes; carried into DeployedImage evidence.
type Inputs struct {
	SourceImage    string `json:"source_image"`
	ContextPath    string `json:"context_path,omitempty"`
	DockerfilePath string `json:"dockerfile_path,omitempty"`
}

// Resolved is a source's answer for one Request.
type Resolved struct {
	Ref    string // repository@sha256:<64 hex>; verified by the catalog
	Inputs Inputs
}

// Source resolves images for one config type C.
type Source[C proto.Message] interface {
	Resolve(ctx context.Context, req Request, config C) (Resolved, error)
}

// Declared is the plan's choice of source for one image: the decoded form of
// contracts.ImageSource, so core never names a config type. A nil Config means
// the source's empty config.
type Declared struct {
	Image    string
	SourceID string
	Config   *anypb.Any
}

// Every error type of this package that can be an infrastructure outcome
// (ResolveError, TimeoutError) implements InfrastructureOutcome() bool, the one
// marker the lifecycle coordinator checks. Implementations that add a classified
// failure (for example the BuildKit build failure) implement the same method.

// Error attributes a failure to the image and the source that produced it. It
// unwraps to the source's own typed cause (ResolveError, BuildFailureError, and
// so on), so callers keep using errors.As.
type Error struct {
	Image    string
	SourceID string
	Err      error
}

func (e *Error) Error() string {
	return fmt.Sprintf("resolve image %s with source %s: %v", e.Image, e.SourceID, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// ResolveErrorKind says what to do about a failed resolution.
type ResolveErrorKind string

const (
	// ResolveTransient is a rate limit, timeout or network failure: the reference
	// may be fine, so retrying later is the right advice.
	ResolveTransient ResolveErrorKind = "transient"
	// ResolveMissing is a registry that has no such repository or tag.
	ResolveMissing ResolveErrorKind = "missing"
	// ResolveUnauthorized is an image that needs credentials (private).
	ResolveUnauthorized ResolveErrorKind = "unauthorized"
	// ResolveRefused is a target the operator will not contact, or an invalid reference.
	ResolveRefused ResolveErrorKind = "refused"
)

// ResolveError is a classified resolution failure. Core inspects it with
// errors.As, so it is defined here and implementations return it.
type ResolveError struct {
	Kind ResolveErrorKind
	Err  error
}

func (e *ResolveError) Error() string { return e.Err.Error() }
func (e *ResolveError) Unwrap() error { return e.Err }

// InfrastructureOutcome marks a rate limit, timeout or network failure: the
// reference may be fine, so the coordinator retries instead of blaming the plan.
func (e *ResolveError) InfrastructureOutcome() bool { return e.Kind == ResolveTransient }

// TimeoutError reports that resolving an image ran out of time. It is an
// infrastructure outcome, not a verdict on the plan. ResolveAll returns it
// when a source's shared budget expires while the caller's context is live;
// a source may return it for its own per-image deadline.
type TimeoutError struct {
	Image string
	Limit time.Duration
	// Budget is true when the shared budget of all images of the source ran
	// out, false when the single resolution hit its own deadline.
	Budget bool
}

func (e *TimeoutError) Error() string {
	scope := fmt.Sprintf("its per-image deadline of %s", e.Limit)
	if e.Budget {
		scope = fmt.Sprintf("the shared budget of %s for all images of its source", e.Limit)
	}
	return fmt.Sprintf("resolving image %s timed out: it exceeded %s. This is a time limit, not a plan error: resubmit the same plan unchanged", e.Image, scope)
}

// Unwrap lets errors.Is(err, context.DeadlineExceeded) hold.
func (e *TimeoutError) Unwrap() error { return context.DeadlineExceeded }

// InfrastructureOutcome marks the error as a time limit, not a plan error. It
// is the one marker the lifecycle coordinator checks.
func (e *TimeoutError) InfrastructureOutcome() bool { return true }

var digestReference = regexp.MustCompile(`^[^@\s]+@sha256:[a-f0-9]{64}$`)

// ResolveAll resolves every distinct image once, using its declared source or
// the catalog default. Images are resolved sequentially in the order given, so
// the result and any error do not depend on scheduling. It derives one context
// per source from that source's Budget, started at the source's first image,
// and verifies every Ref: a digest reference, and for a source registered with
// Options.SameRepository, in the repository of the requested image. The result
// maps each requested image to its Resolved.
//
// base supplies Tree, FS and Checkout; its Image is ignored. Declared entries
// must name distinct images that appear in images and registered sources.
func (c *Catalog) ResolveAll(ctx context.Context, images []string, declared []Declared, base Request) (map[string]Resolved, error) {
	if c == nil {
		return nil, errors.New("imagesrc: nil catalog")
	}
	for _, image := range images {
		if image == "" {
			return nil, errors.New("imagesrc: empty image reference")
		}
	}
	wanted := make(map[string]bool, len(images))
	for _, image := range images {
		wanted[image] = true
	}
	choice := make(map[string]Declared, len(declared))
	for _, d := range declared {
		if !wanted[d.Image] {
			return nil, fmt.Errorf("imagesrc: image %q is declared with source %q but no rendered container uses it", d.Image, d.SourceID)
		}
		if prior, dup := choice[d.Image]; dup {
			return nil, fmt.Errorf("imagesrc: image %q is declared twice (sources %q and %q)", d.Image, prior.SourceID, d.SourceID)
		}
		if _, ok := c.entries[d.SourceID]; !ok {
			return nil, fmt.Errorf("imagesrc: image %q names unknown source %q (registered: %v)", d.Image, d.SourceID, c.IDs())
		}
		choice[d.Image] = d
	}

	budgets := map[string]context.Context{}
	var stops []context.CancelFunc
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	out := make(map[string]Resolved, len(images))
	for _, image := range images {
		if _, done := out[image]; done {
			continue
		}
		d, ok := choice[image]
		if !ok {
			if c.pushHost != "" && registryHost(image) == c.pushHost {
				return nil, fmt.Errorf("imagesrc: image %q is under the trusted push registry %q but no source is declared for it; images the operator pushes must be declared (a typo or a rewritten name would otherwise pin a stale earlier push)", image, c.pushHost)
			}
			if c.defaultID == "" {
				return nil, fmt.Errorf("imagesrc: image %q declares no source and the catalog has no default", image)
			}
			d = Declared{Image: image, SourceID: c.defaultID}
		}
		e := c.entries[d.SourceID]
		config, err := c.Decode(d.SourceID, d.Config)
		if err != nil {
			return nil, &Error{Image: image, SourceID: d.SourceID, Err: err}
		}
		sourceCtx, started := budgets[d.SourceID]
		if !started {
			sourceCtx = ctx
			if e.budget > 0 {
				var stop context.CancelFunc
				sourceCtx, stop = context.WithTimeout(ctx, e.budget)
				stops = append(stops, stop)
			}
			budgets[d.SourceID] = sourceCtx
		}
		req := base
		req.Image = image
		resolved, err := e.resolve(sourceCtx, req, config)
		if err != nil {
			if ctx.Err() == nil && e.budget > 0 && errors.Is(sourceCtx.Err(), context.DeadlineExceeded) {
				return nil, &TimeoutError{Image: image, Limit: e.budget, Budget: true}
			}
			return nil, &Error{Image: image, SourceID: d.SourceID, Err: err}
		}
		if err := verify(image, resolved.Ref, e.sameRepository); err != nil {
			return nil, &Error{Image: image, SourceID: d.SourceID, Err: err}
		}
		out[image] = resolved
	}
	return out, nil
}

func verify(image, ref string, sameRepository bool) error {
	if !digestReference.MatchString(ref) {
		return fmt.Errorf("source returned a mutable or malformed reference %q", ref)
	}
	if sameRepository && Repository(ref) != Repository(image) {
		return fmt.Errorf("source returned %q, which is outside the requested repository %q", ref, Repository(image))
	}
	return nil
}

// registryHost is the first path segment of an image reference.
func registryHost(image string) string {
	for i := 0; i < len(image); i++ {
		if image[i] == '/' {
			return image[:i]
		}
	}
	return ""
}

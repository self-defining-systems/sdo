package deployment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sdo.dev/operator/imagesrc"
)

// BuildTimeoutError reports that an image build ran out of time. It is an
// infrastructure outcome, not a verdict on the plan: the deployer must not
// rewrite a plan whose paths were already validated.
type BuildTimeoutError struct {
	Image string
	Limit time.Duration
	// Budget is true when the shared budget of all builds ran out, false when
	// the single build hit its own deadline.
	Budget bool
}

func (e *BuildTimeoutError) Error() string {
	scope := fmt.Sprintf("its per-build deadline of %s", e.Limit)
	if e.Budget {
		scope = fmt.Sprintf("the shared build budget of %s for all image builds", e.Limit)
	}
	return fmt.Sprintf("building image %s timed out: it exceeded %s. THE PLAN WAS FINE: this is a time limit, not a plan error, and nothing about the context, Dockerfile, base images or package mirrors was judged wrong. "+
		"Resubmit exactly the same image_builds and leave the Dockerfile, base images, package sources and context_path unchanged; the build only needs more time, and the operator retries it with the registry layer cache of earlier builds. "+
		"Rewriting the plan after this error makes it worse", e.Image, scope)
}

// InfrastructureOutcome marks the error for the lifecycle coordinator, which
// retries the trusted step instead of consuming a deployer attempt.
func (e *BuildTimeoutError) InfrastructureOutcome() bool { return true }

// buildTimeout classifies err as a timeout when the shared budget expired or
// the backend reported its per-build deadline, while the caller's own context
// is still live; the builder reports its own deadline as *imagesrc.TimeoutError.
// It returns nil for every other failure.
func buildTimeout(parent, build context.Context, image string, budget time.Duration, err error) error {
	if parent.Err() != nil {
		return nil
	}
	if budget > 0 && errors.Is(build.Err(), context.DeadlineExceeded) {
		return &BuildTimeoutError{Image: image, Limit: budget, Budget: true}
	}
	var existing *imagesrc.TimeoutError
	if errors.As(err, &existing) {
		return existing
	}
	return nil
}

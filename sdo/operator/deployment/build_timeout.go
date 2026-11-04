package deployment

import (
	"context"
	"errors"
	"fmt"
	"time"
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
	return fmt.Sprintf("building image %s timed out: it exceeded %s. This is a build-time limit, not a plan error: the plan's paths were validated against the checkout. "+
		"Resubmit the same image_builds unchanged unless the Dockerfile itself can be made cheaper (for example installing a prebuilt package instead of compiling from source). "+
		"Do not change context_path or dockerfile_path because of this error", e.Image, scope)
}

// InfrastructureTimeout marks the error for the lifecycle coordinator, which
// words the deployer's correction feedback accordingly.
func (e *BuildTimeoutError) InfrastructureTimeout() bool { return true }

// buildTimeout classifies err as a timeout when the shared budget expired or
// the backend reported its per-build deadline, while the caller's own context
// is still live. It returns nil for every other failure.
func buildTimeout(parent, build context.Context, image string, budget time.Duration, err error) error {
	if parent.Err() != nil {
		return nil
	}
	if budget > 0 && errors.Is(build.Err(), context.DeadlineExceeded) {
		return &BuildTimeoutError{Image: image, Limit: budget, Budget: true}
	}
	var existing *BuildTimeoutError
	if errors.As(err, &existing) {
		return existing
	}
	return nil
}

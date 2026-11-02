package runtime

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/shellpath"
)

// ShellDetectorSource binds a transport to the shell plugin. This keeps the
// registry independent of Kubernetes, HTTP and benchmark adapters.
type ShellDetectorSource struct{ Reader shellpath.Reader }

func (s ShellDetectorSource) Detectors(ctx context.Context, request DetectorSourceRequest) ([]sdk.Detector, error) {
	if s.Reader == nil {
		return nil, fmt.Errorf("shell detector report reader is required")
	}
	output, err := exec.CommandContext(ctx, "git", "-C", request.RepositoryRoot, "rev-parse", "HEAD").Output()
	if err != nil {
		return nil, err
	}
	return []sdk.Detector{&shellpath.Detector{Reader: s.Reader, Namespace: request.Namespace, Commit: strings.TrimSpace(string(output))}}, nil
}

type shellCodePathValidation struct{}

func (shellCodePathValidation) Validate(ctx context.Context, root string, _ []string) error {
	return validate.ShellHygieneValidator(ctx, root, nil)
}

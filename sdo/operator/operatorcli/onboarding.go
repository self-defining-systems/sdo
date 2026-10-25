package operatorcli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/rendered"
)

// Seed adoption is an explicit install event, never an incident-time baseline.
// Verify accepted detectors once; subsequent installs reuse the immutable record
// without building detectors, sampling health, or expanding the identities.
func acknowledgeExistingNamespace(ctx context.Context, client kubernetes.Interface, launch InstallExistingConfig, commit string, probe deployment.HealthProbe) error {
	control := launch.Install.ControlNamespace
	if control == "" {
		control = launch.Install.Namespace
	}
	record, err := onboarding.Load(ctx, client, control, launch.Install.Namespace)
	if err != nil || record != nil {
		return err
	}
	status, err := probe.Evaluate(ctx, launch.Repository, launch.OperatorRoot, launch.Install.Namespace)
	if err != nil {
		return fmt.Errorf("independent onboarding health: %w", err)
	}
	if status != "clear" {
		return fmt.Errorf("independent onboarding health is %s; no identities acknowledged", status)
	}
	var paths []string
	for i, arg := range launch.Install.ExtraControllerArgs {
		if strings.HasPrefix(arg, "--deployed-manifest-path=") {
			paths = append(paths, strings.TrimPrefix(arg, "--deployed-manifest-path="))
		}
		if arg == "--deployed-manifest-path" && i+1 < len(launch.Install.ExtraControllerArgs) {
			paths = append(paths, launch.Install.ExtraControllerArgs[i+1])
		}
	}
	source := rendered.Source{Repository: launch.Repository, ManifestPaths: paths}
	objects, err := source.RawManifests(ctx, commit, launch.Install.Namespace)
	if err != nil {
		return fmt.Errorf("accepted onboarding manifests: %w", err)
	}
	current, err := acceptedHead(ctx, launch.Repository)
	if err != nil {
		return err
	}
	if current != commit {
		return fmt.Errorf("accepted commit changed during onboarding verification")
	}
	_, err = onboarding.Establish(ctx, client, control, launch.Install.Namespace, commit, objects, time.Now())
	return err
}

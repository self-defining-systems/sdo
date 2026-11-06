package operatorcli

import (
	"context"
	"fmt"
	"os"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/install"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/rendered"
)

// Seed adoption is an explicit install event, never an incident-time baseline.
// Freeze identities now; the in-cluster controller verifies them without a
// host-local probe or any blocking startup health check.
func stageExistingNamespace(ctx context.Context, client kubernetes.Interface, launch InstallExistingConfig, commit string) error {
	control := launch.Install.ControlNamespace
	if control == "" {
		control = launch.Install.Namespace
	}
	record, err := onboarding.Load(ctx, client, control, launch.Install.Namespace)
	if err != nil || record != nil {
		return err
	}
	source := rendered.Source{Repository: launch.Repository, Deployment: launch.DeploymentSource, Catalog: renderers.Catalog()}
	objects, err := source.RawManifests(ctx, commit, launch.Install.Namespace)
	if err != nil {
		return fmt.Errorf("accepted onboarding manifests: %w", err)
	}
	current, err := acceptedHead(ctx, launch.Repository)
	if err != nil {
		return err
	}
	if current != commit {
		return fmt.Errorf("accepted commit changed during onboarding staging")
	}
	return onboarding.Stage(ctx, client, control, launch.Install.Namespace, commit, objects, processio.Now())
}

func existingNamespaceOnboardingHook(client kubernetes.Interface, launch InstallExistingConfig, commit string) func(context.Context, install.Config) error {
	return func(ctx context.Context, config install.Config) error {
		if err := stageExistingNamespace(ctx, client, launch, commit); err != nil {
			// Missing install evidence grants no acknowledgement; monitoring proceeds.
			fmt.Fprintln(os.Stderr, "namespace onboarding not staged:", err)
		}
		return nil
	}
}

package lifecycle

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

// InstallStrategy keeps the lifecycle independent of installer transport and
// allows the accepted-repository seeder to be bound for each operation.
type InstallStrategy interface {
	Install(context.Context, install.Config, install.RepositorySeeder) (install.Result, error)
}

type preparedInstallStrategy interface {
	InstallPrepared(context.Context, install.Config, string) (install.Result, error)
}

// KubernetesInstallStrategy runs the concrete Kubernetes installer.
type KubernetesInstallStrategy struct {
	Installer install.Installer
	// Renderers renders the accepted deployment source for onboarding.
	Renderers *render.Catalog
}

func (s KubernetesInstallStrategy) Install(ctx context.Context, config install.Config, seeder install.RepositorySeeder) (install.Result, error) {
	installer := s.Installer
	installer.SeedRepository = seeder
	return installer.Install(ctx, config)
}

func (s KubernetesInstallStrategy) InstallPrepared(ctx context.Context, config install.Config, acceptedRoot string) (install.Result, error) {
	return s.Installer.InstallPrepared(ctx, config, acceptedRoot)
}

// PublishOperability exposes a verified operability report as the
// sdo-operability ConfigMap of the control namespace.
func (s KubernetesInstallStrategy) PublishOperability(ctx context.Context, control string, report operability.Report, observedAt time.Time) error {
	return install.PublishOperability(ctx, s.Installer.Client, control, report, observedAt)
}

// WithVerifiedDeployment binds acknowledgement to this independently healthy
// native deployment; verification is completed by the in-cluster controller.
func (s KubernetesInstallStrategy) WithVerifiedDeployment(root, commit, namespace string, deployment *contracts.DeploymentSource, environment *contracts.RenderEnvironment, pins rendered.ReceiptPins) InstallStrategy {
	s.Installer.BeforeControllerStart = func(ctx context.Context, config install.Config) error {
		control := config.ControlNamespace
		if control == "" {
			control = namespace
		}
		source := rendered.Source{Repository: root, Deployment: deployment, Catalog: s.Renderers, Environment: environment, Pins: pins}
		objects, err := source.RawManifests(ctx, commit, namespace)
		if err != nil {
			return fmt.Errorf("onboarding accepted source: %w", err)
		}
		existing, err := onboarding.Load(ctx, s.Installer.Client, control, namespace)
		if err != nil || existing != nil {
			return err
		}
		return onboarding.Stage(ctx, s.Installer.Client, control, namespace, commit, objects, s.Installer.Observed())
	}
	return s
}

// NativeInstallAdapter installs from the broker's accepted repository commit.
// A worker proposal path is never forwarded to the installer.
type NativeInstallAdapter struct {
	Strategy           InstallStrategy
	Config             install.Config
	AcceptedRepository string
	Prepared           bool
	// Now stamps the exposed operability summary; it is injected, never read
	// from the process clock here.
	Now func() time.Time
}

func (a NativeInstallAdapter) Install(ctx context.Context, acceptedCommit, namespace string) error {
	if a.Strategy == nil || a.AcceptedRepository == "" {
		return errors.New("native installer strategy and accepted repository are required")
	}
	if acceptedCommit == "" || namespace == "" {
		return errors.New("accepted commit and namespace are required for native install")
	}
	commitBytes, err := hex.DecodeString(acceptedCommit)
	if err != nil || len(commitBytes) != 20 {
		return errors.New("native install requires a full accepted SHA-1 commit ID")
	}
	config := a.Config
	config.AcceptedCommit = acceptedCommit
	config.Namespace = namespace
	if a.Prepared {
		prepared, ok := a.Strategy.(preparedInstallStrategy)
		if !ok {
			return errors.New("prepared native installer strategy is required inside operate")
		}
		if _, err := prepared.InstallPrepared(ctx, config, a.AcceptedRepository); err != nil {
			return fmt.Errorf("install accepted commit %s from prepared checkout: %w", acceptedCommit, err)
		}
		return nil
	}
	seeder := install.KubectlTarSeeder(a.AcceptedRepository, acceptedCommit)
	if _, err := a.Strategy.Install(ctx, config, seeder); err != nil {
		return fmt.Errorf("install accepted commit %s: %w", acceptedCommit, err)
	}
	return nil
}

// operabilityStrategy is an install strategy that can expose the report.
type operabilityStrategy interface {
	PublishOperability(ctx context.Context, control string, report operability.Report, observedAt time.Time) error
}

var _ operabilityStrategy = KubernetesInstallStrategy{}

// PublishOperability exposes the verified report in the control namespace
// (Config.ControlNamespace, else the application namespace). A strategy that
// cannot expose it (a fake) exposes nothing.
func (a NativeInstallAdapter) PublishOperability(ctx context.Context, namespace string, report operability.Report) error {
	strategy, ok := a.Strategy.(operabilityStrategy)
	if !ok {
		return nil
	}
	control := a.Config.ControlNamespace
	if control == "" {
		control = namespace
	}
	if a.Now == nil {
		return errors.New("exposing the operability summary requires a clock (NativeInstallAdapter.Now)")
	}
	return strategy.PublishOperability(ctx, control, report, a.Now())
}

// InstallDeployment installs the controller for the accepted commit and the
// DeploymentSource the receipt deployed: onboarding stages that source's
// objects (the controller reads it from the accepted tree's plan.yaml).
func (a NativeInstallAdapter) InstallDeployment(ctx context.Context, acceptedCommit, namespace string, receipt DeploymentReceipt) error {
	if receipt.Source == nil {
		return errors.New("verified deployment names no deployment source")
	}
	if strategy, ok := a.Strategy.(interface {
		WithVerifiedDeployment(string, string, string, *contracts.DeploymentSource, *contracts.RenderEnvironment, rendered.ReceiptPins) InstallStrategy
	}); ok {
		a.Strategy = strategy.WithVerifiedDeployment(a.AcceptedRepository, acceptedCommit, namespace, receipt.Source, receipt.Environment, rendered.ReceiptPins{EffectiveTree: receipt.EffectiveTree, BaseTree: receipt.BaseTree, Images: receipt.ContainerImages})
	}
	environment, err := renderenv.Format(receipt.Environment)
	if err != nil {
		return err
	}
	if environment != "" {
		a.Config.ExtraControllerArgs = append(slices.Clone(a.Config.ExtraControllerArgs), "--render-env="+environment)
	}
	return a.Install(ctx, acceptedCommit, namespace)
}

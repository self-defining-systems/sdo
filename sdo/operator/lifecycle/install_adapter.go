package lifecycle

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/rendered"
	"time"

	"sdo.dev/operator/install"
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
}

func (s KubernetesInstallStrategy) Install(ctx context.Context, config install.Config, seeder install.RepositorySeeder) (install.Result, error) {
	installer := s.Installer
	installer.SeedRepository = seeder
	return installer.Install(ctx, config)
}

func (s KubernetesInstallStrategy) InstallPrepared(ctx context.Context, config install.Config, acceptedRoot string) (install.Result, error) {
	return s.Installer.InstallPrepared(ctx, config, acceptedRoot)
}

// WithVerifiedDeployment binds acknowledgement to this independently healthy
// native deployment; verification is completed by the in-cluster controller.
func (s KubernetesInstallStrategy) WithVerifiedDeployment(root, commit, namespace string, paths []string) InstallStrategy {
	s.Installer.BeforeControllerStart = func(ctx context.Context, config install.Config) error {
		control := config.ControlNamespace
		if control == "" {
			control = namespace
		}
		source := rendered.Source{Repository: root, ManifestPaths: paths}
		objects, err := source.RawManifests(ctx, commit, namespace)
		if err != nil {
			return fmt.Errorf("onboarding accepted source: %w", err)
		}
		if err := install.ReconcileAcceptedSourceGrants(ctx, s.Installer.Client, namespace, control, objects); err != nil {
			return err
		}
		existing, err := onboarding.Load(ctx, s.Installer.Client, control, namespace)
		if err != nil || existing != nil {
			return err
		}
		return onboarding.Stage(ctx, s.Installer.Client, control, namespace, commit, objects, time.Now())
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

func (a NativeInstallAdapter) InstallDeployment(ctx context.Context, acceptedCommit, namespace string, receipt DeploymentReceipt) error {
	if strategy, ok := a.Strategy.(interface {
		WithVerifiedDeployment(string, string, string, []string) InstallStrategy
	}); ok {
		a.Strategy = strategy.WithVerifiedDeployment(a.AcceptedRepository, acceptedCommit, namespace, receipt.ManifestPaths)
	}
	for _, path := range receipt.ManifestPaths {
		a.Config.ExtraControllerArgs = append(a.Config.ExtraControllerArgs, "--deployed-manifest-path="+path)
	}
	return a.Install(ctx, acceptedCommit, namespace)
}

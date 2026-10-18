package lifecycle

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

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

package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"sdo.dev/operator/install"
)

type SyncExistingConfig struct {
	Repository, ControlNamespace, RepositoryPVC, ControllerImage, Kubeconfig string
}

func ParseSyncExistingLaunch(args []string) (SyncExistingConfig, error) {
	if len(args) == 0 || args[0] != "sync-existing" {
		return SyncExistingConfig{}, errors.New("expected sync-existing subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator sync-existing", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repository := flags.String("repository", "", "local accepted repository")
	control := flags.String("control-namespace", "", "operator namespace")
	pvc := flags.String("repository-pvc", "", "shared accepted repository PVC")
	image := flags.String("controller-image", "", "expected controller image")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes configuration")
	if err := flags.Parse(args[1:]); err != nil {
		return SyncExistingConfig{}, err
	}
	if flags.NArg() != 0 {
		return SyncExistingConfig{}, fmt.Errorf("unexpected sync-existing arguments: %v", flags.Args())
	}
	for label, value := range map[string]string{"repository": *repository, "control namespace": *control, "repository PVC": *pvc, "controller image": *image} {
		if strings.TrimSpace(value) == "" {
			return SyncExistingConfig{}, fmt.Errorf("sync-existing requires %s", label)
		}
	}
	if !filepath.IsAbs(*repository) || (*kubeconfig != "" && !filepath.IsAbs(*kubeconfig)) {
		return SyncExistingConfig{}, errors.New("repository and kubeconfig paths must be absolute")
	}
	return SyncExistingConfig{*repository, *control, *pvc, *image, *kubeconfig}, nil
}

func RunSyncExisting(ctx context.Context, cfg SyncExistingConfig) error {
	client, err := hostKubernetesClient(cfg.Kubeconfig)
	if err != nil {
		return err
	}
	installer := install.Installer{Client: client, SyncRepository: install.KubectlTarSynchronizerWithKubeconfig(cfg.Repository, cfg.Kubeconfig)}
	return installer.SyncExistingRepository(ctx, install.SyncConfig{ControlNamespace: cfg.ControlNamespace, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage})
}

type MaintenanceConfig struct {
	ControlNamespace, Generation, Kubeconfig string
	Paused                                   bool
}

func ParseMaintenanceLaunch(args []string) (MaintenanceConfig, error) {
	if len(args) == 0 || args[0] != "maintenance" {
		return MaintenanceConfig{}, errors.New("expected maintenance subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator maintenance", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	control := flags.String("control-namespace", "", "operator namespace")
	paused := flags.Bool("paused", false, "pause controller observation")
	generation := flags.String("generation", "", "unique redeploy generation")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes configuration")
	if err := flags.Parse(args[1:]); err != nil {
		return MaintenanceConfig{}, err
	}
	if flags.NArg() != 0 {
		return MaintenanceConfig{}, fmt.Errorf("unexpected maintenance arguments: %v", flags.Args())
	}
	if strings.TrimSpace(*control) == "" || strings.TrimSpace(*generation) == "" {
		return MaintenanceConfig{}, errors.New("maintenance requires control namespace and generation")
	}
	if *kubeconfig != "" && !filepath.IsAbs(*kubeconfig) {
		return MaintenanceConfig{}, errors.New("kubeconfig path must be absolute")
	}
	return MaintenanceConfig{ControlNamespace: *control, Generation: *generation, Kubeconfig: *kubeconfig, Paused: *paused}, nil
}

func RunMaintenance(ctx context.Context, cfg MaintenanceConfig) error {
	client, err := hostKubernetesClient(cfg.Kubeconfig)
	if err != nil {
		return err
	}
	return install.SetMaintenance(ctx, client, cfg.ControlNamespace, cfg.Paused, cfg.Generation)
}

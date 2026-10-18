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

// InspectExistingConfig identifies an already running native controller. It
// intentionally has no local repository path, since local HEAD can lag the
// accepted PVC checkout after an incident learns new operational memory.
type InspectExistingConfig struct {
	Inspect    install.InspectConfig
	Kubeconfig string
}

func ParseInspectExistingLaunch(args []string) (InspectExistingConfig, error) {
	if len(args) == 0 || args[0] != "inspect-existing" {
		return InspectExistingConfig{}, errors.New("expected inspect-existing subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator inspect-existing", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	control := flags.String("control-namespace", "", "operator namespace")
	namespace := flags.String("namespace", "", "application namespace")
	application := flags.String("application", "", "application identity")
	pvc := flags.String("repository-pvc", "", "shared repository PVC")
	image := flags.String("controller-image", "", "expected controller image")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes configuration")
	if err := flags.Parse(args[1:]); err != nil {
		return InspectExistingConfig{}, err
	}
	if flags.NArg() != 0 {
		return InspectExistingConfig{}, fmt.Errorf("unexpected inspect-existing arguments: %v", flags.Args())
	}
	for label, value := range map[string]string{"control namespace": *control, "namespace": *namespace, "application": *application, "repository PVC": *pvc, "controller image": *image} {
		if strings.TrimSpace(value) == "" {
			return InspectExistingConfig{}, fmt.Errorf("inspect-existing requires %s", label)
		}
	}
	if *kubeconfig != "" && !filepath.IsAbs(*kubeconfig) {
		return InspectExistingConfig{}, errors.New("kubeconfig path must be absolute")
	}
	return InspectExistingConfig{Inspect: install.InspectConfig{ControlNamespace: *control, Namespace: *namespace, Application: *application, RepositoryPVC: *pvc, ControllerImage: *image}, Kubeconfig: *kubeconfig}, nil
}

func RunInspectExisting(ctx context.Context, cfg InspectExistingConfig) (install.ExistingController, error) {
	client, err := hostKubernetesClient(cfg.Kubeconfig)
	if err != nil {
		return install.ExistingController{}, err
	}
	installer := install.Installer{Client: client}
	read := install.KubectlAcceptedHeadWithKubeconfig(cfg.Kubeconfig)
	installer.ReadAcceptedCommit = func(ctx context.Context, syncCfg install.SyncConfig) (string, error) {
		var commit string
		err := installer.WithSyncPod(ctx, syncCfg, func(ctx context.Context, podConfig install.Config) error {
			var readErr error
			commit, readErr = read(ctx, podConfig)
			return readErr
		})
		return commit, err
	}
	return installer.InspectExisting(ctx, cfg.Inspect)
}

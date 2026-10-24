package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	"sdo.dev/operator/memory/observation"
)

type memoryImportConfig struct {
	Action               string
	Root                 string
	Workspaces           string
	ID                   string
	Namespace            string
	ApplicationNamespace string
	Image                string
	PVC                  string
	Mount                string
	SubPath              string
	Kubeconfig           string
	Context              string
	OperatorRoot         string
	HealthyBaseline      bool
}

func parseMemoryImport(args []string, getenv func(string) string) (memoryImportConfig, error) {
	c := memoryImportConfig{}
	if len(args) == 0 {
		return c, errors.New("import requires prepare or publish")
	}
	c.Action = args[0]
	f := flag.NewFlagSet("memory import", flag.ContinueOnError)
	f.StringVar(&c.Root, "root", ".", "accepted application Git root")
	f.StringVar(&c.Workspaces, "worktree-root", "", "broker workspaces, outside accepted repository")
	f.StringVar(&c.ID, "id", "", "unique operator import ID")
	f.StringVar(&c.Namespace, "namespace", "", "isolated validator namespace")
	f.StringVar(&c.ApplicationNamespace, "application-namespace", "", "application observation namespace")
	f.StringVar(&c.Image, "validator-image", "", "isolated validator image")
	f.StringVar(&c.PVC, "repository-pvc", "", "repository PVC visible to validator")
	f.StringVar(&c.Mount, "repository-mount-path", "", "host path corresponding to repository PVC")
	f.StringVar(&c.SubPath, "repository-pvc-subpath", "", "repository PVC subpath")
	f.StringVar(&c.Kubeconfig, "kubeconfig", "", "Kubernetes configuration path")
	f.StringVar(&c.Context, "context", "", "Kubernetes context")
	f.StringVar(&c.OperatorRoot, "operator-root", operatorRootDefault(getenv), "operator sources matching validator image")
	f.BoolVar(&c.HealthyBaseline, "healthy-baseline", false, "validate incident detectors against provisioned healthy baseline")
	positional, err := parseFlags(f, args[1:])
	if err != nil {
		return c, err
	}
	if len(positional) != 0 || (c.Action != "prepare" && c.Action != "publish") || c.ID == "" || c.Workspaces == "" {
		return c, errors.New("import requires prepare|publish, --id, and --worktree-root")
	}
	c.Root, err = absRoot(c.Root)
	if err != nil {
		return c, err
	}
	c.Workspaces, err = filepath.Abs(c.Workspaces)
	if err != nil {
		return c, err
	}
	if c.Action == "publish" && (c.Namespace == "" || c.Image == "" || c.PVC == "" || c.Mount == "") {
		return c, errors.New("import publish requires --namespace, --validator-image, --repository-pvc, and --repository-mount-path")
	}
	return c, nil
}

func memoryImport(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	c, err := parseMemoryImport(args, getenv)
	if err != nil {
		return err
	}
	ceiling, err := observation.CeilingFromEnvironment()
	if err != nil {
		return err
	}
	var runner broker.Validator
	if c.Action == "publish" {
		schema, err := builder.SchemaIdentity(c.OperatorRoot)
		if err != nil {
			return err
		}
		loading := clientcmd.NewDefaultClientConfigLoadingRules()
		loading.ExplicitPath = c.Kubeconfig
		config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, &clientcmd.ConfigOverrides{CurrentContext: c.Context}).ClientConfig()
		if err != nil {
			return err
		}
		client, err := kubernetes.NewForConfig(config)
		if err != nil {
			return err
		}
		runner = broker.JobValidator(broker.KubernetesValidationJobClient{Client: client, Namespace: c.Namespace, Image: c.Image, RepositoryPVC: c.PVC, RepositoryMountPath: c.Mount, RepositoryPVCSubPath: c.SubPath, SchemaIdentity: schema, HealthyBaseline: c.HealthyBaseline})
	}
	repository, err := broker.New(c.Root, c.Workspaces, runner, broker.ObservationPolicy{Ceiling: ceiling, ApplicationNamespace: c.ApplicationNamespace})
	if err != nil {
		return err
	}
	if c.Action == "prepare" {
		proposal, err := repository.CreateProposalForPhase(ctx, c.ID, broker.ImportPhase)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(proposal)
	}
	receipt, err := repository.Publish(ctx, c.ID, broker.Human, broker.ImportPhase)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(receipt)
}

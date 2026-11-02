package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/install"
	"sdo.dev/operator/worker_registry"
)

type IncidentEvidenceConfig struct {
	Sync                               install.SyncConfig
	IncidentID, Kubeconfig, Repository string
}

type IncidentEvidenceResult struct {
	IncidentID       string          `json:"incident_id"`
	ResponderJobName string          `json:"responder_job_name"`
	Ledger           json.RawMessage `json:"ledger"`
}

func ParseIncidentEvidenceLaunch(args []string) (IncidentEvidenceConfig, error) {
	if len(args) == 0 || args[0] != "incident-evidence" {
		return IncidentEvidenceConfig{}, errors.New("expected incident-evidence subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator incident-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	control := flags.String("control-namespace", "", "operator namespace")
	pvc := flags.String("repository-pvc", "", "shared accepted repository PVC")
	image := flags.String("controller-image", "", "controller image")
	id := flags.String("incident-id", "", "logical incident ID")
	repository := flags.String("repository", "", "optional accepted repository destination for a single-pod sync")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes configuration")
	if err := flags.Parse(args[1:]); err != nil {
		return IncidentEvidenceConfig{}, err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*control) == "" || strings.TrimSpace(*pvc) == "" || strings.TrimSpace(*image) == "" || strings.TrimSpace(*id) == "" {
		return IncidentEvidenceConfig{}, errors.New("incident-evidence requires namespace, PVC, controller image, and incident ID")
	}
	if *kubeconfig != "" && !filepath.IsAbs(*kubeconfig) {
		return IncidentEvidenceConfig{}, errors.New("kubeconfig path must be absolute")
	}
	if *repository != "" && !filepath.IsAbs(*repository) {
		return IncidentEvidenceConfig{}, errors.New("repository path must be absolute")
	}
	return IncidentEvidenceConfig{Sync: install.SyncConfig{ControlNamespace: *control, RepositoryPVC: *pvc, ControllerImage: *image}, IncidentID: *id, Kubeconfig: *kubeconfig, Repository: *repository}, nil
}

func RunIncidentEvidence(ctx context.Context, cfg IncidentEvidenceConfig) (IncidentEvidenceResult, error) {
	config, err := hostKubernetesConfig(cfg.Kubeconfig)
	if err != nil {
		return IncidentEvidenceResult{}, err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return IncidentEvidenceResult{}, err
	}
	execute, err := newPodExecutor(config, client)
	if err != nil {
		return IncidentEvidenceResult{}, err
	}
	return runIncidentEvidence(ctx, client, cfg, execute)
}

func runIncidentEvidence(ctx context.Context, client kubernetes.Interface, cfg IncidentEvidenceConfig, execute podExecutor) (IncidentEvidenceResult, error) {
	path, err := broker.NativeLedgerPath(broker.DefaultNativeWorktreeRoot, cfg.IncidentID)
	if err != nil {
		return IncidentEvidenceResult{}, err
	}
	var data []byte
	podName := install.RepositorySyncPodName
	job, err := client.BatchV1().Jobs(cfg.Sync.ControlNamespace).Get(ctx, install.ControllerJobName, metav1.GetOptions{})
	if err != nil {
		return IncidentEvidenceResult{}, fmt.Errorf("find current controller Job: %w", err)
	}
	if job.UID == "" {
		return IncidentEvidenceResult{}, errors.New("current controller Job has no UID")
	}
	pods, err := client.CoreV1().Pods(cfg.Sync.ControlNamespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + install.ControllerJobName})
	if err != nil {
		return IncidentEvidenceResult{}, fmt.Errorf("find running controller Pod: %w", err)
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, owner := range pod.OwnerReferences {
			if owner.Kind == "Job" && owner.Name == install.ControllerJobName && owner.UID == job.UID {
				if podName != install.RepositorySyncPodName {
					return IncidentEvidenceResult{}, errors.New("multiple running controller Pods own repository evidence")
				}
				podName = pod.Name
			}
		}
	}
	installer := install.Installer{Client: client}
	if cfg.Repository != "" {
		installer.SyncRepository = install.KubectlTarSynchronizerFromPodWithKubeconfig(cfg.Repository, cfg.Kubeconfig, podName)
	}
	action := func(ctx context.Context, syncConfig install.Config) error {
		if installer.SyncRepository != nil {
			if err := installer.SyncRepository(ctx, syncConfig); err != nil {
				return fmt.Errorf("synchronize accepted repository: %w", err)
			}
		}
		var output, stderr bytes.Buffer
		if err := execute(ctx, cfg.Sync.ControlNamespace, podName, []string{"cat", path}, &output, &stderr); err != nil {
			return fmt.Errorf("read native incident ledger: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
		data = output.Bytes()
		return nil
	}
	if podName == install.RepositorySyncPodName {
		err = installer.WithSyncPod(ctx, cfg.Sync, action)
	} else {
		err = action(ctx, install.Config{ControlNamespace: cfg.Sync.ControlNamespace})
	}
	if err != nil {
		return IncidentEvidenceResult{}, err
	}
	ledger, err := broker.ValidateNativeLedger(data, cfg.IncidentID)
	if err != nil {
		return IncidentEvidenceResult{}, err
	}
	jobName, err := worker_registry.IncidentResponderJobName(ctx, client, cfg.Sync.ControlNamespace, cfg.IncidentID)
	if err != nil {
		return IncidentEvidenceResult{}, err
	}
	return IncidentEvidenceResult{IncidentID: cfg.IncidentID, ResponderJobName: jobName, Ledger: ledger}, nil
}

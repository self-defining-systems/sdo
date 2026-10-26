package install

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var commitID = regexp.MustCompile(`^[0-9a-f]{40}$`)

// KubectlTarSeeder materializes the accepted commit in a detached checkout,
// then copies that checkout into the controller PVC through its sync Pod.
// Uncommitted source files and later commits never enter the controller.
func KubectlTarSeeder(repositoryPath, acceptedCommit string) RepositorySeeder {
	return KubectlTarSeederWithKubeconfig(repositoryPath, acceptedCommit, "")
}

// KubectlTarSeederWithKubeconfig directs kubectl to the same cluster as the
// host client without changing the process environment or other launchers.
func KubectlTarSeederWithKubeconfig(repositoryPath, acceptedCommit, kubeconfig string) RepositorySeeder {
	return KubectlTarSeederWithBrokerEvidence(repositoryPath, acceptedCommit, kubeconfig, "")
}

// KubectlTarSeederWithBrokerEvidence also restores an explicitly supplied trusted
// deployment ledger. The ledger is operator input, never inferred from Git or Pods.
func KubectlTarSeederWithBrokerEvidence(repositoryPath, acceptedCommit, kubeconfig, brokerRoot string) RepositorySeeder {
	return KubectlTarAdoptionSeeder(repositoryPath, acceptedCommit, kubeconfig, brokerRoot, nil)
}

// KubectlTarAdoptionSeeder prefers an operator-supplied ledger. Without one,
// it captures the adopted application's image identities at this install.
func KubectlTarAdoptionSeeder(repositoryPath, acceptedCommit, kubeconfig, brokerRoot string, capture *AdoptionCapture) RepositorySeeder {
	return func(ctx context.Context, cfg Config) error {
		if cfg.AcceptedCommit != "" && cfg.AcceptedCommit != acceptedCommit {
			return fmt.Errorf("repository seeder commit differs from installer accepted commit")
		}
		evidence, err := seedBrokerArchive(ctx, repositoryPath, acceptedCommit, brokerRoot)
		if err != nil {
			return fmt.Errorf("trusted seed deployment evidence: %w", err)
		}
		if brokerRoot == "" && capture != nil {
			if evidence, err = adoptionArchive(ctx, *capture, repositoryPath, acceptedCommit, cfg.Namespace); err != nil {
				return fmt.Errorf("adopted deployment evidence: %w", err)
			}
		}
		checkout, cleanup, err := prepareAcceptedCheckout(ctx, repositoryPath, acceptedCommit)
		if err != nil {
			return err
		}
		defer cleanup()
		reset := kubectlCommand(ctx, kubeconfig, "--namespace", cfg.controlNamespace(), "exec", RepositorySyncPodName, "--", "sh", "-c", "rm -rf /workspace/application /workspace/worktrees && mkdir -p /workspace/application /workspace/worktrees")
		if output, err := reset.CombinedOutput(); err != nil {
			return fmt.Errorf("reset controller PVC checkout: %w: %s", err, strings.TrimSpace(string(output)))
		}
		producer := exec.CommandContext(ctx, "tar", "-C", checkout, "-cf", "-", ".")
		pipe, err := producer.StdoutPipe()
		if err != nil {
			return err
		}
		consumer := kubectlCommand(ctx, kubeconfig, "--namespace", cfg.controlNamespace(), "exec", "-i", RepositorySyncPodName, "--", "tar", "-C", "/workspace/application", "-xf", "-")
		consumer.Stdin = pipe
		consumer.Stderr = os.Stderr
		if err := producer.Start(); err != nil {
			return fmt.Errorf("start accepted repository archive: %w", err)
		}
		if err := consumer.Start(); err != nil {
			_ = producer.Process.Kill()
			_ = producer.Wait()
			return fmt.Errorf("start controller PVC copy: %w", err)
		}
		consumerErr := consumer.Wait()
		producerErr := producer.Wait()
		if producerErr != nil || consumerErr != nil {
			return fmt.Errorf("copy accepted repository into controller PVC: tar=%v kubectl=%v", producerErr, consumerErr)
		}
		if len(evidence) > 0 {
			copy := kubectlCommand(ctx, kubeconfig, "--namespace", cfg.controlNamespace(), "exec", "-i", RepositorySyncPodName, "--", "tar", "-C", "/workspace/worktrees", "-xf", "-")
			copy.Stdin = bytes.NewReader(evidence)
			if output, err := copy.CombinedOutput(); err != nil {
				return fmt.Errorf("copy trusted seed deployment ledger: %w: %s", err, strings.TrimSpace(string(output)))
			}
		}
		return nil
	}
}

// KubectlTarSynchronizerWithKubeconfig keeps sync-back on the requested cluster.
func KubectlTarSynchronizerWithKubeconfig(repositoryPath, kubeconfig string) RepositorySynchronizer {
	return KubectlTarSynchronizerFromPodWithKubeconfig(repositoryPath, kubeconfig, RepositorySyncPodName)
}

// KubectlTarSynchronizerFromPodWithKubeconfig copies accepted commits from an
// already running Pod with the repository volume mounted.
func KubectlTarSynchronizerFromPodWithKubeconfig(repositoryPath, kubeconfig, podName string) RepositorySynchronizer {
	return func(ctx context.Context, cfg Config) error {
		if repositoryPath == "" {
			return fmt.Errorf("repository path is required")
		}
		producer := kubectlCommand(ctx, kubeconfig, "--namespace", cfg.controlNamespace(), "exec", podName, "--", "tar", "-C", "/workspace/application", "-cf", "-", ".")
		pipe, err := producer.StdoutPipe()
		if err != nil {
			return err
		}
		consumer := exec.CommandContext(ctx, "tar", "--no-same-owner", "-C", repositoryPath, "-xf", "-")
		consumer.Stdin = pipe
		consumer.Stderr = os.Stderr
		if err := producer.Start(); err != nil {
			return fmt.Errorf("start runtime repository archive: %w", err)
		}
		if err := consumer.Start(); err != nil {
			_ = producer.Process.Kill()
			_ = producer.Wait()
			return fmt.Errorf("start local repository synchronization: %w", err)
		}
		consumerErr := consumer.Wait()
		producerErr := producer.Wait()
		if producerErr != nil || consumerErr != nil {
			return fmt.Errorf("synchronize accepted runtime commits: kubectl=%v tar=%v", producerErr, consumerErr)
		}
		return removeStaleUntracked(ctx, repositoryPath)
	}
}

// removeStaleUntracked deletes what the archive overlay cannot: files that an accepted commit removed but the
// host checkout still has on disk, which would read as uncommitted changes.
func removeStaleUntracked(ctx context.Context, repositoryPath string) error {
	if output, err := exec.CommandContext(ctx, "git", "-C", repositoryPath, "clean", "-fdq").CombinedOutput(); err != nil {
		return fmt.Errorf("remove files the accepted tree deleted: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// KubectlAcceptedHeadWithKubeconfig reads HEAD from an already-ready sync Pod.
// The caller owns the sync Pod lifecycle; this reader never mutates the PVC.
func KubectlAcceptedHeadWithKubeconfig(kubeconfig string) func(context.Context, Config) (string, error) {
	return func(ctx context.Context, cfg Config) (string, error) {
		command := kubectlCommand(ctx, kubeconfig, "--namespace", cfg.controlNamespace(), "exec", RepositorySyncPodName, "--", "git", "-C", "/workspace/application", "rev-parse", "HEAD")
		output, err := command.Output()
		if err != nil {
			return "", fmt.Errorf("read accepted PVC HEAD: %w", err)
		}
		commit := strings.TrimSpace(string(output))
		if !commitID.MatchString(commit) {
			return "", errors.New("accepted PVC HEAD is not a full Git commit")
		}
		return commit, nil
	}
}

func kubectlCommand(ctx context.Context, kubeconfig string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	if kubeconfig != "" {
		cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	}
	return cmd
}

func prepareAcceptedCheckout(ctx context.Context, repositoryPath, acceptedCommit string) (string, func(), error) {
	if !commitID.MatchString(acceptedCommit) {
		return "", nil, fmt.Errorf("accepted commit must be a full SHA-1 Git commit ID")
	}
	if repositoryPath == "" {
		return "", nil, fmt.Errorf("repository path is required")
	}
	dir, err := os.MkdirTemp("", "sdo-accepted-checkout-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	clone := exec.CommandContext(ctx, "git", "clone", "--no-local", "--no-checkout", "--", repositoryPath, dir)
	if output, err := clone.CombinedOutput(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("clone accepted repository: %w: %s", err, strings.TrimSpace(string(output)))
	}
	checkout := exec.CommandContext(ctx, "git", "-C", dir, "checkout", "--detach", acceptedCommit)
	if output, err := checkout.CombinedOutput(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("checkout accepted commit: %w: %s", err, strings.TrimSpace(string(output)))
	}
	verified := exec.CommandContext(ctx, "git", "-C", dir, "rev-parse", "HEAD")
	output, err := verified.Output()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("inspect accepted checkout: %w", err)
	}
	if strings.TrimSpace(string(output)) != acceptedCommit {
		cleanup()
		return "", nil, fmt.Errorf("accepted commit verification failed")
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	return root, cleanup, nil
}

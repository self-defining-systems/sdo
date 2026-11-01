package install

import (
	"context"
	"regexp"
	"sdo.dev/operator/install/internal/installio"
)

var commitID = regexp.MustCompile(`^[0-9a-f]{40}$`)

func ioConfig(c Config) installio.Config {
	return installio.Config{Namespace: c.controlNamespace(), AcceptedCommit: c.AcceptedCommit, SyncPod: RepositorySyncPodName}
}

// KubectlTarSeeder copies a detached accepted checkout into the controller PVC.
func KubectlTarSeeder(repositoryPath, acceptedCommit string) RepositorySeeder {
	return KubectlTarSeederWithKubeconfig(repositoryPath, acceptedCommit, "")
}

// KubectlTarSeederWithKubeconfig targets the host client's selected cluster.
func KubectlTarSeederWithKubeconfig(repositoryPath, acceptedCommit, kubeconfig string) RepositorySeeder {
	seed := installio.KubectlTarSeederWithKubeconfig(repositoryPath, acceptedCommit, kubeconfig)
	return func(ctx context.Context, c Config) error { return seed(ctx, ioConfig(c)) }
}

// KubectlTarSynchronizerWithKubeconfig synchronizes accepted state from the sync Pod.
func KubectlTarSynchronizerWithKubeconfig(repositoryPath, kubeconfig string) RepositorySynchronizer {
	return KubectlTarSynchronizerFromPodWithKubeconfig(repositoryPath, kubeconfig, RepositorySyncPodName)
}

// KubectlTarSynchronizerFromPodWithKubeconfig synchronizes from an existing repository Pod.
func KubectlTarSynchronizerFromPodWithKubeconfig(repositoryPath, kubeconfig, podName string) RepositorySynchronizer {
	sync := installio.KubectlTarSynchronizerFromPodWithKubeconfig(repositoryPath, kubeconfig, podName)
	return func(ctx context.Context, c Config) error { return sync(ctx, ioConfig(c)) }
}

// KubectlAcceptedHeadWithKubeconfig reads the accepted commit without changing the PVC.
func KubectlAcceptedHeadWithKubeconfig(kubeconfig string) func(context.Context, Config) (string, error) {
	read := installio.KubectlAcceptedHeadWithKubeconfig(kubeconfig)
	return func(ctx context.Context, c Config) (string, error) { return read(ctx, ioConfig(c)) }
}
func prepareAcceptedCheckout(ctx context.Context, repository, commit string) (string, func(), error) {
	return installio.PrepareAcceptedCheckout(ctx, repository, commit)
}
func removeStaleUntracked(ctx context.Context, repository string) error {
	return installio.RemoveStaleUntracked(ctx, repository)
}

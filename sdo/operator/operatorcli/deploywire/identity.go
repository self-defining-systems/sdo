// Package deploywire builds the trusted in-cluster Deployer and the
// independent verifier around it, from one deploy identity. The operate Job
// (onboarding), the controller parent (upstream advances) and the controller
// child (a responder's source repair) each construct their Deployer here, so
// all three build, apply and verify the same way.
package deploywire

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/imagesrc/prebuilt"
	"sdo.dev/operator/operatorcli/catalog/images"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/rendered"
)

// Identity is the trusted in-cluster build and apply identity: the BuildKit
// Job image, the registry push Secret and the build and rollout bounds.
type Identity struct {
	BuildKitImage, RegistryPushSecret, RegistryHost string
	RegistryInsecure                                bool
	BuildTimeout, BuildBudget, RolloutTimeout       time.Duration
}

// Validate checks the identity's combinations.
func (d Identity) Validate() error {
	if (d.BuildKitImage == "") != (d.RegistryPushSecret == "") {
		return errors.New("--buildkit-image and --registry-push-secret go together")
	}
	if d.RegistryInsecure && d.RegistryHost == "" {
		return errors.New("--registry-insecure requires --registry-host")
	}
	if d.BuildTimeout < 0 || d.BuildBudget < 0 || d.RolloutTimeout < 0 {
		return errors.New("build and rollout timeouts must not be negative")
	}
	return nil
}

// Enabled reports whether the identity can build: without a BuildKit image
// and push Secret nothing can redeploy a source change that moves an image.
func (d Identity) Enabled() bool { return d.BuildKitImage != "" && d.RegistryPushSecret != "" }

// Target is where an Identity deploys.
type Target struct {
	Namespace, ControlNamespace    string
	AcceptedRoot, WorkspaceRoot    string
	RepositoryMount, RepositoryPVC string
}

// TargetOf is the deploy target of a controller launch.
func TargetOf(launch *contracts.ControllerConfig) Target {
	return Target{
		Namespace: launch.GetNamespace(), ControlNamespace: ControlNamespace(launch),
		AcceptedRoot: launch.GetAppRoot(), WorkspaceRoot: launch.GetBrokerWorktreeRoot(),
		RepositoryMount: launch.GetRepositoryMountPath(), RepositoryPVC: launch.GetRepositoryPvc(),
	}
}

// ControlNamespace is the namespace holding the controller's own resources.
func ControlNamespace(launch *contracts.ControllerConfig) string {
	if launch.GetControlNamespace() != "" {
		return launch.GetControlNamespace()
	}
	return launch.GetNamespace()
}

// Deployer is the trusted Deployer with every built-in renderer and the
// deployer's image catalog. environment is the install-time render
// environment: the operate Job's discovery, or the controller's recorded one.
func (d Identity) Deployer(client kubernetes.Interface, target Target, environment *contracts.RenderEnvironment) (deployment.Deployer, error) {
	subPath, err := filepath.Rel(target.RepositoryMount, target.WorkspaceRoot)
	if err != nil || subPath == "." || subPath == ".." || filepath.IsAbs(subPath) || strings.HasPrefix(subPath, "../") {
		return deployment.Deployer{}, errors.New("broker proposals must be inside the shared repository mount")
	}
	insecure := ""
	if d.RegistryInsecure {
		insecure = d.RegistryHost
	}
	pre := &prebuilt.Source{InsecureHost: insecure, Cluster: NodeImages{Client: client}}
	var sources *imagesrc.Catalog
	var materializer *rendered.Materializer
	if d.RegistryHost == "" { // nowhere to push a build
		sources, err = images.PrebuiltOnly(pre)
	} else {
		sources, err = images.Catalog(images.Config{Prebuilt: pre, BuildBudget: d.BuildBudget, Builder: buildkit.Builder{
			Client: client, Namespace: target.ControlNamespace, RepositoryPVC: target.RepositoryPVC,
			CheckoutRoot: target.WorkspaceRoot, RepositoryPVCSubPath: subPath,
			BuildKitImage: d.BuildKitImage, RegistrySecret: d.RegistryPushSecret,
			AllowedRegistryHost: d.RegistryHost, RegistryInsecure: d.RegistryInsecure,
			ServiceAccount: "sdo-image-builder", Timeout: d.BuildTimeout,
		}})
		// Builds read the effective tree, materialized beside the broker's
		// proposals on the shared repository volume (the Job mounts it).
		materializer = &rendered.Materializer{Root: filepath.Join(target.WorkspaceRoot, "effective")}
	}
	if err != nil {
		return deployment.Deployer{}, err
	}
	return deployment.Deployer{
		Namespace: target.Namespace, Renderers: renderers.Catalog(), Images: sources,
		Applier:    deployment.CommandBackend{ObserveLimit: d.RolloutTimeout},
		TreeDigest: broker.ProposalTreeDigest, Environment: environment, AcceptedRoot: target.AcceptedRoot, Materializer: materializer,
	}, nil
}

// NodeImages reports the image names the cluster's nodes hold, so the image
// resolver pins a tag the cluster already has to the digest the cluster has.
type NodeImages struct{ Client kubernetes.Interface }

// NodeImageNames implements prebuilt.ClusterImages.
func (n NodeImages) NodeImageNames(ctx context.Context) ([][]string, error) {
	nodes, err := n.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var images [][]string
	for _, node := range nodes.Items {
		for _, image := range node.Status.Images {
			images = append(images, image.Names)
		}
	}
	return images, nil
}

// Proto is the identity as the typed launch config carries it: the parent
// fills it from its flags, the child builds its Deployer from it. The zero
// identity is nil.
func (d Identity) Proto() *contracts.DeployIdentity {
	if d == (Identity{}) {
		return nil
	}
	return &contracts.DeployIdentity{
		BuildkitImage: d.BuildKitImage, RegistryPushSecret: d.RegistryPushSecret, RegistryHost: d.RegistryHost, RegistryInsecure: d.RegistryInsecure,
		BuildTimeout: durationpb.New(d.BuildTimeout), BuildBudget: durationpb.New(d.BuildBudget), RolloutTimeout: durationpb.New(d.RolloutTimeout),
	}
}

// IdentityOf reads the identity of a launch config; an absent one is the zero
// identity (nothing can build, so only a change that moves no image deploys).
func IdentityOf(launch *contracts.ControllerConfig) (Identity, error) {
	message := launch.GetDeployIdentity()
	identity := Identity{
		BuildKitImage: message.GetBuildkitImage(), RegistryPushSecret: message.GetRegistryPushSecret(), RegistryHost: message.GetRegistryHost(), RegistryInsecure: message.GetRegistryInsecure(),
		BuildTimeout: message.GetBuildTimeout().AsDuration(), BuildBudget: message.GetBuildBudget().AsDuration(), RolloutTimeout: message.GetRolloutTimeout().AsDuration(),
	}
	if err := identity.Validate(); err != nil {
		return Identity{}, fmt.Errorf("deploy identity: %w", err)
	}
	return identity, nil
}

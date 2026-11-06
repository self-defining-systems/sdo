package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/imagesrc/prebuilt"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/catalog/images"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/upstream"
)

// deployIdentity is the trusted in-cluster build and apply identity: the
// BuildKit Job image, the registry push Secret and the build and rollout
// bounds. The operate Job deploys onboarding with it; the controller parent
// deploys upstream advances with the same one.
type deployIdentity struct {
	BuildKitImage, RegistryPushSecret, RegistryHost string
	RegistryInsecure                                bool
	BuildTimeout, BuildBudget, RolloutTimeout       time.Duration
}

func (d deployIdentity) validate() error {
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
// and push Secret nothing can redeploy a source change.
func (d deployIdentity) Enabled() bool { return d.BuildKitImage != "" && d.RegistryPushSecret != "" }

// deployTarget is where a deployIdentity deploys.
type deployTarget struct {
	Namespace, ControlNamespace    string
	AcceptedRoot, WorkspaceRoot    string
	RepositoryMount, RepositoryPVC string
}

// deployer is the trusted Deployer with every built-in renderer and the
// deployer's image catalog. Its render environment carries only the
// environment is the install-time one (renderenv): the operate Job's
// discovery, or the controller's --render-env, which the operate Job recorded
// with the deployment evidence.
func (d deployIdentity) deployer(client kubernetes.Interface, target deployTarget, environment *contracts.RenderEnvironment) (deployment.Deployer, error) {
	subPath, err := filepath.Rel(target.RepositoryMount, target.WorkspaceRoot)
	if err != nil || subPath == "." || subPath == ".." || filepath.IsAbs(subPath) || strings.HasPrefix(subPath, "../") {
		return deployment.Deployer{}, errors.New("broker proposals must be inside the shared repository mount")
	}
	insecure := ""
	if d.RegistryInsecure {
		insecure = d.RegistryHost
	}
	pre := &prebuilt.Source{InsecureHost: insecure, Cluster: nodeImages{client: client}}
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

// followUpstream runs the controller parent's upstream follower beside the
// controller child until ctx ends. It never fails the controller: problems
// are logged and following stops or retries.
func followUpstream(ctx context.Context, launch *contracts.ControllerConfig, operatorRoot string, identity deployIdentity) {
	log := slog.Default().With("component", "upstream_follower")
	catalog := upstreams.Catalog()
	sources := func(context.Context) ([]upstream.Source, error) {
		return memory.ReadSources(launch.GetAppRoot(), catalog)
	}
	// Following is off without a sources file: wait for one without
	// touching the broker or the cluster.
	for {
		found, err := hasSources(launch.GetAppRoot())
		if err != nil {
			log.Warn("upstream_sources_unreadable", "error", err.Error())
		}
		if found {
			break
		}
		if processio.SleepUntil(ctx, processio.Now().Add(upstream.DefaultPollInterval)) != nil {
			return
		}
	}
	following, err := newUpstreamFollowing(ctx, launch, operatorRoot, identity, catalog, sources, log)
	if err != nil {
		log.Warn("upstream_following_disabled", "reason", err.Error())
		return
	}
	if err := following.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn("upstream_following_stopped", "error", err.Error())
	}
}

func hasSources(appRoot string) (bool, error) {
	_, err := os.Stat(filepath.Join(appRoot, filepath.FromSlash(layout.SourcesFile)))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func newUpstreamFollowing(ctx context.Context, launch *contracts.ControllerConfig, operatorRoot string, identity deployIdentity, catalog *upstream.Catalog, sources func(context.Context) ([]upstream.Source, error), log *slog.Logger) (upstreamFollowing, error) {
	if !identity.Enabled() {
		return upstreamFollowing{}, errors.New("the controller was installed without a BuildKit image and registry push Secret, so it cannot deploy an upstream revision; reinstall to follow upstream sources")
	}
	if launch.GetFrozenMemory() {
		return upstreamFollowing{}, errors.New("frozen memory accepts no source changes")
	}
	if source := launch.GetCodePath().GetOverrides().GetDetectorSource(); source == "shell" || source == "" && launch.GetCodePath().GetName() == "shell" {
		return upstreamFollowing{}, errors.New("candidate health verification needs Go detectors; the shell detector runner evaluates only the accepted checkout")
	}
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return upstreamFollowing{}, fmt.Errorf("in-cluster Kubernetes credentials: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return upstreamFollowing{}, err
	}
	repository, err := newControllerBroker(launch)
	if err != nil {
		return upstreamFollowing{}, fmt.Errorf("open accepted repository broker: %w", err)
	}
	advancer, cfg, err := controllerAdvancer(launch, operatorRoot, identity, client, repository, catalog)
	if err != nil {
		return upstreamFollowing{}, err
	}
	maintenance := advancer.Maintenance.(advanceMaintenance)
	if recovered, err := maintenance.RecoverStalePause(ctx); err != nil {
		return upstreamFollowing{}, fmt.Errorf("recover a stale advance pause: %w", err)
	} else if recovered {
		log.Warn("upstream_advance_pause_recovered")
	}
	follower, err := upstream.NewFollower(upstream.FollowerConfig{Jitter: 0.1, Rand: processio.Jitter})
	if err != nil {
		return upstreamFollowing{}, err
	}
	return upstreamFollowing{
		Follower: follower,
		Sources:  sources,
		Accepted: repository.LastUpstream,
		Latest: func(ctx context.Context, source upstream.Source) (upstream.Revision, error) {
			return catalog.Latest(ctx, source.Kind, source.Config)
		},
		Advance: func(ctx context.Context, request upstream.Request) (lifecycle.AdvanceResult, error) {
			objective, err := acceptedHealthObjective(launch.GetAppRoot())
			if err != nil {
				return lifecycle.AdvanceResult{}, err
			}
			run := cfg
			run.HealthObjective = objective
			return advancer.Advance(ctx, run, request)
		},
		Holds:      configMapHolds{Client: client, Namespace: controlNamespace(launch)},
		Now:        processio.Now,
		SleepUntil: processio.SleepUntil,
		Log:        log,
	}, nil
}

// controllerAdvancer wires lifecycle.Advancer in the controller parent: the
// upstream catalog, the controller's broker (with its validator), the
// trusted Deployer, the independent verifier at the candidate checkout, the
// shape of the accepted deployment source, the maintenance ConfigMap and the
// child's persisted state.
func controllerAdvancer(launch *contracts.ControllerConfig, operatorRoot string, identity deployIdentity, client kubernetes.Interface, repository *broker.Broker, catalog *upstream.Catalog) (lifecycle.Advancer, lifecycle.AdvanceConfig, error) {
	deployer, err := identity.deployer(client, deployTarget{
		Namespace: launch.GetNamespace(), ControlNamespace: controlNamespace(launch),
		AcceptedRoot: launch.GetAppRoot(), WorkspaceRoot: launch.GetBrokerWorktreeRoot(),
		RepositoryMount: launch.GetRepositoryMountPath(), RepositoryPVC: launch.GetRepositoryPvc(),
	}, launch.GetRenderEnvironment())
	if err != nil {
		return lifecycle.Advancer{}, lifecycle.AdvanceConfig{}, err
	}
	namespace := launch.GetNamespace()
	shapes := renderers.Catalog()
	advancer := lifecycle.Advancer{
		Upstreams: catalog,
		Broker:    repository,
		Readiness: deployer,
		Verifier: deployment.IndependentHealthVerifier{
			AcceptedRoot: launch.GetAppRoot(), OperatorRoot: operatorRoot, Readiness: &deployer,
			Probe: deployment.AcceptedDetectorProbe{Replay: launch.GetReplay(), MetricsURL: launch.GetMetricsUrl()},
		},
		Shape: func(ctx context.Context, root, revision string, source *contracts.DeploymentSource) (string, error) {
			return rendered.Source{Deployment: source, Catalog: shapes, Repository: root, Environment: launch.GetRenderEnvironment()}.ShapeDigest(ctx, revision, namespace)
		},
		Maintenance: advanceMaintenance{Client: client, Namespace: controlNamespace(launch), Now: processio.Now},
		Activity:    controllerActivity{Client: client, Namespace: controlNamespace(launch), Application: namespace},
	}
	return advancer, lifecycle.AdvanceConfig{Namespace: namespace, GitDir: filepath.Join(launch.GetAppRoot(), ".git")}, nil
}

func controlNamespace(launch *contracts.ControllerConfig) string {
	if launch.GetControlNamespace() != "" {
		return launch.GetControlNamespace()
	}
	return launch.GetNamespace()
}

// acceptedHealthObjective reads the human health objective from the accepted
// goal (the body under its "# Health objective" heading).
func acceptedHealthObjective(appRoot string) (string, error) {
	data, err := os.ReadFile(filepath.Join(appRoot, filepath.FromSlash(layout.GoalFile)))
	if err != nil {
		return "", fmt.Errorf("read accepted goal: %w", err)
	}
	text := string(data)
	if _, body, found := strings.Cut(text, "# Health objective\n"); found {
		text = body
	}
	if strings.TrimSpace(text) == "" {
		return "", errors.New("accepted goal has no health objective")
	}
	return strings.TrimSpace(text), nil
}

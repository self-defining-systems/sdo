package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
	"sdo.dev/operator/operatorcli/deploywire"
	"sdo.dev/operator/operatorcli/internal/processio"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/upstream"
)

// deployIdentity is the trusted in-cluster build and apply identity (see
// deploywire.Identity). The operate Job deploys onboarding with it, the
// controller parent deploys upstream advances with it and the controller
// child deploys a responder's source repair with it.
type deployIdentity deploywire.Identity

func (d deployIdentity) validate() error { return deploywire.Identity(d).Validate() }

// Enabled reports whether the identity can build.
func (d deployIdentity) Enabled() bool { return deploywire.Identity(d).Enabled() }

// deployTarget is where a deployIdentity deploys.
type deployTarget deploywire.Target

// deployer is the trusted Deployer of the identity (deploywire.Identity.Deployer).
func (d deployIdentity) deployer(client kubernetes.Interface, target deployTarget, environment *contracts.RenderEnvironment, policy *contracts.DeploymentPolicy) (deployment.Deployer, error) {
	return deploywire.Identity(d).Deployer(client, deploywire.Target(target), environment, policy)
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
	policy, err := deploywire.Settings(launch)
	if err != nil {
		return upstreamFollowing{}, err
	}
	if policy.Mode != settings.ModeOperate {
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
	return assembleUpstreamFollowing(ctx, launch, operatorRoot, identity, catalog, sources, log, client, repository)
}

// assembleUpstreamFollowing wires the production follower from its cluster
// client and accepted-repository broker, and refuses one with a missing
// collaborator (upstreamFollowing.validate).
func assembleUpstreamFollowing(ctx context.Context, launch *contracts.ControllerConfig, operatorRoot string, identity deployIdentity, catalog *upstream.Catalog, sources func(context.Context) ([]upstream.Source, error), log *slog.Logger, client kubernetes.Interface, repository *broker.Broker) (upstreamFollowing, error) {
	advancer, cfg, maintenance, err := controllerAdvancer(launch, operatorRoot, identity, client, repository, catalog)
	if err != nil {
		return upstreamFollowing{}, err
	}
	if recovered, err := maintenance.RecoverStalePause(ctx); err != nil {
		return upstreamFollowing{}, fmt.Errorf("recover a stale advance pause: %w", err)
	} else if recovered {
		log.Warn("upstream_advance_pause_recovered")
	}
	follower, err := upstream.NewFollower(upstream.FollowerConfig{Jitter: 0.1, Rand: processio.Jitter})
	if err != nil {
		return upstreamFollowing{}, err
	}
	following := upstreamFollowing{
		Follower: follower,
		Sources:  sources,
		Accepted: repository.LastUpstream,
		Latest: func(ctx context.Context, source upstream.Source) (upstream.Revision, error) {
			return catalog.Latest(ctx, source.Kind, source.Config)
		},
		Advance: func(ctx context.Context, request upstream.Request) (lifecycle.AdvanceResult, error) {
			objective, err := memory.AcceptedHealthObjective(launch.GetAppRoot())
			if err != nil {
				return lifecycle.AdvanceResult{}, err
			}
			run := cfg
			run.HealthObjective = objective
			return advancer.Advance(ctx, run, request)
		},
		Holds:       configMapHolds{Client: client, Namespace: controlNamespace(launch)},
		Obligations: configMapObligations{Client: client, Namespace: controlNamespace(launch)},
		// The retry is the advance's own refresh: same scoped publication ID
		// (re-attaches after a restart) and a no-op once detection is current.
		Refresh: func(ctx context.Context, o upstream.Obligation) error {
			return advancer.Designer.RefreshDetection(ctx, lifecycle.ShapeChange{Upstream: o.Upstream, Accepted: o.Accepted})
		},
		Now:        processio.Now,
		SleepUntil: processio.SleepUntil,
		Log:        log,
	}
	return following, following.validate()
}

// controllerAdvancer wires lifecycle.Advancer in the controller parent: the
// upstream catalog, the controller's broker (with its validator), the
// trusted Deployer, the independent verifier at the candidate checkout, the
// shape of the accepted deployment source, the maintenance ConfigMap and the
// child's persisted state.
func controllerAdvancer(launch *contracts.ControllerConfig, operatorRoot string, identity deployIdentity, client kubernetes.Interface, repository *broker.Broker, catalog *upstream.Catalog) (lifecycle.Advancer, lifecycle.AdvanceConfig, advanceMaintenance, error) {
	deployer, err := identity.deployer(client, deployTarget(deploywire.TargetOf(launch)), launch.GetRenderEnvironment(), launch.GetDeploymentPolicy())
	if err != nil {
		return lifecycle.Advancer{}, lifecycle.AdvanceConfig{}, advanceMaintenance{}, err
	}
	verifier, err := deploywire.AppliedVerifier(launch, operatorRoot, &deployer, client)
	if err != nil {
		return lifecycle.Advancer{}, lifecycle.AdvanceConfig{}, advanceMaintenance{}, err
	}
	namespace := launch.GetNamespace()
	shapes := renderers.Catalog()
	maintenance := advanceMaintenance{Client: client, Namespace: controlNamespace(launch), Now: processio.Now}
	if err := maintenance.validate(); err != nil {
		return lifecycle.Advancer{}, lifecycle.AdvanceConfig{}, advanceMaintenance{}, err
	}
	advancer := lifecycle.Advancer{
		Upstreams: catalog,
		Broker:    repository,
		Readiness: deployer,
		Verifier:  verifier,
		Shape: func(ctx context.Context, root, revision string, source *contracts.DeploymentSource) (string, error) {
			return rendered.Source{Deployment: source, Catalog: shapes, Repository: root, Environment: launch.GetRenderEnvironment()}.ShapeDigest(ctx, revision, namespace)
		},
		Designer:    designerRefresh{launch: launch, operatorRoot: operatorRoot, repository: repository},
		Deployer:    deployerCorrection{launch: launch, operatorRoot: operatorRoot, repository: repository},
		Maintenance: maintenance,
		Exposer:     operabilityExposer{Client: client, Namespace: controlNamespace(launch)},
		Now:         processio.Now,
		Activity:    controllerActivity{Client: client, Namespace: controlNamespace(launch), Application: namespace},
	}
	policy, err := deploywire.Settings(launch)
	if err != nil {
		return lifecycle.Advancer{}, lifecycle.AdvanceConfig{}, advanceMaintenance{}, err
	}
	return advancer, lifecycle.AdvanceConfig{SettingsDigest: policy.DetectionDigest(), Namespace: namespace, GitDir: filepath.Join(launch.GetAppRoot(), ".git")}, maintenance, nil
}

// operabilityExposer publishes an accepted advance's report to the
// sdo-operability ConfigMap (lifecycle.OperabilityExposer).
type operabilityExposer struct {
	Client    kubernetes.Interface
	Namespace string
}

func (e operabilityExposer) PublishOperability(ctx context.Context, report operability.Report) error {
	return install.PublishOperability(ctx, e.Client, e.Namespace, report, processio.Now())
}

func controlNamespace(launch *contracts.ControllerConfig) string {
	return deploywire.ControlNamespace(launch)
}

// designerRefresh is the detection designer route of an advance: the
// controller's own health refresh. It computes whether the accepted health
// requirements still match the application's desired-state shape and, when they
// do not, dispatches the health judge, whose candidate is staged and accepted
// through the operability gate (lifecycle.Coordinator.RefreshHealth). It does
// nothing when the accepted detection is current.
type designerRefresh struct {
	launch       *contracts.ControllerConfig
	operatorRoot string
	repository   lifecycle.ProposalBroker
}

func (d designerRefresh) RefreshDetection(ctx context.Context, _ lifecycle.ShapeChange) error {
	return refreshControllerHealth(ctx, lifecycle.RefreshScopeAdvance, d.launch, d.operatorRoot, d.repository)
}

// deployerCorrection is the deployment agent's route of an advance: a hold the
// deployer owns (render, patch, build) starts a deployer task over the advance
// checkout with the hold as its request (lifecycle.Advancer.Deployer). It uses
// the controller's agent transport and the trusted deployer's catalogs; the
// agent only proposes, the broker validates the correction and the advance
// deploys and verifies it like any other.
type deployerCorrection struct {
	launch       *contracts.ControllerConfig
	operatorRoot string
	repository   lifecycle.ProposalBroker
}

func (d deployerCorrection) CorrectDeployment(ctx context.Context, correction lifecycle.DeploymentCorrection) (*contracts.DeploymentPlan, error) {
	registry, client, workers, err := controllerAgentWorkers(d.launch)
	if err != nil {
		return nil, err
	}
	coordinator, err := healthRefreshCoordinator(d.launch, d.operatorRoot, client, d.repository, registry, workers)
	if err != nil {
		return nil, err
	}
	// The agent is offered what the trusted deployer can deploy, and nothing else.
	if deployer, ok := coordinator.Readiness.(deployment.Deployer); ok {
		coordinator.ImageSources = deployer.Images
	}
	identity, err := deploywire.IdentityOf(d.launch)
	if err != nil {
		return nil, err
	}
	objective, err := memory.AcceptedHealthObjective(d.launch.GetAppRoot())
	if err != nil {
		return nil, err
	}
	policy, err := deploywire.Settings(d.launch)
	if err != nil {
		return nil, err
	}
	return coordinator.CorrectDeployment(ctx, lifecycle.Config{
		SettingsDigest: policy.DetectionDigest(),
		Application:    d.launch.GetApplication(), Namespace: d.launch.GetNamespace(), HealthObjective: objective, RegistryHost: identity.RegistryHost,
	}, correction)
}

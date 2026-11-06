package operatorcli

import (
	"context"
	"errors"
	"log/slog"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/operatorcli/deploywire"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// controllerPolicyHold is the policy check of every controller start, whether
// or not a health refresh is due: an accepted deployment that violates the
// deployment policy in force is a typed hold (owner operator, cause policy),
// published to the operability ConfigMap. The controller then keeps running
// without deploying instead of exiting into a crash loop. A failure of the
// check itself is logged and does not block the start: the refresh and every
// deploy path verify the accepted deployment again.
func controllerPolicyHold(ctx context.Context, launch *contracts.ControllerConfig, identity deployIdentity, log *slog.Logger) *lifecycle.PolicyRefusedError {
	hold, client, err := checkControllerPolicy(ctx, launch, identity)
	if err != nil {
		log.Warn("controller_policy_check_failed", "error", err.Error())
		return nil
	}
	if hold == nil {
		return nil
	}
	log.Error("controller_policy_hold", "owner", "operator", "cause", "policy", "error", hold.Error())
	if client != nil {
		if err := install.PublishOperability(ctx, client, controlNamespace(launch), hold.Report, processio.Now()); err != nil {
			log.Warn("controller_policy_hold_unpublished", "error", err.Error())
		}
	}
	return hold
}

func checkControllerPolicy(ctx context.Context, launch *contracts.ControllerConfig, identity deployIdentity) (*lifecycle.PolicyRefusedError, kubernetes.Interface, error) {
	repository, err := newControllerBroker(launch)
	if err != nil {
		return nil, nil, err
	}
	revision, err := memory.AcceptedHealthRevision(ctx, launch.GetAppRoot(), nil)
	if err != nil {
		return nil, nil, err
	}
	// The check never touches the cluster; the client only reports the hold.
	var client kubernetes.Interface
	if restConfig, err := rest.InClusterConfig(); err == nil {
		if c, err := kubernetes.NewForConfig(restConfig); err == nil {
			client = c
		}
	}
	deployer, err := identity.deployer(client, deployTarget(deploywire.TargetOf(launch)), launch.GetRenderEnvironment(), launch.GetDeploymentPolicy())
	if err != nil {
		return nil, nil, err
	}
	hold, err := lifecycle.AcceptedPolicyHold(ctx, repository, deployer, revision.Commit, launch.GetNamespace())
	return hold, client, err
}

// holdRefreshOnPolicy turns a refresh refused by the deployment policy into a
// continued start: the controller runs on its accepted detectors and the hold
// is reported, instead of the process exiting for a condition a restart cannot
// change. Every other error is returned.
func holdRefreshOnPolicy(err error, log *slog.Logger) error {
	var refused *lifecycle.PolicyRefusedError
	if errors.As(err, &refused) {
		log.Error("controller_refresh_policy_hold", "owner", "operator", "cause", "policy", "error", refused.Error())
		return nil
	}
	return err
}

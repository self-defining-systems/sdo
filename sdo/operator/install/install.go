package install

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// RepositorySeeder copies only an independently accepted checkout into the
// controller-owned PVC. The installer never accepts a worker proposal path.
type RepositorySeeder func(context.Context, Config) error
type RepositorySynchronizer func(context.Context, Config) error

// Extension installs optional transport resources outside the production
// controller's own RBAC/PVC/Job plan. Implementations belong to adapters.
type Extension interface {
	InstallResources(context.Context, Config, kubernetes.Interface) error
	WaitUntilReady(context.Context, Config) error
	Complete(context.Context, Config, Result) error
	Cleanup(context.Context, Config) error
}

type Installer struct {
	Client             kubernetes.Interface
	SeedRepository     RepositorySeeder
	SyncRepository     RepositorySynchronizer
	ReadAcceptedCommit func(context.Context, SyncConfig) (string, error)
	Extension          Extension
	PollInterval       time.Duration
	SyncTimeout        time.Duration
}

type Result struct {
	Reused         bool
	ControllerLogs string
	Fingerprint    string
}

// SyncConfig names only the resources needed to copy an accepted checkout
// from the PVC. The local repository may lag, so no accepted commit is needed.
type SyncConfig struct {
	ControlNamespace string
	RepositoryPVC    string
	ControllerImage  string
}

const MaintenanceConfigMap = "sdo-controller-maintenance"

// SetMaintenance pauses observation during a planned application redeploy.
// A new generation on resume makes the controller relist application state.
func SetMaintenance(ctx context.Context, client kubernetes.Interface, namespace string, paused bool, generation string) error {
	return SetMaintenanceWithOptions(ctx, client, namespace, paused, generation, MaintenanceOptions{})
}

// MaintenanceOptions distinguishes a redeploy from a temporary observation barrier.
type MaintenanceOptions struct {
	PreserveBaseline bool
}

// SetMaintenanceWithOptions can retain the last healthy baseline across a pause.
func SetMaintenanceWithOptions(ctx context.Context, client kubernetes.Interface, namespace string, paused bool, generation string, options MaintenanceOptions) error {
	if client == nil {
		return errors.New("Kubernetes client is required")
	}
	if !namespaceName.MatchString(namespace) {
		return fmt.Errorf("invalid maintenance namespace %q", namespace)
	}
	if strings.TrimSpace(generation) == "" {
		return errors.New("maintenance generation must be non-empty")
	}
	state := "active"
	if paused {
		state = "paused"
	}
	configMaps := client.CoreV1().ConfigMaps(namespace)
	want := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: MaintenanceConfigMap, Namespace: namespace}, Data: map[string]string{"state": state, "generation": generation}}
	if paused && options.PreserveBaseline {
		want.Data["preserve_baseline"] = "true"
	}
	prior, err := configMaps.Get(ctx, MaintenanceConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, want, metav1.CreateOptions{})
	} else if err == nil {
		want.ResourceVersion = prior.ResourceVersion
		_, err = configMaps.Update(ctx, want, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("set controller maintenance: %w", err)
	}
	return nil
}

func (i Installer) Install(ctx context.Context, cfg Config) (Result, error) {
	return i.install(ctx, cfg, false)
}

// SyncExistingRepository copies the accepted controller checkout back to the
// host through a temporary sync Pod. It never seeds or replaces the controller.
func (i Installer) SyncExistingRepository(ctx context.Context, syncConfig SyncConfig) error {
	if i.SyncRepository == nil {
		return errors.New("repository synchronizer is required")
	}
	return i.WithSyncPod(ctx, syncConfig, func(ctx context.Context, cfg Config) error {
		if err := i.SyncRepository(ctx, cfg); err != nil {
			return fmt.Errorf("synchronize accepted controller repository: %w", err)
		}
		return nil
	})
}

// WithSyncPod gives a read-only transport callback temporary access to the
// repository PVC. The callback owns no Kubernetes resource lifecycle.
func (i Installer) WithSyncPod(ctx context.Context, syncConfig SyncConfig, action func(context.Context, Config) error) error {
	if i.Client == nil || action == nil {
		return errors.New("Kubernetes client and sync Pod action are required")
	}
	if !namespaceName.MatchString(syncConfig.ControlNamespace) || !namespaceName.MatchString(syncConfig.RepositoryPVC) || strings.TrimSpace(syncConfig.ControllerImage) == "" {
		return errors.New("repository synchronization requires control namespace, PVC, and controller image")
	}
	cfg := Config{Namespace: syncConfig.ControlNamespace, ControlNamespace: syncConfig.ControlNamespace, RepositoryPVC: syncConfig.RepositoryPVC, ControllerImage: syncConfig.ControllerImage}
	owner := fmt.Sprintf("sync-%d", time.Now().UnixNano())
	if err := i.acquireOperateLock(ctx, cfg, owner); err != nil {
		return err
	}
	defer func() { _ = i.releaseOperateLock(context.Background(), cfg, owner) }()
	if err := i.ensureSyncPod(ctx, syncPod(cfg)); err != nil {
		return err
	}
	defer func() {
		_ = i.Client.CoreV1().Pods(cfg.controlNamespace()).Delete(context.Background(), RepositorySyncPodName, metav1.DeleteOptions{})
	}()
	if err := i.waitSync(ctx, cfg.controlNamespace()); err != nil {
		return err
	}
	return action(ctx, cfg)
}

// InstallPrepared starts the controller from a clean accepted checkout already
// on the host-prepared shared PVC. It needs only namespaced runtime privileges.
func (i Installer) InstallPrepared(ctx context.Context, cfg Config, acceptedRoot string) (Result, error) {
	if err := verifyPreparedCheckout(ctx, acceptedRoot, cfg.AcceptedCommit); err != nil {
		return Result{}, err
	}
	return i.install(ctx, cfg, true)
}

func verifyPreparedCheckout(ctx context.Context, root, acceptedCommit string) error {
	if root == "" || acceptedCommit == "" {
		return errors.New("prepared install requires accepted checkout and commit")
	}
	absolute, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve prepared checkout: %w", err)
	}
	git := func(args ...string) (string, error) {
		output, err := exec.CommandContext(ctx, "git", append([]string{"-C", absolute}, args...)...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("inspect prepared checkout (%s): %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	gitRoot, err := git("rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	if gitRoot != absolute {
		return errors.New("prepared checkout must be the accepted Git root")
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return err
	}
	if head != acceptedCommit {
		return errors.New("prepared checkout HEAD differs from accepted commit")
	}
	status, err := git("status", "--porcelain")
	if err != nil {
		return err
	}
	if status != "" {
		return errors.New("prepared checkout has uncommitted changes")
	}
	return nil
}

func (i Installer) install(ctx context.Context, cfg Config, prepared bool) (Result, error) {
	if i.Client == nil {
		return Result{}, errors.New("Kubernetes client is required")
	}
	plan, err := i.resources(cfg)
	if err != nil {
		return Result{}, err
	}
	control := cfg.controlNamespace()
	prior, err := i.Client.BatchV1().Jobs(control).Get(ctx, ControllerJobName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return Result{}, fmt.Errorf("inspect controller Job: %w", err)
	}
	reuse := cfg.ReuseExisting && err == nil && prior.Annotations[InstallFingerprintAnnotation] == plan.Job.Annotations[InstallFingerprintAnnotation] && prior.Status.Active > 0 && !jobFailed(prior)
	if !reuse && !prepared && i.SeedRepository == nil {
		return Result{}, errors.New("fresh installation requires an accepted-repository seeder")
	}
	if !reuse && cfg.WaitForCompletion && !prepared && i.SyncRepository == nil {
		return Result{}, errors.New("completed installation requires repository synchronization")
	}
	if prior != nil {
		if err := i.removeStalePermissiveGrants(ctx, cfg, prior); err != nil {
			return Result{}, err
		}
	}
	if !prepared {
		if err := i.ensureBase(ctx, cfg, plan); err != nil {
			return Result{}, err
		}
	}
	if i.Extension != nil {
		if err := i.Extension.InstallResources(ctx, cfg, i.Client); err != nil {
			return Result{}, fmt.Errorf("install transport resources: %w", err)
		}
		if err := i.Extension.WaitUntilReady(ctx, cfg); err != nil {
			return Result{}, fmt.Errorf("wait for transport: %w", err)
		}
	}
	if reuse {
		return Result{Reused: true, Fingerprint: plan.Job.Annotations[InstallFingerprintAnnotation]}, nil
	}
	if i.Extension != nil {
		defer func() { _ = i.Extension.Cleanup(context.Background(), cfg) }()
	}
	if prior != nil {
		foreground := metav1.DeletePropagationForeground
		if err := i.Client.BatchV1().Jobs(control).Delete(ctx, ControllerJobName, metav1.DeleteOptions{PropagationPolicy: &foreground}); err != nil && !apierrors.IsNotFound(err) {
			return Result{}, fmt.Errorf("replace controller Job: %w", err)
		}
		if err := i.waitJobDeleted(ctx, control); err != nil {
			return Result{}, err
		}
	}
	if !prepared {
		if err := i.ensureSyncPod(ctx, plan.SyncPod); err != nil {
			return Result{}, err
		}
		defer func() {
			_ = i.Client.CoreV1().Pods(control).Delete(context.Background(), RepositorySyncPodName, metav1.DeleteOptions{})
		}()
		if err := i.waitSync(ctx, control); err != nil {
			return Result{}, err
		}
		if err := i.SeedRepository(ctx, cfg); err != nil {
			return Result{}, fmt.Errorf("seed accepted repository: %w", err)
		}
		if err := i.markPreparedCommit(ctx, cfg); err != nil {
			return Result{}, err
		}
	}
	if _, err := i.Client.BatchV1().Jobs(control).Create(ctx, plan.Job, metav1.CreateOptions{}); err != nil {
		return Result{}, fmt.Errorf("start controller Job: %w", err)
	}
	result := Result{Fingerprint: plan.Job.Annotations[InstallFingerprintAnnotation]}
	if cfg.WaitForCompletion {
		if err := i.waitJob(ctx, control, time.Duration(cfg.TimeoutSeconds+300)*time.Second); err != nil {
			return Result{}, err
		}
		pods, err := i.Client.CoreV1().Pods(control).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + ControllerJobName})
		if err != nil {
			return Result{}, fmt.Errorf("list controller Job Pods: %w", err)
		}
		foundPod := false
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodSucceeded {
				foundPod = true
				logs, err := i.Client.CoreV1().Pods(control).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
				if err != nil {
					return Result{}, fmt.Errorf("read controller logs: %w", err)
				}
				result.ControllerLogs = string(logs)
				break
			}
		}
		if !foundPod {
			return Result{}, errors.New("completed controller Pod logs are unavailable")
		}
		if !prepared {
			if err := i.SyncRepository(ctx, cfg); err != nil {
				return Result{}, fmt.Errorf("synchronize accepted controller repository: %w", err)
			}
		}
		if i.Extension != nil {
			if err := i.Extension.Complete(ctx, cfg, result); err != nil {
				return Result{}, fmt.Errorf("complete transport: %w", err)
			}
		}
	}
	return result, nil
}

func (i Installer) ensureBase(ctx context.Context, cfg Config, p Plan) error {
	if p.Namespace != nil {
		namespaces := i.Client.CoreV1().Namespaces()
		prior, err := namespaces.Get(ctx, p.Namespace.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = namespaces.Create(ctx, p.Namespace, metav1.CreateOptions{})
		} else if err == nil {
			updated := prior.DeepCopy()
			if updated.Labels == nil {
				updated.Labels = map[string]string{}
			}
			for key, value := range p.Namespace.Labels {
				updated.Labels[key] = value
			}
			_, err = namespaces.Update(ctx, updated, metav1.UpdateOptions{})
		}
		if err != nil {
			return fmt.Errorf("ensure controller namespace: %w", err)
		}
	}
	if _, err := i.Client.CoreV1().PersistentVolumeClaims(p.PVC.Namespace).Create(ctx, p.PVC, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create repository PVC: %w", err)
	}
	for _, sa := range p.ServiceAccounts {
		if _, err := i.Client.CoreV1().ServiceAccounts(sa.Namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create ServiceAccount %s: %w", sa.Name, err)
		}
	}
	if err := i.ensurePermissiveNamespaces(ctx, cfg); err != nil {
		return err
	}
	if err := i.reconcileObservationGrants(ctx, cfg, p); err != nil {
		return err
	}
	for _, role := range p.ObservationClusterRoles {
		prior, err := i.Client.RbacV1().ClusterRoles().Get(ctx, role.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = i.Client.RbacV1().ClusterRoles().Create(ctx, role, metav1.CreateOptions{})
		} else if err == nil {
			role.ResourceVersion = prior.ResourceVersion
			_, err = i.Client.RbacV1().ClusterRoles().Update(ctx, role, metav1.UpdateOptions{})
		}
		if err != nil {
			return err
		}
	}
	for _, binding := range p.ObservationClusterBindings {
		prior, err := i.Client.RbacV1().ClusterRoleBindings().Get(ctx, binding.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = i.Client.RbacV1().ClusterRoleBindings().Create(ctx, binding, metav1.CreateOptions{})
		} else if err == nil {
			binding.ResourceVersion = prior.ResourceVersion
			_, err = i.Client.RbacV1().ClusterRoleBindings().Update(ctx, binding, metav1.UpdateOptions{})
		}
		if err != nil {
			return err
		}
	}
	for _, role := range append(append(p.ControlRoles, p.AppRoles...), p.ObservationRoles...) {
		if err := i.upsertRole(ctx, role); err != nil {
			return err
		}
	}
	for _, binding := range append(append(p.ControlBindings, p.AppBindings...), p.ObservationBindings...) {
		if err := i.upsertBinding(ctx, binding); err != nil {
			return err
		}
	}
	if err := i.ensurePermissiveClusterGrants(ctx, p); err != nil {
		return err
	}
	_, err := i.Client.CoreV1().Secrets(cfg.controlNamespace()).Get(ctx, cfg.CredentialsSecret, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if len(cfg.Credentials) == 0 {
			return fmt.Errorf("credentials Secret %q is missing and no credentials were supplied", cfg.CredentialsSecret)
		}
		_, err = i.Client.CoreV1().Secrets(cfg.controlNamespace()).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.controlNamespace()}, Type: corev1.SecretTypeOpaque, Data: cfg.Credentials}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create credentials Secret: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("inspect credentials Secret: %w", err)
	}
	if cfg.RegistrationConfigMap != "" {
		configMaps := i.Client.CoreV1().ConfigMaps(cfg.controlNamespace())
		_, err := configMaps.Get(ctx, cfg.RegistrationConfigMap, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if len(cfg.RegistrationData) == 0 {
				return fmt.Errorf("agent registration ConfigMap %q is missing and no registration data were supplied", cfg.RegistrationConfigMap)
			}
			_, err = configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cfg.RegistrationConfigMap, Namespace: cfg.controlNamespace()}, Data: cfg.RegistrationData}, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("create agent registration ConfigMap: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("inspect agent registration ConfigMap: %w", err)
		}
	}
	if cfg.RegistryPushSecret != "" {
		secrets := i.Client.CoreV1().Secrets(cfg.controlNamespace())
		prior, err := secrets.Get(ctx, cfg.RegistryPushSecret, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if len(cfg.RegistryPushDockerConfig) == 0 {
				return fmt.Errorf("registry push Secret %q is missing and no registry credentials were supplied", cfg.RegistryPushSecret)
			}
			_, err = secrets.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.RegistryPushSecret, Namespace: cfg.controlNamespace()}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: cfg.RegistryPushDockerConfig}}, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("create registry push Secret: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("inspect registry push Secret: %w", err)
		} else if prior.Type != corev1.SecretTypeDockerConfigJson || len(prior.Data[corev1.DockerConfigJsonKey]) == 0 {
			return errors.New("registry push Secret lacks .dockerconfigjson data")
		}
	}
	return nil
}

func (i Installer) upsertRole(ctx context.Context, want *rbacv1.Role) error {
	roles := i.Client.RbacV1().Roles(want.Namespace)
	prior, err := roles.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = roles.Create(ctx, want, metav1.CreateOptions{})
	} else if err == nil {
		updated := want.DeepCopy()
		updated.ResourceVersion = prior.ResourceVersion
		_, err = roles.Update(ctx, updated, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure Role %s/%s: %w", want.Namespace, want.Name, err)
	}
	return nil
}
func (i Installer) upsertBinding(ctx context.Context, want *rbacv1.RoleBinding) error {
	bindings := i.Client.RbacV1().RoleBindings(want.Namespace)
	prior, err := bindings.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = bindings.Create(ctx, want, metav1.CreateOptions{})
	} else if err == nil {
		updated := want.DeepCopy()
		updated.ResourceVersion = prior.ResourceVersion
		_, err = bindings.Update(ctx, updated, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure RoleBinding %s/%s: %w", want.Namespace, want.Name, err)
	}
	return nil
}
func (i Installer) ensureSyncPod(ctx context.Context, want *corev1.Pod) error {
	pods := i.Client.CoreV1().Pods(want.Namespace)
	if err := pods.Delete(ctx, want.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete stale repository sync Pod: %w", err)
	}
	for {
		_, err := pods.Get(ctx, want.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			break
		}
		if err != nil {
			return err
		}
		if err := i.pause(ctx); err != nil {
			return err
		}
	}
	if _, err := pods.Create(ctx, want, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create repository sync Pod: %w", err)
	}
	return nil
}
func (i Installer) waitSync(ctx context.Context, namespace string) error {
	timeout := i.SyncTimeout
	if timeout <= 0 {
		timeout = 180 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		pod, err := i.Client.CoreV1().Pods(namespace).Get(waitCtx, RepositorySyncPodName, metav1.GetOptions{})
		if err == nil {
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					return nil
				}
			}
			if pod.Status.Phase == corev1.PodFailed {
				return errors.New("repository sync Pod failed")
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if err := i.pause(waitCtx); err != nil {
			return fmt.Errorf("repository sync Pod did not become ready: %w", err)
		}
	}
}
func (i Installer) waitJobDeleted(ctx context.Context, namespace string) error {
	for {
		_, err := i.Client.BatchV1().Jobs(namespace).Get(ctx, ControllerJobName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := i.pause(ctx); err != nil {
			return err
		}
	}
}
func (i Installer) waitJob(ctx context.Context, namespace string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		job, err := i.Client.BatchV1().Jobs(namespace).Get(waitCtx, ControllerJobName, metav1.GetOptions{})
		if err == nil {
			if job.Status.Succeeded > 0 {
				return nil
			}
			if jobFailed(job) {
				return errors.New("controller Job failed")
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		if err := i.pause(waitCtx); err != nil {
			return fmt.Errorf("controller Job did not complete: %w", err)
		}
	}
}
func (i Installer) pause(ctx context.Context) error {
	interval := i.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func jobFailed(job *batchv1.Job) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

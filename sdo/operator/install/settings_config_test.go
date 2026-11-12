package install

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/contracts/settings"
)

func settingsConfig(t *testing.T, mutate func(*settings.Config)) Config {
	t.Helper()
	resolved := settings.Default()
	resolved.Agent.Model = "m"
	if mutate != nil {
		mutate(&resolved)
	}
	cfg := environmentConfig()
	cfg.Settings = &resolved
	cfg.BuildKitImage = "buildkit@sha256:" + strings.Repeat("a", 64)
	cfg.RegistryPushSecret = "push"
	return cfg
}

func writeSettingsFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func mountsSettings(volumes []corev1.Volume, mounts []corev1.VolumeMount, configMap string) bool {
	var fromMap, mounted bool
	for _, volume := range volumes {
		fromMap = fromMap || (volume.ConfigMap != nil && volume.ConfigMap.Name == configMap && volume.Name == "sdo-config")
	}
	for _, mount := range mounts {
		mounted = mounted || (mount.Name == "sdo-config" && mount.MountPath == SettingsDir && mount.ReadOnly)
	}
	return fromMap && mounted
}

// The resolved configuration is a ConfigMap in the control namespace, mounted read-only into every process that
// reads policy, and its hash is part of the controller template so a change replaces the controller.
func TestResolvedConfigIsMountedAndHashedIntoTheInstallFingerprint(t *testing.T) {
	plan, err := Resources(settingsConfig(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if plan.Settings == nil || plan.Settings.Name != SettingsConfigMapName || plan.Settings.Namespace != "app" {
		t.Fatalf("settings ConfigMap = %+v", plan.Settings)
	}
	if plan.Settings.Labels["app.kubernetes.io/managed-by"] != "sdo" {
		t.Fatalf("the ConfigMap is not installer-owned: %v", plan.Settings.Labels)
	}
	if _, err := settings.Read(writeSettingsFile(t, plan.Settings.Data[SettingsFileName])); err != nil {
		t.Fatalf("the ConfigMap document must be a valid config: %v", err)
	}
	pod := plan.Job.Spec.Template.Spec
	if !mountsSettings(pod.Volumes, pod.Containers[0].VolumeMounts, SettingsConfigMapName) {
		t.Fatalf("controller Job does not mount %s read-only at %s", SettingsConfigMapName, SettingsDir)
	}
	job, err := OperateJob(settingsConfig(t, nil), "goal", "op-1")
	if err != nil {
		t.Fatal(err)
	}
	operate := job.Spec.Template.Spec
	if !mountsSettings(operate.Volumes, operate.Containers[0].VolumeMounts, SettingsConfigMapName) {
		t.Fatal("operate Job does not mount the sdo-config ConfigMap")
	}
	changed, err := Resources(settingsConfig(t, func(c *settings.Config) { c.Response.Timeout = settings.Duration(time.Hour) }))
	if err != nil {
		t.Fatal(err)
	}
	const hash = "sdo.dev/config-hash"
	a, b := plan.Job.Spec.Template.Annotations[hash], changed.Job.Spec.Template.Annotations[hash]
	if a == "" || a == b {
		t.Fatalf("a changed config must change the template hash: %q vs %q", a, b)
	}
	if plan.Job.Annotations[InstallFingerprintAnnotation] == changed.Job.Annotations[InstallFingerprintAnnotation] {
		t.Fatal("the install fingerprint ignores the config, so a changed config would not replace the controller")
	}
	same, err := Resources(settingsConfig(t, nil))
	if err != nil || same.Job.Annotations[InstallFingerprintAnnotation] != plan.Job.Annotations[InstallFingerprintAnnotation] {
		t.Fatal("the fingerprint must be stable for an unchanged config")
	}
}

func TestInstallingCreatesThenReplacesTheConfigMap(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	installer := Installer{Client: client}
	for _, timeout := range []time.Duration{time.Hour, 2 * time.Hour} {
		cfg := settingsConfig(t, func(c *settings.Config) { c.Response.Timeout = settings.Duration(timeout) })
		plan, err := Resources(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := installer.ensureSettings(ctx, plan); err != nil {
			t.Fatal(err)
		}
		stored, err := client.CoreV1().ConfigMaps("app").Get(ctx, SettingsConfigMapName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stored.Data[SettingsFileName], "timeout: "+timeout.String()) {
			t.Fatalf("stored config does not carry %s:\n%s", timeout, stored.Data[SettingsFileName])
		}
	}
}

func testSettings() *settings.Config {
	resolved := settings.Default()
	resolved.Agent.Model = "m"
	return &resolved
}

// The configuration is required: an install without one has no values to run with, and none are defaulted here.
func TestAnInstallWithoutResolvedConfigIsRefused(t *testing.T) {
	cfg := environmentConfig()
	cfg.Settings = nil
	if _, err := Resources(cfg); err == nil || !strings.Contains(err.Error(), "operator configuration") {
		t.Fatalf("an install without the resolved config must be refused: %v", err)
	}
	invalid := settingsConfig(t, func(c *settings.Config) { c.Agent.Model = "" })
	if _, err := Resources(invalid); err == nil || !strings.Contains(err.Error(), "agent.model") {
		t.Fatalf("an invalid config must be refused by its one validator: %v", err)
	}
}

// No timing or policy value crosses the Jobs' argv: they read the mounted sdo-config. A distinctive value in every
// knob would otherwise show up in an argument.
func TestOperateAndControllerArgvCarryNoPolicyValues(t *testing.T) {
	cfg := settingsConfig(t, func(c *settings.Config) {
		c.Mode = settings.ModeWatchOnly
		c.Agent.Model, c.Agent.Effort, c.Agent.LifecycleTimeout = "model-x", "xhigh", settings.Duration(97*time.Minute)
		c.Agent.ExtraEnv = []string{"EXTRA_X=1"}
		c.Response.Timeout, c.Response.VerificationTimeout = settings.Duration(91*time.Minute), settings.Duration(93*time.Minute)
		c.Response.MaxLoops, c.Response.RepairPolicy, c.Response.Standby = 7, "recorded-actions", false
		c.Learning.Replay, c.Learning.ReflectionGuidance = "off", "generalize"
		c.Learning.HealthyBaseline = true
		c.Deployment.BuildTimeout, c.Deployment.BuildBudget, c.Deployment.RolloutTimeout = settings.Duration(89*time.Minute), settings.Duration(99*time.Minute), settings.Duration(87*time.Minute)
	})
	plan, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	operate, err := OperateJob(cfg, "goal", "op-1")
	if err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{"controller": plan.Job.Spec.Template.Spec.Containers[0].Args, "operate": operate.Spec.Template.Spec.Containers[0].Args} {
		joined := strings.Join(args, " ")
		for _, policy := range []string{"model-x", "xhigh", "1h37m", "EXTRA_X", "1h31m", "1h33m", "recorded-actions", "generalize", "fresh", "watch-only", "frozen", "healthy-baseline", "1h29m", "1h39m", "1h27m", "--replay", "--max-response-loops", "--responder-standby", "--max-attempts"} {
			if strings.Contains(joined, policy) {
				t.Errorf("%s argv carries policy %q: %v", name, policy, args)
			}
		}
	}
	for _, env := range append(plan.Job.Spec.Template.Spec.Containers[0].Env, operate.Spec.Template.Spec.Containers[0].Env...) {
		if env.Name == "SDO_RESPONDER_MENU" {
			t.Error("the responder menu is response.menu, not an environment variable")
		}
	}
}

// The operate Job's own deadline leaves every deployment attempt room for the configured limits.
func TestOperateJobDeadlineBudgetsTheConfiguredLimits(t *testing.T) {
	cfg := settingsConfig(t, func(c *settings.Config) {
		c.Agent.LifecycleTimeout = settings.Duration(3 * time.Hour)
		c.Deployment.MaxAttempts = 4
	})
	job, err := OperateJob(cfg, "goal", "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if deadline := time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second; deadline < 4*3*time.Hour {
		t.Fatalf("operate Job deadline %s leaves no room for four 3h designer sessions", deadline)
	}
}

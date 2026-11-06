//go:build integration && controller_e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/telemetry"
)

const (
	AppNamespace     = "sdo-toy"
	ControlNamespace = "sdo-toy-sdo"
	ControllerImage  = "sdo-controller:e2e"
	ResponderImage   = "sdo-responder:e2e"
	RepositoryPVC    = "sdo-application-repository"
)

// World is one hermetic run: API server, fake kubelet, toy app and installed controller.
type World struct {
	T         testing.TB
	Root      string // repository root
	Dir       string // run directory (artifacts, PVCs, pods)
	Cluster   *Cluster
	Kubelet   *Kubelet
	Proxy     *Proxy
	Images    *Images
	Workspace string // host copy of the accepted application repository
	Commit    string
	Install   install.Config
	Control   string // directory shared with containers at /e2e (agent mode files)

	quiesced time.Time       // when the run stopped injecting faults (Quiesce)
	killed   map[string]bool // controller Pods the run killed on purpose
}

// RepoRoot finds the repository root from the test's working directory.
func RepoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "pyproject.toml")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repository root not found")
		}
		dir = parent
	}
}

func runIn(t testing.TB, dir string, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, out.String())
	}
	return strings.TrimSpace(out.String())
}

// RunDir returns the run directory: SDO_E2E_DIR when set, else a fresh directory under TMPDIR.
func RunDir(t testing.TB) string {
	t.Helper()
	if dir := os.Getenv("SDO_E2E_DIR"); dir != "" {
		return dir
	}
	dir, err := os.MkdirTemp("", "sdo-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !t.Failed() && os.Getenv("SDO_E2E_KEEP") == "" {
			_ = os.RemoveAll(dir)
		} else {
			t.Logf("run directory kept: %s", dir)
		}
	})
	return dir
}

// NewWorld builds the binaries, starts the cluster and kubelet, deploys the toy
// application, and installs the controller exactly as the toy lane does.
func NewWorld(t testing.TB) *World {
	t.Helper()
	root := RepoRoot(t)
	lockCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	release, err := lockHost(lockCtx, hostLockPort)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	dir := RunDir(t)
	w := &World{T: t, Root: root, Dir: dir}
	binary := filepath.Join(dir, "bin", "sdo-operator")
	if err := os.MkdirAll(filepath.Dir(binary), 0o755); err != nil {
		t.Fatal(err)
	}
	runIn(t, filepath.Join(root, "sdo/operator"), "go", "build", "-trimpath", "-o", binary, "./cmd/sdo-operator")
	cacheDir := os.Getenv("SDO_E2E_CACHE")
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "sdo-e2e-cache")
	}
	w.Images = DefaultImages(t, root, binary, cacheDir)
	w.Control = filepath.Join(dir, "control")
	if err := os.MkdirAll(w.Control, 0o777); err != nil {
		t.Fatal(err)
	}
	w.Images.Binds = append(w.Images.Binds, Bind{Src: w.Control, Dst: "/e2e"})
	w.Cluster = StartCluster(t, filepath.Join(dir, "cluster"))
	w.Kubelet = StartKubelet(t, w.Cluster, w.Images, dir)
	w.Kubelet.JobBackoffBase = 2 * time.Second
	w.Proxy = StartProxy(t, w.Cluster, 1)
	w.Kubelet.Run.APIPort = func(pod *corev1.Pod) int {
		if isControllerPod(pod.Name) {
			return w.Proxy.Port // only the controller sees injected API faults
		}
		return w.Cluster.Port
	}
	return w
}

// Kubectl runs the host kubectl against the API server directly (the way the toy lane injects faults).
func (w *World) Kubectl(namespace string, input string, args ...string) string {
	w.T.Helper()
	full := append([]string{"--kubeconfig", w.Cluster.Kubeconfig}, args...)
	if namespace != "" {
		full = append([]string{"-n", namespace}, full...)
	}
	cmd := exec.Command("kubectl", full...)
	if input != "" {
		cmd.Stdin = strings.NewReader(input)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		w.T.Fatalf("kubectl %v: %v\n%s", args, err, errOut.String())
	}
	return out.String()
}

// DeployToy prepares the toy repository exactly as the toy lane does (source commit, then memory seed commit)
// and applies its manifests to the cluster.
func (w *World) DeployToy() {
	t := w.T
	t.Helper()
	app := filepath.Join(w.Root, "apps/toy")
	workspace := filepath.Join(w.Dir, "application_workspace")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	runIn(t, w.Dir, "cp", "-R", filepath.Join(app, "kubernetes"), filepath.Join(workspace, "kubernetes"))
	for _, name := range []string{"main.go", "Dockerfile"} {
		runIn(t, w.Dir, "cp", filepath.Join(app, name), filepath.Join(workspace, name))
	}
	git := func(args ...string) string {
		return runIn(t, workspace, "git", append([]string{"-c", "user.name=toy", "-c", "user.email=toy@invalid"}, args...)...)
	}
	git("init", "-q")
	git("add", ".")
	git("commit", "-qm", "feat: seed toy application")
	source := git("rev-parse", "HEAD")
	runIn(t, w.Dir, "cp", "-R", filepath.Join(app, "seed/.sdo"), filepath.Join(workspace, ".sdo"))
	manifests, _ := filepath.Glob(filepath.Join(workspace, "kubernetes", "*.yaml"))
	sort.Strings(manifests)
	topology := sha256.New()
	for _, manifest := range manifests {
		data, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatal(err)
		}
		topology.Write(data)
	}
	arch := filepath.Join(workspace, ".sdo/arch.md")
	text, err := os.ReadFile(arch)
	if err != nil {
		t.Fatal(err)
	}
	text = bytes.ReplaceAll(text, []byte("SOURCE_COMMIT"), []byte(source))
	text = bytes.ReplaceAll(text, []byte("TOPOLOGY_FINGERPRINT"), []byte(hex.EncodeToString(topology.Sum(nil))))
	if err := os.WriteFile(arch, text, 0o644); err != nil {
		t.Fatal(err)
	}
	// The deployment source is part of the accepted tree: the controller reads
	// it from .sdo/deployment/plan.yaml.
	planPath := filepath.Join(workspace, ".sdo/deployment/plan.yaml")
	if err := os.MkdirAll(filepath.Dir(planPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(planPath, []byte(`{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["kubernetes"]}}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	git("add", ".sdo")
	git("commit", "-qm", "feat: seed toy operational memory")
	w.Workspace, w.Commit = workspace, git("rev-parse", "HEAD")
	ctx := context.Background()
	for _, ns := range []string{AppNamespace, ControlNamespace} {
		if _, err := w.Cluster.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
	for _, manifest := range manifests {
		w.Kubectl(AppNamespace, "", "apply", "-f", manifest)
	}
	w.waitFor(60*time.Second, "toy deployments ready", func() bool {
		deployments, err := w.Cluster.Client.AppsV1().Deployments(AppNamespace).List(ctx, metav1.ListOptions{})
		if err != nil || len(deployments.Items) != 3 {
			return false
		}
		for _, d := range deployments.Items {
			if d.Status.ReadyReplicas != 1 {
				return false
			}
		}
		return true
	})
}

func (w *World) waitFor(limit time.Duration, what string, check func() bool) {
	w.T.Helper()
	deadline := time.Now().Add(limit)
	for !check() {
		if time.Now().After(deadline) {
			w.logControllerTail(40)
			w.T.Fatalf("timed out after %s waiting for %s", limit, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// logControllerTail prints the end of the controller's output (and the state of its Pods) so a
// timeout on a hosted runner, where the run directory is gone, says what the controller was doing.
func (w *World) logControllerTail(n int) {
	w.T.Helper()
	pods, err := w.Cluster.Client.CoreV1().Pods(ControlNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		w.T.Logf("controller diagnosis: listing pods: %v", err)
		return
	}
	for _, pod := range pods.Items {
		w.T.Logf("controller diagnosis: pod %s phase=%s", pod.Name, pod.Status.Phase)
		for _, status := range append(append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
			w.T.Logf("controller diagnosis:   container %s ready=%v state=%+v", status.Name, status.Ready, status.State)
		}
	}
	for _, name := range w.ControllerPods() {
		lines := w.Kubelet.Run.Lines(ControlNamespace, name, "controller")
		if len(lines) > n {
			lines = lines[len(lines)-n:]
		}
		for _, line := range lines {
			w.T.Logf("controller diagnosis: %s: %s", name, line.Text)
		}
	}
}

// publishRegistrations stores the no-model agent registrations the toy lane uses.
func (w *World) publishRegistrations() {
	t := w.T
	t.Helper()
	files, err := filepath.Glob(filepath.Join(w.Root, "benchmarks/sregym/fastloop/agent_registrations/*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("agent registrations missing: %v", err)
	}
	data := map[string]string{}
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var registration map[string]any
		if err := json.Unmarshal(bytes.ReplaceAll(content, []byte("${RESPONDER_IMAGE}"), []byte(ResponderImage)), &registration); err != nil {
			t.Fatal(err)
		}
		if wrap := w.wrapAgent(registration); wrap != nil {
			registration = wrap
		}
		encoded, _ := json.Marshal(registration)
		data[filepath.Base(file)] = string(encoded)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-agent-registrations", Namespace: ControlNamespace}, Data: data}
	if _, err := w.Cluster.Client.CoreV1().ConfigMaps(ControlNamespace).Create(context.Background(), cm, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// wrapAgent lets the harness change what a launched agent does (crash, hang, idle) through a file the
// containers see at /e2e/agent-mode, without touching the production worker. See agentmode.go.
func (w *World) wrapAgent(registration map[string]any) map[string]any {
	return wrapRegistration(registration)
}

// InstallController installs the controller with the production installer, seeding the accepted repository
// onto the claim directory instead of through kubectl exec.
func (w *World) InstallController(extra ...string) {
	t := w.T
	t.Helper()
	ctx := context.Background()
	client := w.Cluster.Client
	if _, err := client.CoreV1().ConfigMaps(ControlNamespace).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "sdo-controller-maintenance", Namespace: ControlNamespace}, Data: map[string]string{"state": "active", "generation": "e2e"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Secrets(ControlNamespace).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "sdo-agent-credentials", Namespace: ControlNamespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	w.publishRegistrations()
	w.Install = install.Config{
		Namespace: AppNamespace, ControlNamespace: ControlNamespace, Application: "toy",
		ControllerImage: ControllerImage, ResponderImage: ResponderImage,
		RepositoryPVC: RepositoryPVC, CredentialsSecret: "sdo-agent-credentials", Model: "gpt-6-luna",
		RepairPolicy: "recorded-actions", AcceptedCommit: w.Commit, ResponderMenu: true, MaxResponseLoops: 10,
		TimeoutSeconds: 300, VerificationTimeoutSeconds: 120,
		NativeCommand: []string{"/usr/local/bin/sdo-operator"}, ReuseExisting: true,
		RegistrationConfigMap: "sdo-agent-registrations", RegistrationDirectory: "/sdo/registrations",
		TelemetryProfile: telemetry.Profile{Name: "none"}, ExtraControllerArgs: extra,
	}
	seed := func(ctx context.Context, c install.Config) error {
		destination := filepath.Join(w.Kubelet.Run.PVCDir(c.ControlNamespace, c.RepositoryPVC), "application")
		if err := os.MkdirAll(filepath.Dir(destination), 0o777); err != nil {
			return err
		}
		if _, err := os.Stat(destination); err == nil {
			return nil
		}
		out, err := exec.CommandContext(ctx, "cp", "-R", w.Workspace, destination).CombinedOutput()
		if err != nil {
			return fmt.Errorf("seed repository: %v: %s", err, out)
		}
		return nil
	}
	installer := install.Installer{Client: client, SeedRepository: seed, PollInterval: 200 * time.Millisecond}
	if _, err := installer.Install(ctx, w.Install); err != nil {
		t.Fatalf("install controller: %v", err)
	}
}

// ControllerPod returns the newest controller Pod, or nil.
func (w *World) ControllerPod() *corev1.Pod {
	pods, err := w.Cluster.Client.CoreV1().Pods(ControlNamespace).List(context.Background(), metav1.ListOptions{LabelSelector: "app.kubernetes.io/name=sdo-controller"})
	if err != nil || len(pods.Items) == 0 {
		return nil
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].CreationTimestamp.After(pods.Items[j].CreationTimestamp.Time)
	})
	return &pods.Items[0]
}

// ControllerLog is the newest controller container's output so far.
func (w *World) ControllerLog() string {
	pod := w.ControllerPod()
	if pod == nil {
		return ""
	}
	return w.Kubelet.Run.Logs(pod.Namespace, pod.Name, "controller")
}

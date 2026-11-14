//go:build integration && controller_e2e

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

type ctrState struct {
	status            string // "", running, terminated
	started, finished time.Time
	exit              int
	reason, message   string
}

type podRun struct {
	phase      corev1.PodPhase
	containers map[string]ctrState
}

// Runtime runs the containers of process-backed Pods as bubblewrap sandboxes.
type Runtime struct {
	t       testing.TB
	cluster *Cluster
	images  *Images
	kubelet *Kubelet
	runDir  string

	// APIPort picks the API port a Pod's in-cluster config points at; the
	// harness routes the controller through its fault proxy.
	APIPort func(*corev1.Pod) int

	mu      sync.Mutex
	pods    map[types.UID]*podRun
	cancels map[types.UID]context.CancelFunc
	procs   map[string]*exec.Cmd // namespace/pod/container
	buffers map[string]*LogBuffer
	wg      sync.WaitGroup
}

func newRuntime(t testing.TB, cluster *Cluster, images *Images, runDir string, kubelet *Kubelet) *Runtime {
	return &Runtime{t: t, cluster: cluster, images: images, kubelet: kubelet, runDir: runDir, APIPort: func(*corev1.Pod) int { return cluster.Port },
		pods: map[types.UID]*podRun{}, cancels: map[types.UID]context.CancelFunc{}, procs: map[string]*exec.Cmd{}, buffers: map[string]*LogBuffer{}}
}

// PVCDir is where a claim's data lives on the host.
func (r *Runtime) PVCDir(namespace, claim string) string {
	return filepath.Join(r.runDir, "pvc", namespace, claim)
}

func (r *Runtime) ensure(pod *corev1.Pod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pods[pod.UID]; ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.pods[pod.UID] = &podRun{phase: corev1.PodPending, containers: map[string]ctrState{}}
	r.cancels[pod.UID] = cancel
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.runPod(ctx, pod.DeepCopy())
	}()
}

func (r *Runtime) snapshot(uid types.UID) podRun {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, ok := r.pods[uid]
	if !ok {
		return podRun{containers: map[string]ctrState{}}
	}
	copied := podRun{phase: state.phase, containers: map[string]ctrState{}}
	for name, c := range state.containers {
		copied.containers[name] = c
	}
	return copied
}

func (r *Runtime) setContainer(uid types.UID, name string, update func(*ctrState)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.pods[uid].containers[name]
	update(&state)
	r.pods[uid].containers[name] = state
}

func (r *Runtime) setPhase(uid types.UID, phase corev1.PodPhase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pods[uid].phase = phase
}

func (r *Runtime) kill(uid types.UID) {
	r.mu.Lock()
	cancel := r.cancels[uid]
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *Runtime) killAll() {
	r.mu.Lock()
	for _, cancel := range r.cancels {
		cancel()
	}
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Runtime) logs(namespace, pod, container string) *LogBuffer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buffers[namespace+"/"+pod+"/"+container]
}

// Logs returns a process-backed container's output so far.
func (r *Runtime) Logs(namespace, pod, container string) string {
	if buffer := r.logs(namespace, pod, container); buffer != nil {
		return buffer.render(false, 0, 0)
	}
	return ""
}

// Signal sends a signal to the sandbox of a running container whose Pod name starts with prefix.
// It returns how many sandboxes were signalled; the container's exit is then observed normally.
func (r *Runtime) Signal(namespace, podPrefix string, sig syscall.Signal) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for key, cmd := range r.procs {
		if strings.HasPrefix(key, namespace+"/"+podPrefix) && cmd.Process != nil && cmd.ProcessState == nil {
			_ = syscall.Kill(-cmd.Process.Pid, sig)
			count++
		}
	}
	return count
}

func (r *Runtime) runPod(ctx context.Context, pod *corev1.Pod) {
	podDir := filepath.Join(r.runDir, "pods", pod.Namespace+"_"+pod.Name)
	volumes, err := r.prepareVolumes(ctx, pod, podDir)
	if err != nil {
		r.failPod(pod, "VolumeSetupFailed", err.Error())
		return
	}
	sidecars, stopSidecars := context.WithCancel(ctx)
	defer stopSidecars()
	for _, c := range pod.Spec.InitContainers {
		if r.images.SkipInit[c.Name] {
			r.setContainer(pod.UID, c.Name, func(s *ctrState) {
				s.status, s.reason, s.started, s.finished = "terminated", "Completed", time.Now(), time.Now()
			})
			continue
		}
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			// A sidecar: it runs for the life of the Pod and the main containers start once it runs.
			go r.runContainer(sidecars, pod, c, podDir, volumes)
			continue
		}
		if code := r.runContainer(ctx, pod, c, podDir, volumes); code != 0 {
			r.setPhase(pod.UID, corev1.PodFailed)
			return
		}
	}
	r.setPhase(pod.UID, corev1.PodRunning)
	var group sync.WaitGroup
	var failed sync.Mutex
	failure := false
	for _, c := range pod.Spec.Containers {
		group.Add(1)
		go func() {
			defer group.Done()
			if r.runContainer(ctx, pod, c, podDir, volumes) != 0 {
				failed.Lock()
				failure = true
				failed.Unlock()
			}
		}()
	}
	group.Wait()
	if failure {
		r.setPhase(pod.UID, corev1.PodFailed)
	} else {
		r.setPhase(pod.UID, corev1.PodSucceeded)
	}
}

func (r *Runtime) failPod(pod *corev1.Pod, reason, message string) {
	for _, c := range pod.Spec.Containers {
		r.setContainer(pod.UID, c.Name, func(s *ctrState) {
			s.status, s.reason, s.message, s.exit, s.started, s.finished = "terminated", reason, message, 128, time.Now(), time.Now()
		})
	}
	r.setPhase(pod.UID, corev1.PodFailed)
}

var expansion = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)

func expand(value string, env map[string]string) string {
	return expansion.ReplaceAllStringFunc(value, func(match string) string {
		if v, ok := env[match[2:len(match)-1]]; ok {
			return v
		}
		return match
	})
}

func (r *Runtime) resolveEnv(ctx context.Context, pod *corev1.Pod, c corev1.Container) (map[string]string, error) {
	env := map[string]string{}
	for k, v := range r.images.Env {
		env[k] = v
	}
	client := r.cluster.Client
	for _, from := range c.EnvFrom {
		switch {
		case from.SecretRef != nil:
			secret, err := client.CoreV1().Secrets(pod.Namespace).Get(ctx, from.SecretRef.Name, metav1GetOptions)
			if err != nil {
				return nil, err
			}
			for k, v := range secret.Data {
				env[k] = string(v)
			}
		case from.ConfigMapRef != nil:
			cm, err := client.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, from.ConfigMapRef.Name, metav1GetOptions)
			if err != nil {
				return nil, err
			}
			for k, v := range cm.Data {
				env[k] = v
			}
		}
	}
	for _, e := range c.Env {
		switch {
		case e.ValueFrom == nil:
			env[e.Name] = expand(e.Value, env)
		case e.ValueFrom.FieldRef != nil:
			switch e.ValueFrom.FieldRef.FieldPath {
			case "metadata.uid":
				env[e.Name] = string(pod.UID)
			case "metadata.name":
				env[e.Name] = pod.Name
			case "metadata.namespace":
				env[e.Name] = pod.Namespace
			case "spec.nodeName":
				env[e.Name] = pod.Spec.NodeName
			case "status.podIP":
				env[e.Name] = r.kubelet.podIP(pod.UID)
			case "spec.serviceAccountName":
				env[e.Name] = pod.Spec.ServiceAccountName
			default:
				return nil, fmt.Errorf("unsupported fieldRef %s", e.ValueFrom.FieldRef.FieldPath)
			}
		case e.ValueFrom.SecretKeyRef != nil:
			secret, err := client.CoreV1().Secrets(pod.Namespace).Get(ctx, e.ValueFrom.SecretKeyRef.Name, metav1GetOptions)
			if err != nil {
				return nil, err
			}
			env[e.Name] = string(secret.Data[e.ValueFrom.SecretKeyRef.Key])
		case e.ValueFrom.ConfigMapKeyRef != nil:
			cm, err := client.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, e.ValueFrom.ConfigMapKeyRef.Name, metav1GetOptions)
			if err != nil {
				return nil, err
			}
			env[e.Name] = cm.Data[e.ValueFrom.ConfigMapKeyRef.Key]
		default:
			return nil, fmt.Errorf("unsupported env source for %s", e.Name)
		}
	}
	for k, v := range r.images.Force {
		env[k] = v
	}
	if automount(pod) {
		env["KUBERNETES_SERVICE_HOST"] = "127.0.0.1"
		env["KUBERNETES_SERVICE_PORT"] = fmt.Sprint(r.APIPort(pod))
		env["KUBERNETES_SERVICE_PORT_HTTPS"] = env["KUBERNETES_SERVICE_PORT"]
	}
	return env, nil
}

func automount(pod *corev1.Pod) bool {
	return pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken
}

// runContainer runs one container to completion and returns its exit code.
func (r *Runtime) runContainer(ctx context.Context, pod *corev1.Pod, c corev1.Container, podDir string, volumes map[string]string) int {
	key := pod.Namespace + "/" + pod.Name + "/" + c.Name
	buffer := &LogBuffer{}
	if err := os.MkdirAll(filepath.Join(podDir, "logs"), 0o777); err == nil {
		if file, err := os.Create(filepath.Join(podDir, "logs", c.Name+".log")); err == nil {
			defer file.Close()
			buffer.file = file
		}
	}
	r.mu.Lock()
	r.buffers[key] = buffer
	r.mu.Unlock()
	fail := func(reason string, err error) int {
		r.setContainer(pod.UID, c.Name, func(s *ctrState) {
			s.status, s.reason, s.message, s.exit, s.started, s.finished = "terminated", reason, err.Error(), 128, time.Now(), time.Now()
		})
		return 128
	}
	env, err := r.resolveEnv(ctx, pod, c)
	if err != nil {
		return fail("CreateContainerConfigError", err)
	}
	args, err := r.sandboxArgs(pod, c, podDir, volumes, env)
	if err != nil {
		return fail("CreateContainerError", err)
	}
	terminationPath := c.TerminationMessagePath
	if terminationPath == "" {
		terminationPath = "/dev/termination-log"
	}
	termination := filepath.Join(podDir, "termination-"+c.Name)
	if err := os.WriteFile(termination, nil, 0o666); err != nil {
		return fail("CreateContainerError", err)
	}
	args = append(args, "--bind", termination, terminationPath)
	command := append(append([]string{}, c.Command...), c.Args...)
	if len(command) == 0 {
		return fail("CreateContainerError", errors.New("container has no command: images carry no entrypoint"))
	}
	for i := range command {
		command[i] = expand(command[i], env)
	}
	if c.WorkingDir != "" {
		args = append(args, "--chdir", c.WorkingDir)
	}
	for _, name := range sortedKeys(env) {
		args = append(args, "--setenv", name, env[name])
	}
	final := append([]string{"--clearenv"}, args...)
	final = append(final, "--")
	final = append(final, command...)
	cmd := exec.Command("bwrap", final...)
	cmd.Stdout, cmd.Stderr = buffer, buffer
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		return fail("RunContainerError", err)
	}
	r.mu.Lock()
	r.procs[key] = cmd
	r.mu.Unlock()
	r.setContainer(pod.UID, c.Name, func(s *ctrState) { s.status, s.started = "running", time.Now() })
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	var waitErr error
	select {
	case waitErr = <-exited:
	case <-ctx.Done():
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		waitErr = <-exited
	}
	code := 0
	if waitErr != nil {
		var exit *exec.ExitError
		if errors.As(waitErr, &exit) {
			code = exit.ExitCode()
			if code < 0 {
				code = 137
			}
		} else {
			code = 1
		}
	}
	message, _ := os.ReadFile(termination)
	if len(message) > 4096 {
		message = message[:4096]
	}
	_ = os.WriteFile(filepath.Join(podDir, "logs", c.Name+".exit"), []byte(fmt.Sprintf("exit=%d message=%s\n", code, bytes.TrimSpace(message))), 0o644)
	r.setContainer(pod.UID, c.Name, func(s *ctrState) {
		s.status, s.exit, s.finished, s.message = "terminated", code, time.Now(), string(bytes.TrimSpace(message))
		s.reason = "Completed"
		if code != 0 {
			s.reason = "Error"
		}
	})
	return code
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sandboxArgs builds the bubblewrap mount list: the host userland read-only,
// the Pod's volumes at their mount paths, then the image's extra binds.
func (r *Runtime) sandboxArgs(pod *corev1.Pod, c corev1.Container, podDir string, volumes map[string]string, env map[string]string) ([]string, error) {
	args := []string{"--die-with-parent", "--unshare-pid", "--unshare-ipc", "--tmpfs", "/",
		"--ro-bind", "/usr", "/usr", "--symlink", "usr/bin", "/bin", "--symlink", "usr/sbin", "/sbin",
		"--symlink", "usr/lib", "/lib", "--symlink", "usr/lib64", "/lib64",
		"--tmpfs", "/etc"}
	// /etc is a writable tmpfs holding read-only binds of the host's entries, so a Pod volume can mount at a
	// path the host lacks (the controller's /etc/sdo config) without bubblewrap needing to create it on a
	// read-only /etc.
	etcEntries, err := os.ReadDir("/etc")
	if err != nil {
		return nil, err
	}
	for _, entry := range etcEntries {
		path := filepath.Join("/etc", entry.Name())
		if entry.Name() == "hosts" {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return nil, err
			}
			args = append(args, "--symlink", target, path)
			continue
		}
		args = append(args, "--ro-bind", path, path)
	}
	args = append(args, "--ro-bind", r.kubelet.net.hostsPath, "/etc/hosts", "--ro-bind", "/mnt", "/mnt", "--ro-bind", "/home", "/home",
		"--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp", "--tmpfs", "/usr/local", "--tmpfs", "/go")
	type mount struct {
		src, dst string
		ro       bool
	}
	var mounts []mount
	for _, m := range c.VolumeMounts {
		source, ok := volumes[m.Name]
		if !ok {
			return nil, fmt.Errorf("container %s mounts unknown volume %s", c.Name, m.Name)
		}
		if m.SubPath != "" {
			source = filepath.Join(source, m.SubPath)
			if _, err := os.Stat(source); os.IsNotExist(err) {
				if err := os.MkdirAll(source, 0o777); err != nil {
					return nil, err
				}
			}
		}
		if r.imageProvides(m.MountPath) {
			continue // the toolchain volume the init container would fill: the image supplies it from the host
		}
		mounts = append(mounts, mount{source, m.MountPath, m.ReadOnly})
	}
	if automount(pod) {
		mounts = append(mounts, mount{volumes["__serviceaccount"], "/var/run/secrets/kubernetes.io/serviceaccount", true})
	}
	sort.SliceStable(mounts, func(i, j int) bool { return strings.Count(mounts[i].dst, "/") < strings.Count(mounts[j].dst, "/") })
	for _, m := range mounts {
		flag := "--bind"
		if m.ro {
			flag = "--ro-bind"
		}
		args = append(args, flag, m.src, m.dst)
	}
	for _, b := range r.images.Binds {
		flag := "--bind"
		if b.RO {
			flag = "--ro-bind"
		}
		args = append(args, flag, b.Src, b.Dst)
	}
	return args, nil
}

func (r *Runtime) imageProvides(path string) bool {
	for _, b := range r.images.Binds {
		if b.Dst == path || strings.HasPrefix(b.Dst, path+"/") {
			return true
		}
	}
	return false
}

// prepareVolumes materialises every volume of the Pod on the host and returns name to directory.
func (r *Runtime) prepareVolumes(ctx context.Context, pod *corev1.Pod, podDir string) (map[string]string, error) {
	volumes := map[string]string{}
	base := filepath.Join(podDir, "volumes")
	if err := os.MkdirAll(base, 0o777); err != nil {
		return nil, err
	}
	for _, v := range pod.Spec.Volumes {
		dir := filepath.Join(base, v.Name)
		switch {
		case v.EmptyDir != nil:
			if err := os.MkdirAll(dir, 0o777); err != nil {
				return nil, err
			}
		case v.PersistentVolumeClaim != nil:
			dir = r.PVCDir(pod.Namespace, v.PersistentVolumeClaim.ClaimName)
			if err := os.MkdirAll(dir, 0o777); err != nil {
				return nil, err
			}
		case v.ConfigMap != nil:
			cm, err := r.cluster.Client.CoreV1().ConfigMaps(pod.Namespace).Get(ctx, v.ConfigMap.Name, metav1GetOptions)
			if err != nil {
				if v.ConfigMap.Optional != nil && *v.ConfigMap.Optional {
					_ = os.MkdirAll(dir, 0o777)
					break
				}
				return nil, fmt.Errorf("configmap volume %s: %w", v.Name, err)
			}
			files := map[string][]byte{}
			for k, val := range cm.Data {
				files[k] = []byte(val)
			}
			for k, val := range cm.BinaryData {
				files[k] = val
			}
			if err := writeFiles(dir, files); err != nil {
				return nil, err
			}
		case v.Secret != nil:
			secret, err := r.cluster.Client.CoreV1().Secrets(pod.Namespace).Get(ctx, v.Secret.SecretName, metav1GetOptions)
			if err != nil {
				if v.Secret.Optional != nil && *v.Secret.Optional {
					_ = os.MkdirAll(dir, 0o777)
					break
				}
				return nil, fmt.Errorf("secret volume %s: %w", v.Name, err)
			}
			if err := writeFiles(dir, secret.Data); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("volume %s: the fake kubelet supports emptyDir, persistentVolumeClaim, configMap and secret", v.Name)
		}
		volumes[v.Name] = dir
	}
	if automount(pod) {
		dir := filepath.Join(base, "__serviceaccount")
		ca, _ := os.ReadFile(r.cluster.CAPath)
		if err := writeFiles(dir, map[string][]byte{"token": []byte(r.cluster.Token), "ca.crt": ca, "namespace": []byte(pod.Namespace)}); err != nil {
			return nil, err
		}
		volumes["__serviceaccount"] = dir
	}
	return volumes, nil
}

func writeFiles(dir string, files map[string][]byte) error {
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return err
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o644); err != nil {
			return err
		}
	}
	return nil
}

var metav1GetOptions = metav1.GetOptions{}

// LogLine is one line of container output with the time the harness saw it.
type LogLine struct {
	At   time.Time
	Text string
}

// Lines returns a process-backed container's output with arrival times.
func (r *Runtime) Lines(namespace, pod, container string) []LogLine {
	buffer := r.logs(namespace, pod, container)
	if buffer == nil {
		return nil
	}
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	lines := make([]LogLine, len(buffer.lines))
	for i, l := range buffer.lines {
		lines[i] = LogLine{At: l.at, Text: l.text}
	}
	return lines
}

package operatorcli

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"k8s.io/client-go/kubernetes"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/install"
	"sdo.dev/operator/worker_registry"
)

const runtimeArtifactsArchiveScript = `cd "$1" 2>/dev/null || exit 0; find usage telemetry sessions setup codex/sessions claude/projects -type f 2>/dev/null | while IFS= read -r path; do lower_path=$(printf '%s' "$path" | tr '[:upper:]' '[:lower:]'); case "$lower_path" in *cache*|*plugin*|*catalog*) continue;; esac; base=${lower_path##*/}; case "$base" in *auth*|*credential*|*token*) [ "$base" = token_usage.json ] || continue;; esac; case "$path" in setup/*/*.jsonl|usage/*.jsonl|telemetry/*.jsonl|telemetry/*.jsonl.1|sessions/*/*.jsonl|sessions/*/token_usage.json|sessions/*/scratch/sdo-reports/*/[0-9][0-9][0-9][0-9]-act.json|sessions/*/scratch/sdo-reports/*/[0-9][0-9][0-9][0-9]-apply.json|sessions/*/scratch/sdo-reports/*/[0-9][0-9][0-9][0-9]-mitigate.json|sessions/*/startup-controller.json|sessions/*/startup-workers/*/startup-worker.json|sessions/*/startup/attempts/*/startup-controller.json|sessions/*/startup/attempts/*/startup-dispatch.json|sessions/*/startup-worker.json|sessions/*/startup-daemon.json|sessions/*/startup-session.json|sessions/*/startup-dispatch.json|sessions/*/corpus/*.json|sessions/*/model_io/*/*.json|sessions/*/checkpoints/*/request.json|sessions/*/checkpoints/*/result.json|sessions/*/checkpoints/*/file_edits.json|sessions/*/codex/sessions/*/rollout-*.jsonl|sessions/*/claude/projects/*.jsonl|codex/sessions/*/rollout-*.jsonl|claude/projects/*.jsonl) printf '%s\n' "$path";; esac; done | tar -cf - -T -`

// runtimeSnapshotScript seals the whitelisted runtime evidence into one
// immutable archive: $1 runtime root, $2 snapshot directory, $3 snapshot ID,
// $4 archive script. Publication is a hard link, so an existing archive is
// never replaced and a retry reports the archive sealed at the boundary.
const runtimeSnapshotScript = `set -eu; archive="$2/$3.tar"; mkdir -p "$2"; state=existing; if [ ! -e "$archive" ]; then partial="$2/.$3.$$.partial"; trap 'rm -f "$partial"' EXIT; sh -c "$4" sh "$1" > "$partial"; chmod 0444 "$partial"; if ln "$partial" "$archive" 2>/dev/null; then state=created; fi; fi; size=$(wc -c < "$archive"); set -- $(sha256sum "$archive"); printf '%s %s %s\n' "$state" "$1" "$size"`

const (
	runtimeSnapshotRoot          = "/workspace/.sdo-runtime-snapshots"
	runtimeSnapshotReadScript    = `exec cat "$1/$2.tar"`
	runtimeSnapshotRetireScript  = `rm -f "$1/$2.tar"`
	runtimeSnapshotMarker        = ".sdo-snapshot"
	runtimeArtifactsModeExport   = "export"
	runtimeArtifactsModeSnapshot = "snapshot"
	runtimeArtifactsModeDeliver  = "deliver"
)

var (
	runtimeSnapshotID     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	runtimeSnapshotDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// RuntimeArtifactsConfig is a read-only export of whitelisted runtime evidence.
// It contains no agent credentials and does not expose the accepted checkout.
// Export copies the live cumulative evidence. Snapshot seals it on the
// repository PVC at an incident boundary; deliver later copies exactly that
// sealed archive, so evidence never includes a later incident's files.
type RuntimeArtifactsConfig struct {
	Sync                  install.SyncConfig
	OutputDir, Kubeconfig string
	Mode, SnapshotID      string
	SHA256                string
}
type RuntimeArtifactsResult struct {
	Directory  string `json:"directory,omitempty"`
	SnapshotID string `json:"snapshot_id,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Bytes      int64  `json:"bytes,omitempty"`
	// Created is false when the snapshot already existed and was not recaptured.
	Created bool `json:"created,omitempty"`
	// Retired reports that delivery removed the sealed archive from the PVC.
	Retired bool `json:"retired,omitempty"`
	// SealedBy names the Pod kind that sealed the archive: controller or sync.
	SealedBy string `json:"sealed_by,omitempty"`
}

func ParseRuntimeArtifactsLaunch(args []string) (RuntimeArtifactsConfig, error) {
	if len(args) == 0 || args[0] != "runtime-artifacts" {
		return RuntimeArtifactsConfig{}, errors.New("expected runtime-artifacts subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator runtime-artifacts", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	control := flags.String("control-namespace", "", "operator namespace")
	pvc := flags.String("repository-pvc", "", "shared repository PVC")
	image := flags.String("controller-image", "", "controller image")
	output := flags.String("output-dir", "", "results parent directory")
	kubeconfig := flags.String("kubeconfig", "", "Kubernetes configuration")
	mode := flags.String("mode", runtimeArtifactsModeExport, "export, snapshot, or deliver")
	snapshot := flags.String("snapshot-id", "", "sealed snapshot identifier")
	digest := flags.String("sha256", "", "sealed snapshot digest to deliver")
	if err := flags.Parse(args[1:]); err != nil {
		return RuntimeArtifactsConfig{}, err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*control) == "" || strings.TrimSpace(*pvc) == "" || strings.TrimSpace(*image) == "" {
		return RuntimeArtifactsConfig{}, errors.New("runtime-artifacts requires namespace, PVC, and image")
	}
	switch *mode {
	case runtimeArtifactsModeExport:
		if *snapshot != "" || *digest != "" {
			return RuntimeArtifactsConfig{}, errors.New("runtime-artifacts export takes no snapshot")
		}
	case runtimeArtifactsModeSnapshot:
		if !runtimeSnapshotID.MatchString(*snapshot) || *digest != "" || *output != "" {
			return RuntimeArtifactsConfig{}, errors.New("runtime-artifacts snapshot requires only a valid snapshot ID")
		}
	case runtimeArtifactsModeDeliver:
		if !runtimeSnapshotID.MatchString(*snapshot) || !runtimeSnapshotDigest.MatchString(*digest) {
			return RuntimeArtifactsConfig{}, errors.New("runtime-artifacts deliver requires a valid snapshot ID and SHA-256 digest")
		}
	default:
		return RuntimeArtifactsConfig{}, fmt.Errorf("unknown runtime-artifacts mode %q", *mode)
	}
	if *mode != runtimeArtifactsModeSnapshot && strings.TrimSpace(*output) == "" {
		return RuntimeArtifactsConfig{}, errors.New("runtime-artifacts requires an output directory")
	}
	if (*output != "" && !filepath.IsAbs(*output)) || (*kubeconfig != "" && !filepath.IsAbs(*kubeconfig)) {
		return RuntimeArtifactsConfig{}, errors.New("output and kubeconfig paths must be absolute")
	}
	return RuntimeArtifactsConfig{Sync: install.SyncConfig{ControlNamespace: *control, RepositoryPVC: *pvc, ControllerImage: *image}, OutputDir: *output, Kubeconfig: *kubeconfig, Mode: *mode, SnapshotID: *snapshot, SHA256: *digest}, nil
}

func RunRuntimeArtifacts(ctx context.Context, cfg RuntimeArtifactsConfig) (RuntimeArtifactsResult, error) {
	config, err := hostKubernetesConfig(cfg.Kubeconfig)
	if err != nil {
		return RuntimeArtifactsResult{}, err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return RuntimeArtifactsResult{}, err
	}
	run, err := newContainerExecutor(config, client)
	if err != nil {
		return RuntimeArtifactsResult{}, err
	}
	execute := inContainer(run, "")
	installer := install.Installer{Client: client}
	switch cfg.Mode {
	case runtimeArtifactsModeSnapshot:
		// The running controller already mounts the PVC, so sealing there costs
		// one exec instead of a sync Pod start on the row's critical path.
		if pod, container, err := sealingController(ctx, client, cfg.Sync); err == nil && pod != "" {
			result, err := sealRuntimeSnapshot(ctx, inContainer(run, container), cfg.Sync.ControlNamespace, pod, "/workspace/.sdo-runtime", runtimeSnapshotRoot, cfg.SnapshotID)
			if err == nil {
				result.SealedBy = "controller"
				return result, nil
			}
		}
		var result RuntimeArtifactsResult
		err := installer.WithSyncPod(ctx, cfg.Sync, func(ctx context.Context, _ install.Config) error {
			var err error
			result, err = sealRuntimeSnapshot(ctx, execute, cfg.Sync.ControlNamespace, install.RepositorySyncPodName, "/workspace/.sdo-runtime", runtimeSnapshotRoot, cfg.SnapshotID)
			result.SealedBy = "sync"
			return err
		})
		return result, err
	case runtimeArtifactsModeDeliver:
		// An already delivered snapshot needs no Pod; the archive may be retired.
		if delivered, err := runtimeSnapshotDelivered(cfg.OutputDir, cfg.SnapshotID, cfg.SHA256); err != nil || delivered {
			return RuntimeArtifactsResult{Directory: filepath.Join(cfg.OutputDir, "sdo_runtime"), SnapshotID: cfg.SnapshotID, SHA256: cfg.SHA256}, err
		}
		var result RuntimeArtifactsResult
		err := installer.WithRuntimeExportPod(ctx, cfg.Sync, func(ctx context.Context, _ install.Config) error {
			var err error
			result, err = deliverRuntimeSnapshot(ctx, execute, cfg.Sync.ControlNamespace, install.RuntimeExportPodName, runtimeSnapshotRoot, cfg.SnapshotID, cfg.SHA256, cfg.OutputDir)
			return err
		})
		return result, err
	}
	destination := filepath.Join(cfg.OutputDir, "sdo_runtime")
	if err := os.MkdirAll(destination, 0700); err != nil {
		return RuntimeArtifactsResult{}, err
	}
	err = installer.WithSyncPod(ctx, cfg.Sync, func(ctx context.Context, _ install.Config) error {
		return copyRuntimeArtifacts(ctx, execute, cfg.Sync.ControlNamespace, destination)
	})
	if err != nil {
		return RuntimeArtifactsResult{}, err
	}
	return RuntimeArtifactsResult{Directory: destination}, nil
}

// copyRuntimeArtifacts streams the sync Pod's evidence archive into destination.
// Archive only evidence files; extraction applies the same allowlist.
func copyRuntimeArtifacts(ctx context.Context, execute podExecutor, namespace, destination string) error {
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	reader, writer := io.Pipe()
	defer reader.Close()
	var stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		err := execute(streamCtx, namespace, install.RepositorySyncPodName, []string{"sh", "-c", runtimeArtifactsArchiveScript, "sh", "/workspace/.sdo-runtime"}, writer, &stderr)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	copyErr := extractRuntimeArtifacts(reader, destination)
	if copyErr != nil {
		stop()
		_ = reader.CloseWithError(copyErr)
	}
	waitErr := <-done
	if waitErr != nil {
		// A producer failing partway also breaks the copy; its stderr says why.
		waitErr = fmt.Errorf("export runtime artifacts: %w: %s", waitErr, strings.TrimSpace(stderr.String()))
	}
	if copyErr != nil && waitErr != nil && !errors.Is(waitErr, copyErr) {
		return errors.Join(copyErr, waitErr)
	}
	if waitErr != nil {
		return waitErr
	}
	return copyErr
}

// sealingController returns the one running controller Pod and container that
// mount the lane repository PVC at /workspace, or "" when there is none.
func sealingController(ctx context.Context, client kubernetes.Interface, sync install.SyncConfig) (string, string, error) {
	pods, err := client.CoreV1().Pods(sync.ControlNamespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + install.ControllerJobName})
	if err != nil {
		return "", "", err
	}
	var pod, container string
	for _, candidate := range pods.Items {
		if candidate.Status.Phase != corev1.PodRunning || candidate.DeletionTimestamp != nil {
			continue
		}
		volumes := map[string]bool{}
		for _, volume := range candidate.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == sync.RepositoryPVC && !volume.PersistentVolumeClaim.ReadOnly {
				volumes[volume.Name] = true
			}
		}
		for _, c := range candidate.Spec.Containers {
			for _, mount := range c.VolumeMounts {
				if mount.MountPath == "/workspace" && volumes[mount.Name] && !mount.ReadOnly && mount.SubPath == "" {
					if pod != "" {
						return "", "", nil
					}
					pod, container = candidate.Name, c.Name
				}
			}
		}
	}
	return pod, container, nil
}

// sealRuntimeSnapshot freezes the current whitelisted evidence under snapshotRoot.
func sealRuntimeSnapshot(ctx context.Context, execute podExecutor, namespace, pod, runtimeRoot, snapshotRoot, id string) (RuntimeArtifactsResult, error) {
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, namespace, pod, []string{"sh", "-c", runtimeSnapshotScript, "sh", runtimeRoot, snapshotRoot, id, runtimeArtifactsArchiveScript}, &stdout, &stderr); err != nil {
		return RuntimeArtifactsResult{}, fmt.Errorf("seal runtime snapshot %s: %w: %s", id, err, strings.TrimSpace(stderr.String()))
	}
	var state, digest string
	var size int64
	if _, err := fmt.Sscanf(strings.TrimSpace(stdout.String()), "%s %s %d", &state, &digest, &size); err != nil || (state != "created" && state != "existing") || !runtimeSnapshotDigest.MatchString(digest) || size < 0 {
		return RuntimeArtifactsResult{}, fmt.Errorf("seal runtime snapshot %s: unexpected report %q", id, strings.TrimSpace(stdout.String()))
	}
	return RuntimeArtifactsResult{SnapshotID: id, SHA256: digest, Bytes: size, Created: state == "created"}, nil
}

func runtimeSnapshotDelivered(outputDir, id, digest string) (bool, error) {
	marker, err := os.ReadFile(filepath.Join(outputDir, "sdo_runtime", runtimeSnapshotMarker))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return string(marker) == id+" "+digest+"\n", nil
}

// deliverRuntimeSnapshot copies exactly one sealed archive into
// outputDir/sdo_runtime. It verifies the digest before publishing and replaces
// the destination only as a whole, so a failed attempt leaves no partial tree.
func deliverRuntimeSnapshot(ctx context.Context, execute podExecutor, namespace, pod, snapshotRoot, id, digest, outputDir string) (RuntimeArtifactsResult, error) {
	destination := filepath.Join(outputDir, "sdo_runtime")
	result := RuntimeArtifactsResult{Directory: destination, SnapshotID: id, SHA256: digest}
	if delivered, err := runtimeSnapshotDelivered(outputDir, id, digest); err != nil || delivered {
		return result, err
	}
	if err := os.MkdirAll(outputDir, 0700); err != nil {
		return result, err
	}
	staging, err := os.MkdirTemp(outputDir, ".sdo_runtime-"+id+"-")
	if err != nil {
		return result, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	hash := sha256.New()
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	reader, writer := io.Pipe()
	defer reader.Close()
	var stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		err := execute(streamCtx, namespace, pod, []string{"sh", "-c", runtimeSnapshotReadScript, "sh", snapshotRoot, id}, writer, &stderr)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	copyErr := extractRuntimeArtifacts(io.TeeReader(reader, hash), staging)
	if copyErr != nil {
		stop()
		_ = reader.CloseWithError(copyErr)
	}
	if waitErr := <-done; waitErr != nil {
		return result, errors.Join(copyErr, fmt.Errorf("read runtime snapshot %s: %w: %s", id, waitErr, strings.TrimSpace(stderr.String())))
	}
	if copyErr != nil {
		return result, copyErr
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != digest {
		return result, fmt.Errorf("runtime snapshot %s digest %s does not match sealed digest %s", id, got, digest)
	}
	if err := os.WriteFile(filepath.Join(staging, runtimeSnapshotMarker), []byte(id+" "+digest+"\n"), 0600); err != nil {
		return result, err
	}
	// The live stage-end export is superseded by the sealed boundary snapshot.
	if err := os.RemoveAll(destination); err != nil {
		return result, err
	}
	if err := os.Rename(staging, destination); err != nil {
		return result, err
	}
	published = true
	var retireErr bytes.Buffer
	result.Retired = execute(ctx, namespace, pod, []string{"sh", "-c", runtimeSnapshotRetireScript, "sh", snapshotRoot, id}, io.Discard, &retireErr) == nil
	return result, nil
}

var commandReportName = regexp.MustCompile(`^[0-9]{4,}-(act|apply|mitigate)\.json$`)

func allowedRuntimeArtifact(name string) bool {
	parts := strings.Split(name, "/")
	lowerPath := strings.ToLower(name)
	if strings.Contains(lowerPath, "cache") || strings.Contains(lowerPath, "plugin") || strings.Contains(lowerPath, "catalog") {
		return false
	}
	base := strings.ToLower(parts[len(parts)-1])
	if base != "token_usage.json" && (strings.Contains(base, "auth") || strings.Contains(base, "credential") || strings.Contains(base, "token")) {
		return false
	}
	if len(parts) == 2 && (parts[0] == "usage" || parts[0] == "telemetry") {
		return strings.HasSuffix(base, ".jsonl") || parts[0] == "telemetry" && strings.HasSuffix(base, ".jsonl.1")
	}
	if len(parts) >= 3 && parts[0] == "codex" && parts[1] == "sessions" {
		return strings.HasPrefix(base, "rollout-") && strings.HasSuffix(base, ".jsonl")
	}
	if len(parts) >= 3 && parts[0] == "claude" && parts[1] == "projects" {
		return strings.HasSuffix(base, ".jsonl")
	}
	if len(parts) == 3 && parts[0] == "setup" {
		return strings.HasSuffix(base, ".jsonl")
	}
	if len(parts) < 3 || parts[0] != "sessions" {
		return false
	}
	if len(parts) == 3 {
		return strings.HasSuffix(base, ".jsonl") || base == "token_usage.json" || isStartupTimingArtifact(base)
	}
	if len(parts) == 5 && parts[2] == "startup-workers" {
		return base == "startup-worker.json"
	}
	if len(parts) == 6 && parts[2] == "startup" && parts[3] == "attempts" {
		return base == "startup-controller.json" || base == "startup-dispatch.json"
	}
	if len(parts) == 4 && parts[2] == "startup" {
		return base == "startup-controller.json" || base == "startup-session.json" || base == "startup-dispatch.json"
	}
	if len(parts) == 6 && parts[2] == "scratch" && parts[3] == "sdo-reports" {
		return commandReportName.MatchString(parts[5])
	}
	if len(parts) == 4 && parts[2] == "corpus" {
		return strings.HasSuffix(base, ".json")
	}
	if len(parts) == 5 && parts[2] == "checkpoints" {
		return base == "request.json" || base == "result.json" || base == "file_edits.json"
	}
	if len(parts) == 5 && parts[2] == "model_io" {
		return strings.HasSuffix(base, ".json")
	}
	if len(parts) >= 5 && parts[2] == "codex" && parts[3] == "sessions" {
		return strings.HasPrefix(base, "rollout-") && strings.HasSuffix(base, ".jsonl")
	}
	return len(parts) >= 5 && parts[2] == "claude" && parts[3] == "projects" && strings.HasSuffix(base, ".jsonl")
}

func isStartupTimingArtifact(base string) bool {
	switch base {
	case "startup-controller.json", "startup-worker.json", "startup-daemon.json", "startup-session.json", "startup-dispatch.json":
		return true
	}
	return false
}

func extractRuntimeArtifacts(source io.Reader, destination string) error {
	reader := tar.NewReader(source)
	var total int64
	extracted := make(map[string]int64)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			// Consume the record padding after the end marker so the
			// producer's stream can complete instead of blocking on write.
			if _, err := io.Copy(io.Discard, source); err != nil {
				return fmt.Errorf("drain runtime artifact archive: %w", err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("read runtime artifact archive: %w", err)
		}
		name := strings.TrimSuffix(header.Name, "/")
		clean := filepath.Clean(name)
		if filepath.IsAbs(name) || clean != name || strings.Contains(name, "\\") || strings.HasPrefix(name, "../") || strings.Contains(name, "/../") {
			return fmt.Errorf("runtime artifact archive contains unsafe path %q", header.Name)
		}
		if !allowedRuntimeArtifact(name) {
			if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA && header.Typeflag != tar.TypeDir {
				return fmt.Errorf("runtime artifact archive contains unsupported entry %q", name)
			}
			continue
		}
		path := filepath.Join(destination, name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0700); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA, tar.TypeLink:
			var content io.Reader = reader
			if header.Typeflag == tar.TypeLink {
				size, ok := extracted[header.Linkname]
				if !ok || !allowedRuntimeArtifact(header.Linkname) {
					return fmt.Errorf("runtime artifact hardlink has unsafe or unavailable target %q", header.Linkname)
				}
				data, err := os.ReadFile(filepath.Join(destination, header.Linkname))
				if err != nil {
					return err
				}
				content = bytes.NewReader(data)
				header.Size = size
			}
			if header.Size < 0 || header.Size > 128<<20 {
				return errors.New("runtime artifact file exceeds 128 MiB")
			}

			if isStartupTimingArtifact(filepath.Base(name)) {
				if header.Size > 64<<10 {
					return errors.New("startup timing artifact exceeds 64 KiB")
				}
				data, err := io.ReadAll(io.LimitReader(content, header.Size))
				if err != nil {
					return err
				}
				if err := worker_registry.ValidateStartupArtifact(filepath.Base(name), data); err != nil {
					return err
				}
				content = bytes.NewReader(data)
			}
			total += header.Size
			if total > 1<<30 {
				return errors.New("runtime artifact archive exceeds 1 GiB")
			}
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return err
			}
			file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			_, copyErr := io.CopyN(file, content, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			extracted[name] = header.Size
		default:
			return fmt.Errorf("runtime artifact archive contains unsupported entry %q", name)
		}
	}
}

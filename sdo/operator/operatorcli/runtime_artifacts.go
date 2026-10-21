package operatorcli

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"sdo.dev/operator/install"
)

// RuntimeArtifactsConfig is a read-only export of whitelisted runtime evidence.
// It contains no agent credentials and does not expose the accepted checkout.
type RuntimeArtifactsConfig struct {
	Sync                  install.SyncConfig
	OutputDir, Kubeconfig string
}
type RuntimeArtifactsResult struct {
	Directory string `json:"directory"`
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
	if err := flags.Parse(args[1:]); err != nil {
		return RuntimeArtifactsConfig{}, err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*control) == "" || strings.TrimSpace(*pvc) == "" || strings.TrimSpace(*image) == "" || strings.TrimSpace(*output) == "" {
		return RuntimeArtifactsConfig{}, errors.New("runtime-artifacts requires namespace, PVC, image, and output directory")
	}
	if !filepath.IsAbs(*output) || (*kubeconfig != "" && !filepath.IsAbs(*kubeconfig)) {
		return RuntimeArtifactsConfig{}, errors.New("output and kubeconfig paths must be absolute")
	}
	return RuntimeArtifactsConfig{Sync: install.SyncConfig{ControlNamespace: *control, RepositoryPVC: *pvc, ControllerImage: *image}, OutputDir: *output, Kubeconfig: *kubeconfig}, nil
}

func RunRuntimeArtifacts(ctx context.Context, cfg RuntimeArtifactsConfig) (RuntimeArtifactsResult, error) {
	client, err := hostKubernetesClient(cfg.Kubeconfig)
	if err != nil {
		return RuntimeArtifactsResult{}, err
	}
	destination := filepath.Join(cfg.OutputDir, "sdo_runtime")
	if err := os.MkdirAll(destination, 0700); err != nil {
		return RuntimeArtifactsResult{}, err
	}
	installer := install.Installer{Client: client}
	err = installer.WithSyncPod(ctx, cfg.Sync, func(ctx context.Context, _ install.Config) error {
		// Archive only evidence files; extraction applies the same allowlist.
		script := `cd /workspace/.sdo-runtime 2>/dev/null || exit 0; find usage telemetry sessions codex/sessions claude/projects -type f 2>/dev/null | while IFS= read -r path; do lower_path=$(printf '%s' "$path" | tr '[:upper:]' '[:lower:]'); case "$lower_path" in *cache*|*plugin*|*catalog*) continue;; esac; base=${lower_path##*/}; case "$base" in *auth*|*credential*|*token*) [ "$base" = token_usage.json ] || continue;; esac; case "$path" in usage/*.jsonl|telemetry/*.jsonl|telemetry/*.jsonl.1|sessions/*/*.jsonl|sessions/*/token_usage.json|sessions/*/corpus/*.json|sessions/*/model_io/*/*.json|sessions/*/codex/sessions/*/rollout-*.jsonl|sessions/*/claude/projects/*.jsonl|codex/sessions/*/rollout-*.jsonl|claude/projects/*.jsonl) printf '%s\n' "$path";; esac; done | tar -cf - -T -`
		command := exec.CommandContext(ctx, "kubectl", "--namespace", cfg.Sync.ControlNamespace, "exec", install.RepositorySyncPodName, "--", "sh", "-c", script)
		if cfg.Kubeconfig != "" {
			command.Env = append(os.Environ(), "KUBECONFIG="+cfg.Kubeconfig)
		}
		pipe, err := command.StdoutPipe()
		if err != nil {
			return err
		}
		var stderr bytes.Buffer
		command.Stderr = &stderr
		if err := command.Start(); err != nil {
			return err
		}
		copyErr := extractRuntimeArtifacts(pipe, destination)
		if copyErr != nil {
			_ = command.Process.Kill()
		}
		waitErr := command.Wait()
		if copyErr != nil {
			return copyErr
		}
		if waitErr != nil {
			return fmt.Errorf("export runtime artifacts: %w: %s", waitErr, strings.TrimSpace(stderr.String()))
		}
		return nil
	})
	if err != nil {
		return RuntimeArtifactsResult{}, err
	}
	return RuntimeArtifactsResult{Directory: destination}, nil
}

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
	if len(parts) < 3 || parts[0] != "sessions" {
		return false
	}
	if len(parts) == 3 {
		return strings.HasSuffix(base, ".jsonl") || base == "token_usage.json"
	}
	if len(parts) == 4 && parts[2] == "corpus" {
		return strings.HasSuffix(base, ".json")
	}
	if len(parts) == 5 && parts[2] == "model_io" {
		return strings.HasSuffix(base, ".json")
	}
	if len(parts) >= 5 && parts[2] == "codex" && parts[3] == "sessions" {
		return strings.HasPrefix(base, "rollout-") && strings.HasSuffix(base, ".jsonl")
	}
	return len(parts) >= 5 && parts[2] == "claude" && parts[3] == "projects" && strings.HasSuffix(base, ".jsonl")
}

func extractRuntimeArtifacts(source io.Reader, destination string) error {
	reader := tar.NewReader(source)
	var total int64
	for {
		header, err := reader.Next()
		if err == io.EOF {
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
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 || header.Size > 128<<20 {
				return errors.New("runtime artifact file exceeds 128 MiB")
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
			_, copyErr := io.CopyN(file, reader, header.Size)
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		default:
			return fmt.Errorf("runtime artifact archive contains unsupported entry %q", name)
		}
	}
}

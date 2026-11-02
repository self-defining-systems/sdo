package toolchain

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
)

// SharedSubPath identifies a controller-image-bound, immutable compiler seed.
// Workers receive only these two read-only directories, never the PVC root.
func SharedSubPath(image string) string {
	return fmt.Sprintf(".sdo-runtime/toolchains/%x", sha256.Sum256([]byte(image)))
}

// Seed prepares the offline compiler before the controller accepts incidents.
// A completed seed is immutable and can be reused by a restarted controller.
// Atomic publication prevents a failed copy from looking complete.
func Seed(ctx context.Context, repositoryMount, image string) error {
	return seedFrom(ctx, repositoryMount, image, "/usr/local/go", "/go")
}

func seedFrom(ctx context.Context, repositoryMount, image, compiler, cache string) error {
	destination := filepath.Join(repositoryMount, SharedSubPath(image))
	if info, err := os.Lstat(destination); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("toolchain seed is not a directory: %s", destination)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
		return err
	}
	temporary, err := os.MkdirTemp(filepath.Dir(destination), ".seed-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporary)
	for _, source := range []struct{ from, to string }{{compiler, "go"}, {cache, "cache"}} {
		command := exec.CommandContext(ctx, "cp", "-R", source.from, filepath.Join(temporary, source.to))
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("seed offline toolchain: %w: %s", err, output)
		}
	}
	if err := os.Rename(temporary, destination); err != nil {
		if info, statErr := os.Stat(destination); statErr == nil && info.IsDir() {
			return nil
		}
		return err
	}
	return nil
}

// Seeded reports whether Seed completed for image on this repository mount.
// Publication is an atomic rename, so an existing directory is complete.
func Seeded(repositoryMount, image string) bool {
	if repositoryMount == "" || image == "" {
		return false
	}
	info, err := os.Lstat(filepath.Join(repositoryMount, SharedSubPath(image)))
	return err == nil && info.IsDir()
}

// ApplyShared has no init container or per-incident copy. Seed must have
// completed at controller startup. Build outputs remain private scratch files.
func ApplyShared(pod *corev1.PodSpec, image, pvcSubPath string) {
	if image == "" {
		return
	}
	for i := range pod.Containers {
		pod.Containers[i].VolumeMounts = append(pod.Containers[i].VolumeMounts,
			corev1.VolumeMount{Name: "repository", MountPath: "/usr/local/go", SubPath: path.Join(pvcSubPath, SharedSubPath(image), "go"), ReadOnly: true},
			corev1.VolumeMount{Name: "repository", MountPath: "/go", SubPath: path.Join(pvcSubPath, SharedSubPath(image), "cache"), ReadOnly: true})
	}
}

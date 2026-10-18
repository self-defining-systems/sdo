package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const buildMetadataPrefix = "SDO_BUILD_METADATA="

var buildDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var buildImageNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]*$`)

// KubernetesBuildBackend builds candidate source in a bounded rootless BuildKit
// Job. The Job sees only the proposal checkout, mounted read-only, and its own
// registry-push Secret. The accepted checkout and broker credentials are absent.
// Apply and Observe remain the trusted command backend's responsibility.
type KubernetesBuildBackend struct {
	CommandBackend
	Client               kubernetes.Interface
	Namespace            string
	RepositoryPVC        string
	ProposalRoot         string
	RepositoryPVCSubPath string
	BuildKitImage        string
	RegistrySecret       string
	AllowedRegistryHost  string
	RegistryInsecure     bool
	ServiceAccount       string
	PollInterval         time.Duration
	Timeout              time.Duration
	ReadJobLogs          func(context.Context, string, string) ([]byte, error)
}

func (b KubernetesBuildBackend) BuildImage(ctx context.Context, checkout string, build *contracts.ImageBuild) (string, error) {
	if b.Client == nil || b.Namespace == "" || b.RepositoryPVC == "" || b.ProposalRoot == "" || b.BuildKitImage == "" || b.RegistrySecret == "" || b.ServiceAccount == "" || build == nil || build.GetImage() == "" {
		return "", errors.New("Kubernetes image builder requires client, namespace, proposal PVC, pinned BuildKit image, registry Secret, service account, and image build")
	}
	if !digestReference.MatchString(b.BuildKitImage) {
		return "", errors.New("BuildKit Job image must be pinned by digest")
	}
	if !buildImageNamePattern.MatchString(build.GetImage()) {
		return "", errors.New("image build target contains invalid characters")
	}
	if err := ValidateRegistryHost(b.AllowedRegistryHost); err != nil {
		return "", fmt.Errorf("trusted registry host: %w", err)
	}
	actualHost, _, found := strings.Cut(build.GetImage(), "/")
	if !found || actualHost != b.AllowedRegistryHost {
		return "", fmt.Errorf("image build target %q is outside trusted registry %q", build.GetImage(), b.AllowedRegistryHost)
	}
	subPath, contextRel, dockerfileRel, err := b.proposalPaths(checkout, build)
	if err != nil {
		return "", err
	}
	initialTree, err := broker.ProposalTreeDigest(ctx, checkout)
	if err != nil {
		return "", fmt.Errorf("snapshot candidate tree before image build: %w", err)
	}
	limit := b.Timeout
	if limit <= 0 {
		limit = 10 * time.Minute
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	name := "sdo-image-build-" + rand.String(8)
	job := b.job(name, subPath, contextRel, dockerfileRel, build.GetImage(), limit)
	if _, err := b.Client.BatchV1().Jobs(b.Namespace).Create(bounded, job, metav1.CreateOptions{}); err != nil {
		return "", fmt.Errorf("create BuildKit Job: %w", err)
	}
	interval := b.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		observed, err := b.Client.BatchV1().Jobs(b.Namespace).Get(bounded, name, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("observe BuildKit Job: %w", err)
		}
		for _, condition := range observed.Status.Conditions {
			if condition.Status != corev1.ConditionTrue {
				continue
			}
			if condition.Type == batchv1.JobFailed {
				return "", fmt.Errorf("BuildKit Job %s failed: %s", name, condition.Message)
			}
			if condition.Type == batchv1.JobComplete {
				finalTree, err := broker.ProposalTreeDigest(bounded, checkout)
				if err != nil || finalTree != initialTree {
					return "", fmt.Errorf("candidate tree changed during image build: %v", err)
				}
				logs, err := b.logs(bounded, name)
				if err != nil {
					return "", fmt.Errorf("read BuildKit receipt: %w", err)
				}
				digest, err := parseBuildMetadata(logs)
				if err != nil {
					return "", err
				}
				return imageRepository(build.GetImage()) + "@" + digest, nil
			}
		}
		select {
		case <-bounded.Done():
			return "", fmt.Errorf("BuildKit Job %s did not complete: %w", name, bounded.Err())
		case <-ticker.C:
		}
	}
}

// ValidateRegistryHost accepts an explicit OCI image registry authority. A
// scheme, path, credentials, or implicit Docker Hub target is not trusted.
func ValidateRegistryHost(host string) error {
	if host == "" || strings.ContainsAny(host, "/@ \t\n") {
		return errors.New("registry host[:port] is required without scheme or path")
	}
	name, port, hasPort := strings.Cut(host, ":")
	if hasPort {
		value, err := strconv.Atoi(port)
		if err != nil || value < 1 || value > 65535 {
			return errors.New("registry port must be between 1 and 65535")
		}
	}
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return errors.New("registry host must be a lowercase DNS name or IPv4 address")
	}
	return nil
}

func (b KubernetesBuildBackend) proposalPaths(checkout string, build *contracts.ImageBuild) (string, string, string, error) {
	root, err := filepath.Abs(b.ProposalRoot)
	if err != nil {
		return "", "", "", err
	}
	candidate, err := filepath.Abs(checkout)
	if err != nil {
		return "", "", "", err
	}
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", "", "", errors.New("image build checkout must be a proposal below the configured proposal root")
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve proposal root: %w", err)
	}
	realCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", "", "", fmt.Errorf("resolve proposal checkout: %w", err)
	}
	realRelative, err := filepath.Rel(realRoot, realCandidate)
	if err != nil || realRelative != relative {
		return "", "", "", errors.New("proposal checkout resolves outside configured proposal root")
	}
	prefix := filepath.ToSlash(filepath.Clean(b.RepositoryPVCSubPath))
	if prefix == "." {
		prefix = ""
	}
	if path.IsAbs(prefix) || prefix == ".." || strings.HasPrefix(prefix, "../") || strings.Contains(prefix, "\\") {
		return "", "", "", errors.New("repository PVC subPath must be relative")
	}
	toRelative := func(name string) (string, error) {
		if name == "" {
			return "", errors.New("image build path is empty")
		}
		resolved, err := filepath.EvalSymlinks(name)
		if err != nil {
			return "", err
		}
		rel, err := filepath.Rel(realCandidate, resolved)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return "", errors.New("image build path leaves proposal checkout")
		}
		return filepath.ToSlash(rel), nil
	}
	contextRel, err := toRelative(build.GetContextPath())
	if err != nil {
		return "", "", "", err
	}
	dockerfileRel, err := toRelative(build.GetDockerfilePath())
	if err != nil {
		return "", "", "", err
	}
	if stat, err := os.Stat(build.GetContextPath()); err != nil || !stat.IsDir() {
		return "", "", "", errors.New("image build context is not a directory")
	}
	if stat, err := os.Stat(build.GetDockerfilePath()); err != nil || !stat.Mode().IsRegular() {
		return "", "", "", errors.New("Dockerfile is not a regular file")
	}
	return path.Join(prefix, filepath.ToSlash(relative)), contextRel, dockerfileRel, nil
}

func (b KubernetesBuildBackend) job(name, subPath, contextRel, dockerfileRel, image string, limit time.Duration) *batchv1.Job {
	no := false
	deadline := int64(limit.Seconds())
	if deadline < 1 {
		deadline = 1
	}
	ttl := int32(300)
	// Variables are passed as environment values, never interpolated into the script.
	script := `buildctl-daemonless.sh build --frontend dockerfile.v0 --local "context=$SDO_CONTEXT" --local dockerfile=/workspace --opt "filename=$SDO_DOCKERFILE" --output "$SDO_OUTPUT" --metadata-file /tmp/sdo-metadata.json
printf 'SDO_BUILD_METADATA='; cat /tmp/sdo-metadata.json; printf '\n'`
	output := "type=image,name=" + image + ",push=true"
	if b.RegistryInsecure {
		output += ",registry.insecure=true"
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo", "sdo.dev/component": "image-build"}},
		Spec: batchv1.JobSpec{BackoffLimit: ptr32(0), ActiveDeadlineSeconds: &deadline, TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, ServiceAccountName: b.ServiceAccount, AutomountServiceAccountToken: &no,
				SecurityContext: &corev1.PodSecurityContext{RunAsUser: ptr64(1000), RunAsGroup: ptr64(1000), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}},
				Containers: []corev1.Container{{Name: "buildkit", Image: b.BuildKitImage, Command: []string{"/bin/sh", "-ec"}, Args: []string{script},
					Env:          []corev1.EnvVar{{Name: "BUILDKITD_FLAGS", Value: "--oci-worker-no-process-sandbox"}, {Name: "DOCKER_CONFIG", Value: "/docker-config"}, {Name: "SDO_OUTPUT", Value: output}, {Name: "SDO_CONTEXT", Value: path.Join("/workspace", contextRel)}, {Name: "SDO_DOCKERFILE", Value: dockerfileRel}},
					VolumeMounts: []corev1.VolumeMount{{Name: "candidate", MountPath: "/workspace", SubPath: subPath, ReadOnly: true}, {Name: "registry-auth", MountPath: "/docker-config/config.json", SubPath: ".dockerconfigjson", ReadOnly: true}, {Name: "buildkit-state", MountPath: "/home/user/.local/share/buildkit"}},
					// RootlessKit needs newuidmap's setuid transition to create a
					// subordinate UID map. Disabling privilege escalation breaks
					// rootless BuildKit even though the container runs as UID 1000.
					SecurityContext: &corev1.SecurityContext{RunAsNonRoot: ptrBool(true), AppArmorProfile: &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}},
				}},
				Volumes: []corev1.Volume{{Name: "candidate", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: b.RepositoryPVC, ReadOnly: true}}}, {Name: "registry-auth", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: b.RegistrySecret}}}, {Name: "buildkit-state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
			}},
		},
	}
}

func (b KubernetesBuildBackend) logs(ctx context.Context, name string) ([]byte, error) {
	if b.ReadJobLogs != nil {
		return b.ReadJobLogs(ctx, b.Namespace, name)
	}
	pods, err := b.Client.CoreV1().Pods(b.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) != 1 {
		return nil, fmt.Errorf("expected one BuildKit pod for %s, found %d", name, len(pods.Items))
	}
	return b.Client.CoreV1().Pods(b.Namespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{Container: "buildkit"}).DoRaw(ctx)
}

func parseBuildMetadata(logs []byte) (string, error) {
	output := string(logs)
	if strings.Count(output, buildMetadataPrefix) != 1 {
		return "", errors.New("BuildKit did not emit metadata receipt")
	}
	receipt := output[strings.Index(output, buildMetadataPrefix)+len(buildMetadataPrefix):]
	var metadata struct {
		Digest string `json:"containerimage.digest"`
	}
	decoder := json.NewDecoder(strings.NewReader(receipt))
	if err := decoder.Decode(&metadata); err != nil {
		return "", fmt.Errorf("decode BuildKit metadata: %w", err)
	}
	if strings.TrimSpace(receipt[decoder.InputOffset():]) != "" {
		return "", errors.New("BuildKit metadata receipt has trailing data")
	}
	if !buildDigestPattern.MatchString(metadata.Digest) {
		return "", errors.New("BuildKit metadata lacks immutable sha256 image digest")
	}
	return metadata.Digest, nil
}

func ptr32(value int32) *int32 { return &value }
func ptr64(value int64) *int64 { return &value }
func ptrBool(value bool) *bool { return &value }

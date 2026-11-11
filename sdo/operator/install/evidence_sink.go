package install

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/evidencesink"
)

// EvidenceSinkMountPath is where the evidence sidecar mounts the runner's sink volume.
const EvidenceSinkMountPath = "/sdo/evidence"

const evidenceSinkVolume = "evidence-sink"

// EvidenceSink is the runner's choice of where run evidence is externalized: any Kubernetes volume the control
// namespace does not own (a hostPath, NFS, or a PersistentVolume with the Retain policy through its claim) and the
// run directory inside it. Deleting SDO's namespace never deletes the volume's data.
type EvidenceSink struct {
	RunID  string              `json:"run_id"`
	Volume corev1.VolumeSource `json:"volume"`
}

// ParseEvidenceSink decodes the compact JSON form; empty is no sink (evidence stays on the repository volume).
func ParseEvidenceSink(encoded string) (*EvidenceSink, error) {
	if encoded == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(encoded)))
	decoder.DisallowUnknownFields()
	var sink EvidenceSink
	if err := decoder.Decode(&sink); err != nil {
		return nil, fmt.Errorf("decode evidence sink: %w", err)
	}
	if err := sink.Validate(); err != nil {
		return nil, err
	}
	return &sink, nil
}

// Format is the compact JSON form the installer passes from the host to the operate Job.
func (s EvidenceSink) Format() (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	data, err := json.Marshal(s)
	return string(data), err
}

// Validate requires a run ID and exactly one durable volume source. Sources whose data lives and dies with the Pod
// or the namespace (emptyDir, ephemeral, Secret, ConfigMap, downward API, projected) are refused.
func (s EvidenceSink) Validate() error {
	if !evidencesink.ValidRunID(s.RunID) {
		return fmt.Errorf("invalid evidence run ID %q", s.RunID)
	}
	data, err := json.Marshal(s.Volume)
	if err != nil {
		return err
	}
	var sources map[string]json.RawMessage
	if err := json.Unmarshal(data, &sources); err != nil {
		return err
	}
	if len(sources) != 1 {
		return errors.New("evidence sink requires exactly one volume source")
	}
	v := s.Volume
	if v.EmptyDir != nil || v.Ephemeral != nil || v.Secret != nil || v.ConfigMap != nil || v.DownwardAPI != nil || v.Projected != nil {
		return errors.New("evidence sink volume must outlive the Pod and the control namespace")
	}
	if v.HostPath != nil && !filepath.IsAbs(v.HostPath.Path) {
		return errors.New("evidence sink host path must be absolute")
	}
	return nil
}

// evidenceSinkSidecar mirrors evidence from the repository volume (read-only) into the sink for the Pod's life. It
// is a native sidecar, so a Job still completes when its main container exits; on termination it runs one last
// pass.
func evidenceSinkSidecar(c Config, pod *corev1.PodSpec) error {
	sink, err := ParseEvidenceSink(c.EvidenceSink)
	if err != nil || sink == nil {
		return err
	}
	always := corev1.ContainerRestartPolicyAlways
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: evidenceSinkVolume, VolumeSource: sink.Volume})
	pod.InitContainers = append(pod.InitContainers, corev1.Container{
		Name: "evidence-sink", Image: c.ControllerImage, ImagePullPolicy: corev1.PullIfNotPresent,
		RestartPolicy: &always,
		Command:       []string{"/usr/local/bin/sdo-operator"},
		Args: []string{"evidence-sink", "--runtime-root", "/workspace/.sdo-runtime", "--ledger-root", "/workspace/worktrees/native-ledger",
			"--sink-root", EvidenceSinkMountPath, "--run-id", sink.RunID},
		SecurityContext: containerSecurity(),
		Resources:       limits("10m", "32Mi", "500m", "256Mi"),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "repository", MountPath: "/workspace", ReadOnly: true},
			{Name: evidenceSinkVolume, MountPath: EvidenceSinkMountPath},
		},
	})
	return nil
}

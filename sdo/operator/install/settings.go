package install

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// SettingsConfigMapName holds the fully resolved operator configuration (contracts/settings.Config).
	SettingsConfigMapName = "sdo-config"
	// SettingsDir is where processes that read policy mount it, read-only.
	SettingsDir = "/etc/sdo"
	// SettingsFileName is the document's key and file name.
	SettingsFileName = "config.yaml"
	settingsVolume   = "sdo-config"
	// SettingsHashAnnotation is the controller template annotation carrying the document's digest, so a changed
	// configuration is a changed install fingerprint and replaces the controller.
	SettingsHashAnnotation = "sdo.dev/config-hash"
)

// SettingsFile is the path of the resolved configuration inside a process that mounts it.
func SettingsFile() string { return SettingsDir + "/" + SettingsFileName }

// settingsConfigMap renders the resolved configuration; nil when the caller supplied none.
func settingsConfigMap(c Config) (*corev1.ConfigMap, error) {
	if c.Settings == nil {
		return nil, nil
	}
	document, err := c.Settings.YAML()
	if err != nil {
		return nil, fmt.Errorf("resolved config: %w", err)
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: SettingsConfigMapName, Namespace: c.controlNamespace()},
		Data:       map[string]string{SettingsFileName: string(document)},
	}, nil
}

// mountSettings adds the config volume and its read-only mount to a pod's first container.
func mountSettings(pod *corev1.PodSpec) {
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: settingsVolume, VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: SettingsConfigMapName}}}})
	pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: settingsVolume, MountPath: SettingsDir, ReadOnly: true})
}

func settingsHash(configMap *corev1.ConfigMap) string {
	digest := sha256.Sum256([]byte(configMap.Data[SettingsFileName]))
	return hex.EncodeToString(digest[:8])
}

func (i Installer) ensureSettings(ctx context.Context, p Plan) error {
	if p.Settings == nil {
		return nil
	}
	configMaps := i.Client.CoreV1().ConfigMaps(p.Settings.Namespace)
	prior, err := configMaps.Get(ctx, SettingsConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, p.Settings, metav1.CreateOptions{})
	} else if err == nil {
		want := p.Settings.DeepCopy()
		want.ResourceVersion = prior.ResourceVersion
		_, err = configMaps.Update(ctx, want, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("ensure sdo-config: %w", err)
	}
	return nil
}

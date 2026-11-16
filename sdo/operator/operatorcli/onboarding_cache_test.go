package operatorcli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sdo.dev/operator/onboardcheck"
)

func TestCheckCacheRoundTripsAndIsPerRepositoryAndNamespace(t *testing.T) {
	dir := t.TempDir()
	a, _ := checkCachePath(dir, "/r1", "ns", "clusterA")
	b, _ := checkCachePath(dir, "/r1", "other", "clusterA")
	c, _ := checkCachePath(dir, "/r2", "ns", "clusterA")
	d, _ := checkCachePath(dir, "/r1", "ns", "clusterB")
	if a == b || a == c || a == d {
		t.Fatal("one record per repository, namespace and cluster")
	}
	cache := &onboardcheck.Cache{Detectors: map[string]onboardcheck.DetectorRecord{"d": {Key: "k", At: time.Unix(5, 0).UTC()}}}
	if err := saveCheckCache(a, cache); err != nil {
		t.Fatal(err)
	}
	if got := loadCheckCache(a); got.Detectors["d"].Key != "k" {
		t.Fatalf("round trip: %+v", got)
	}
	info, _ := os.Stat(a)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", info.Mode())
	}
}

func TestUnreadableCheckCacheIsEmptyNotAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	if got := loadCheckCache(path); len(got.Breaks)+len(got.Detectors) != 0 {
		t.Fatal("missing file is an empty cache")
	}
	_ = os.WriteFile(path, []byte("{not json"), 0o600)
	if got := loadCheckCache(path); len(got.Breaks)+len(got.Detectors) != 0 {
		t.Fatal("a corrupt file is an empty cache")
	}
}

type fakeConfig struct {
	configMaps, secrets map[string]string
	asked               []string
}

func (f *fakeConfig) ConfigMapDigest(_ context.Context, _, name string) (string, error) {
	f.asked = append(f.asked, "configmap/"+name)
	v, ok := f.configMaps[name]
	if !ok {
		return "", apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, name)
	}
	return v, nil
}

func (f *fakeConfig) SecretVersion(_ context.Context, _, name string) (string, error) {
	f.asked = append(f.asked, "secret/"+name)
	v, ok := f.secrets[name]
	if !ok {
		return "", apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	return v, nil
}

func referencingTemplate() corev1.PodTemplateSpec {
	ref := func(n string) corev1.LocalObjectReference { return corev1.LocalObjectReference{Name: n} }
	return corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		ImagePullSecrets: []corev1.LocalObjectReference{ref("pull")},
		Volumes: []corev1.Volume{
			{Name: "a", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: ref("vol-cm")}}},
			{Name: "b", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "vol-secret"}}},
			{Name: "c", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
				{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: ref("proj-cm")}},
				{Secret: &corev1.SecretProjection{LocalObjectReference: ref("proj-secret")}},
			}}}},
		},
		InitContainers: []corev1.Container{{Name: "init", EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: ref("init-cm")}}}}},
		Containers: []corev1.Container{{Name: "app",
			EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: ref("env-secret")}}},
			Env: []corev1.EnvVar{
				{Name: "X", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: ref("key-cm"), Key: "k"}}},
				{Name: "Y", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: ref("key-secret"), Key: "k"}}},
			}}},
	}}
}

func TestFingerprintFollowsReferencedConfigMapContentAndSecretVersion(t *testing.T) {
	newReader := func() *fakeConfig {
		return &fakeConfig{
			configMaps: map[string]string{"vol-cm": "1", "proj-cm": "1", "init-cm": "1", "key-cm": "1", "unrelated": "1"},
			secrets:    map[string]string{"pull": "1", "vol-secret": "1", "proj-secret": "1", "env-secret": "1", "key-secret": "1"},
		}
	}
	fp := func(r *fakeConfig) string {
		v, err := templateFingerprint(context.Background(), r, "ns", referencingTemplate())
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	base := fp(newReader())
	if fp(newReader()) != base {
		t.Fatal("stable")
	}
	for _, name := range []string{"vol-cm", "proj-cm", "init-cm", "key-cm"} {
		r := newReader()
		r.configMaps[name] = "2"
		if fp(r) == base {
			t.Fatalf("configmap %s content is in the fingerprint", name)
		}
	}
	for _, name := range []string{"pull", "vol-secret", "proj-secret", "env-secret", "key-secret"} {
		r := newReader()
		r.secrets[name] = "2"
		if fp(r) == base {
			t.Fatalf("secret %s version is in the fingerprint", name)
		}
	}
	r := newReader()
	r.configMaps["unrelated"] = "2"
	if fp(r) != base {
		t.Fatal("a ConfigMap the template does not reference is not in it")
	}
	r = newReader()
	delete(r.secrets, "env-secret")
	if fp(r) == base {
		t.Fatal("a vanished reference changes the fingerprint")
	}
}

func TestSecretsAreOnlyAskedForTheirVersion(t *testing.T) {
	// The reader interface has no method that returns a Secret's data; the
	// ConfigMap digest method is never asked about a Secret.
	r := &fakeConfig{configMaps: map[string]string{}, secrets: map[string]string{}}
	if _, err := templateFingerprint(context.Background(), r, "ns", referencingTemplate()); err != nil {
		t.Fatal(err)
	}
	for _, a := range r.asked {
		if strings.HasPrefix(a, "configmap/") && strings.Contains(a, "secret") {
			t.Fatalf("a Secret was read as a ConfigMap: %v", r.asked)
		}
	}
}

func TestFingerprintFailsOnAReadErrorOtherThanNotFound(t *testing.T) {
	r := &errReader{}
	if _, err := templateFingerprint(context.Background(), r, "ns", referencingTemplate()); err == nil {
		t.Fatal("an unreadable reference gives no fingerprint, so no reuse")
	}
}

type errReader struct{}

func (errReader) ConfigMapDigest(context.Context, string, string) (string, error) {
	return "", errors.New("forbidden")
}
func (errReader) SecretVersion(context.Context, string, string) (string, error) {
	return "", errors.New("forbidden")
}

func TestSaltChangesWithEveryPart(t *testing.T) {
	base := checkSalt("a", "b")
	if checkSalt("a", "b") != base || checkSalt("a", "c") == base || checkSalt("ab", "") == base || checkSalt("a") == base {
		t.Fatal("each part, and where it is split, changes the salt")
	}
}

func TestSaveCheckCacheLeavesNoTemporaryFileAndSurvivesConcurrentSaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			_ = saveCheckCache(path, &onboardcheck.Cache{Detectors: map[string]onboardcheck.DetectorRecord{"d": {Key: "k"}}})
		}()
	}
	for i := 0; i < 4; i++ {
		<-done
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 || loadCheckCache(path).Detectors["d"].Key != "k" {
		t.Fatalf("one intact file, no leftovers: %v", entries)
	}
}

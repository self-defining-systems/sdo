package operatorcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/onboardcheck"
	"sdo.dev/operator/onboardcheck/kubebreak"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// checkCachePath is the file that keeps one session's verdicts: one per
// repository, namespace and cluster, outside the repository.
func checkCachePath(dir, repo, namespace, cluster string) (string, error) {
	if dir == "" {
		base, err := os.UserCacheDir()
		if err != nil {
			return "", err
		}
		dir = filepath.Join(base, "sdo", "onboarding-check")
	}
	sum := sha256.Sum256([]byte(repo + "\x00" + namespace + "\x00" + cluster))
	return filepath.Join(dir, hex.EncodeToString(sum[:8])+".json"), nil
}

// loadCheckCache reads the recorded verdicts; a missing or unreadable file is
// an empty cache, never an error: nothing is reused.
func loadCheckCache(path string) *onboardcheck.Cache {
	cache := &onboardcheck.Cache{}
	if data, err := os.ReadFile(path); err == nil {
		if json.Unmarshal(data, cache) != nil {
			cache = &onboardcheck.Cache{}
		}
	}
	return cache
}

func saveCheckCache(path string, cache *onboardcheck.Cache) error {
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".check-cache-*")
	if err != nil {
		return err
	}
	tmp := file.Name()
	_, werr := file.Write(data)
	if cerr := file.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(tmp)
		return werr
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// diagnosticsFiles reads the diagnostics of the prepared tree by repository path.
func diagnosticsFiles(tree string) (map[string]string, error) {
	files := map[string]string{}
	root := filepath.Join(tree, ".sdo", "diagnostics")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(tree, path)
		files[filepath.ToSlash(rel)] = string(data)
		return nil
	})
	if os.IsNotExist(err) {
		err = nil
	}
	return files, err
}

// configReader reads what a pod template references. A Secret is only ever
// asked for its resourceVersion: its values are never read.
type configReader interface {
	// ConfigMapDigest is a content hash of the ConfigMap's data.
	ConfigMapDigest(ctx context.Context, namespace, name string) (string, error)
	// SecretVersion is the Secret's resourceVersion.
	SecretVersion(ctx context.Context, namespace, name string) (string, error)
}

// kubeConfigReader reads through the cluster: ConfigMaps in full, Secrets as
// metadata only (the API server never sends their data).
type kubeConfigReader struct {
	kube     kubernetes.Interface
	metadata metadata.Interface
}

func newKubeConfigReader(cluster *processio.KubeCluster) (kubeConfigReader, error) {
	md, err := metadata.NewForConfig(cluster.Config())
	if err != nil {
		return kubeConfigReader{}, err
	}
	return kubeConfigReader{kube: cluster.Kube(), metadata: md}, nil
}

func (r kubeConfigReader) ConfigMapDigest(ctx context.Context, namespace, name string) (string, error) {
	cm, err := r.kube.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal([]any{cm.Data, cm.BinaryData, cm.Immutable})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (r kubeConfigReader) SecretVersion(ctx context.Context, namespace, name string) (string, error) {
	obj, err := r.metadata.Resource(corev1.SchemeGroupVersion.WithResource("secrets")).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return obj.GetResourceVersion(), nil
}

// referencedConfig lists the ConfigMaps and Secrets a pod spec names, through
// volumes (projected sources included), envFrom, env value sources and image
// pull secrets, in its containers and init containers.
func referencedConfig(spec corev1.PodSpec) (configMaps, secrets []string) {
	cms, secs := map[string]bool{}, map[string]bool{}
	for _, v := range spec.Volumes {
		if v.ConfigMap != nil {
			cms[v.ConfigMap.Name] = true
		}
		if v.Secret != nil {
			secs[v.Secret.SecretName] = true
		}
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.ConfigMap != nil {
					cms[src.ConfigMap.Name] = true
				}
				if src.Secret != nil {
					secs[src.Secret.Name] = true
				}
			}
		}
	}
	for _, p := range spec.ImagePullSecrets {
		secs[p.Name] = true
	}
	containers := append(append([]corev1.Container{}, spec.InitContainers...), spec.Containers...)
	for _, c := range containers {
		for _, e := range c.EnvFrom {
			if e.ConfigMapRef != nil {
				cms[e.ConfigMapRef.Name] = true
			}
			if e.SecretRef != nil {
				secs[e.SecretRef.Name] = true
			}
		}
		for _, e := range c.Env {
			if e.ValueFrom == nil {
				continue
			}
			if e.ValueFrom.ConfigMapKeyRef != nil {
				cms[e.ValueFrom.ConfigMapKeyRef.Name] = true
			}
			if e.ValueFrom.SecretKeyRef != nil {
				secs[e.ValueFrom.SecretKeyRef.Name] = true
			}
		}
	}
	return sortedKeys(cms), sortedKeys(secs)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// templateFingerprint hashes a workload's pod template with what it
// references: a ConfigMap's content, a Secret's resourceVersion. A reference
// that does not exist is part of the fingerprint ("absent"); any other read
// failure means there is none.
func templateFingerprint(ctx context.Context, reader configReader, namespace string, template corev1.PodTemplateSpec) (string, error) {
	raw, err := json.Marshal(template)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	fmt.Fprintf(h, "template\x00%s\x00", raw)
	configMaps, secrets := referencedConfig(template.Spec)
	for _, name := range configMaps {
		v, err := reader.ConfigMapDigest(ctx, namespace, name)
		if apierrors.IsNotFound(err) {
			v, err = "absent", nil
		}
		if err != nil {
			return "", fmt.Errorf("configmap %s: %w", name, err)
		}
		fmt.Fprintf(h, "configmap\x00%s\x00%s\x00", name, v)
	}
	for _, name := range secrets {
		v, err := reader.SecretVersion(ctx, namespace, name)
		if apierrors.IsNotFound(err) {
			v, err = "absent", nil
		}
		if err != nil {
			return "", fmt.Errorf("secret %s: %w", name, err)
		}
		fmt.Fprintf(h, "secret\x00%s\x00%s\x00", name, v)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// networkFingerprint hashes the namespace's NetworkPolicies, which change what
// reaches what, leaving out the policies the check's own network-mode break
// creates (they exist only while a break runs).
func networkFingerprint(ctx context.Context, cluster *processio.KubeCluster, namespace string) (string, error) {
	list, err := cluster.Kube().NetworkingV1().NetworkPolicies(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}
	type entry struct {
		Name string
		Spec any
	}
	var entries []entry
	for _, p := range list.Items {
		if kubebreak.IsBreakPolicy(p.Name) {
			continue
		}
		entries = append(entries, entry{p.Name, p.Spec})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	raw, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// deployFingerprints fingerprints each component's workload: its pod template
// (what runs, not how many replicas, so a break and its restore leave it
// unchanged) and the ConfigMaps and Secrets the template references. A
// component whose workload or references cannot be read has none and is never
// reused.
func deployFingerprints(ctx context.Context, cluster *processio.KubeCluster, namespace string, m appmap.Map) map[string]string {
	out := map[string]string{}
	reader, err := newKubeConfigReader(cluster)
	if err != nil {
		return out
	}
	network, err := networkFingerprint(ctx, cluster, namespace)
	if err != nil {
		return out
	}
	apps := cluster.Kube().AppsV1()
	for _, c := range m.Components {
		kind, name, ok := strings.Cut(c.Workload, "/")
		if !ok {
			continue
		}
		var template corev1.PodTemplateSpec
		switch kind {
		case "Deployment":
			var d *appsv1.Deployment
			if d, err = apps.Deployments(namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
				template = d.Spec.Template
			}
		case "StatefulSet":
			var d *appsv1.StatefulSet
			if d, err = apps.StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
				template = d.Spec.Template
			}
		case "DaemonSet":
			var d *appsv1.DaemonSet
			if d, err = apps.DaemonSets(namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
				template = d.Spec.Template
			}
		default:
			continue
		}
		if err != nil {
			continue
		}
		fp, err := templateFingerprint(ctx, reader, namespace, template)
		if err != nil {
			continue
		}
		// The component's Services (their ports and selectors decide what
		// reaches it) and the namespace's NetworkPolicies are part of what is
		// deployed. Image tags are not resolved to digests.
		h := sha256.New()
		fmt.Fprintf(h, "%s\x00%s\x00", fp, network)
		services := append([]string(nil), c.Services...)
		sort.Strings(services)
		failed := false
		for _, name := range services {
			svc, err := cluster.Kube().CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				failed = true
				break
			}
			raw, _ := json.Marshal([]any{svc.Spec.Ports, svc.Spec.Selector, svc.Spec.Type})
			fmt.Fprintf(h, "service\x00%s\x00%s\x00", name, raw)
		}
		if !failed {
			out[c.Name] = hex.EncodeToString(h.Sum(nil))
		}
	}
	return out
}

// checkSalt is what a verdict also depends on besides the memory and the
// deployment: the build of the check (its executable, its revision and whether
// the tree was modified), the operator source the detectors compile against,
// the cluster, and the flags that set how and how long the check judges.
func checkSalt(parts ...string) string {
	h := sha256.New()
	rev, modified := "dev", ""
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				modified = s.Value
			}
		}
	}
	exe := ""
	if path, err := os.Executable(); err == nil {
		exe = fileDigest(path)
	}
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00", rev, modified, exe)
	for _, p := range parts {
		fmt.Fprintf(h, "%d\x00%s\x00", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// sourceIdentity says which operator source a root holds: its commit and a
// hash of any uncommitted change, so an edited tree is a different source.
func sourceIdentity(ctx context.Context, root string) string {
	head := processio.HeadCommit(ctx, root)
	diff, _ := processio.Git(ctx, "-C", root, "diff", "HEAD")
	status, _ := processio.Git(ctx, "-C", root, "status", "--porcelain", "--untracked-files=all")
	sum := sha256.Sum256([]byte(diff + "\x00" + status))
	return head + ":" + hex.EncodeToString(sum[:8])
}

// clusterIdentity names the cluster and namespace the check ran against: the
// API server, the kube-system namespace's UID (one per cluster) and the
// application namespace's UID (a recreated namespace is a different one). An
// unreadable UID is part of the identity, never a reason to share one.
func clusterIdentity(ctx context.Context, cluster *processio.KubeCluster, namespace string) string {
	uid := func(ns string) string {
		obj, err := cluster.Kube().CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if err != nil {
			return "unknown:" + err.Error()
		}
		return string(obj.UID)
	}
	return cluster.Config().Host + "|" + uid("kube-system") + "|" + uid(namespace)
}

// fileDigest is the content hash of a file, or its error text: an unreadable
// file must still change the salt.
func fileDigest(path string) string {
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "unreadable:" + err.Error()
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

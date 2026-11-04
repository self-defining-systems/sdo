//go:build integration

// Package envtestio starts a real kube-apiserver and etcd (controller-runtime
// envtest) for integration tests, at the Kubernetes version the kind lanes run.
// It is a test-only edge: it launches processes, reads the environment and
// downloads the binaries once. Nothing outside tests may import it.
//
//	env := envtestio.Start(t)
//	env.Client()      // client-go client as cluster-admin
//	env.Kubeconfig    // file for a controller binary or kubectl
//	env.CreateNamespace(t, "app")
//
// Binaries come from, in order: $KUBEBUILDER_ASSETS; the cache directory
// ($SDO_ENVTEST_CACHE, else the user cache dir's sdo-envtest); a download into
// that cache with setup-envtest. A host with a fast disk should export
// SDO_ENVTEST_CACHE (on the lane hosts: /mnt/data/shli/cache/envtest).
package envtestio

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	// KubernetesVersion tracks the kindest/node image the real-cluster lanes use.
	KubernetesVersion = "1.34.0"
	// setupEnvtest is the pinned downloader; it is the only network access.
	setupEnvtest = "sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.25.2"
)

var (
	assetsOnce sync.Once
	assetsDir  string
	assetsErr  error
)

// CacheDir is where downloaded binaries live.
func CacheDir() string {
	if dir := os.Getenv("SDO_ENVTEST_CACHE"); dir != "" {
		return dir
	}
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "sdo-envtest")
}

// Assets returns the directory holding kube-apiserver and etcd, downloading
// them into CacheDir on first use. The result is the same for every caller in
// the process.
func Assets() (string, error) {
	assetsOnce.Do(func() { assetsDir, assetsErr = resolveAssets() })
	return assetsDir, assetsErr
}

func resolveAssets() (string, error) {
	if dir := os.Getenv("KUBEBUILDER_ASSETS"); dir != "" {
		return dir, nil
	}
	cache := CacheDir()
	cached := filepath.Join(cache, "bin", "k8s", fmt.Sprintf("%s-%s-%s", KubernetesVersion, runtime.GOOS, runtime.GOARCH))
	if _, err := os.Stat(filepath.Join(cached, "kube-apiserver")); err == nil {
		return cached, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "run", setupEnvtest, "use", KubernetesVersion, "--bin-dir", filepath.Join(cache, "bin"), "-p", "path")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("download envtest assets with %s: %w: %s (run scripts/install_envtest.sh, or set KUBEBUILDER_ASSETS to an existing directory, to skip the download)", setupEnvtest, err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

// Env is a running control plane.
type Env struct {
	// Config is a cluster-admin client configuration.
	Config *rest.Config
	// Kubeconfig is the path of a kubeconfig file for the same identity, for a
	// controller binary or kubectl. It lives in the test's temporary directory.
	Kubeconfig string
	// Port is the secure port of the API server on 127.0.0.1.
	Port int
	// CA is the PEM certificate authority that signed the API server's serving
	// certificate. A TLS endpoint that stands in front of the API server (a
	// proxy, a fake kubelet) presents its own certificate, and a client that
	// reaches both needs this and that certificate in one trust file.
	CA  []byte
	env *envtest.Environment
}

// Options change the control plane. The zero value is Start's default: a
// certificate-authenticated cluster-admin user, RBAC, and the ServiceAccount
// admission plugin disabled (there is no controller manager to create the
// default ServiceAccount), with etcd not syncing to disk.
type Options struct {
	// AdminToken, when set, adds a static-token cluster-admin user (group
	// system:masters, which RBAC always allows) and makes Config and Kubeconfig
	// authenticate with it instead of a client certificate. A harness that
	// mounts the same token into Pods needs this. Do not switch the API server
	// to AlwaysAllow (see AuthorizationMode).
	AdminToken string
	// AuthorizationMode replaces --authorization-mode (default RBAC; "Node,RBAC" is
	// typical). Never "AlwaysAllow": it turns anonymous authentication off and
	// envtest's readiness probe is anonymous, so the control plane never becomes ready.
	AuthorizationMode string
	// DisabledAdmissionPlugins are disabled in addition to ServiceAccount, which is
	// always disabled (no controller manager creates the default ServiceAccount).
	DisabledAdmissionPlugins []string
	// APIServerFlags set or replace other kube-apiserver flags, name without the
	// leading dashes (for example "service-cluster-ip-range": "10.96.0.0/16",
	// "kubelet-preferred-address-types": "InternalIP"). Use them last: the
	// defaults above are the supported surface.
	APIServerFlags map[string]string
}

// Start launches the API server and etcd and stops them when the test ends.
// Missing binaries that cannot be downloaded fail the test, unless
// SDO_ENVTEST_OPTIONAL is set, which skips it (for suites that run without
// network access).
func Start(t testing.TB) *Env { return StartWith(t, Options{}) }

// StartWith is Start with Options.
func StartWith(t testing.TB, options Options) *Env {
	t.Helper()
	dir, err := Assets()
	if err != nil {
		if os.Getenv("SDO_ENVTEST_OPTIONAL") != "" {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	env := &envtest.Environment{BinaryAssetsDirectory: dir}
	args := env.ControlPlane.GetAPIServer().Configure()
	if options.AdminToken != "" {
		tokens := filepath.Join(t.TempDir(), "tokens.csv")
		if err := os.WriteFile(tokens, []byte(options.AdminToken+",sdo-admin,sdo-admin-uid,\"system:masters\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		args.Set("token-auth-file", tokens)
	}
	if options.AuthorizationMode != "" {
		args.Set("authorization-mode", options.AuthorizationMode)
	}
	if len(options.DisabledAdmissionPlugins) > 0 {
		args.Set("disable-admission-plugins", strings.Join(append([]string{"ServiceAccount"}, options.DisabledAdmissionPlugins...), ","))
	}
	for name, value := range options.APIServerFlags {
		args.Set(name, value)
	}
	if _, err := env.Start(); err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	host, err := url.Parse(env.Config.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(host.Port())
	if err != nil {
		t.Fatalf("envtest host %q has no port: %v", env.Config.Host, err)
	}
	var raw []byte
	var admin *rest.Config
	if options.AdminToken != "" {
		admin = rest.CopyConfig(env.Config)
		admin.CertData, admin.KeyData, admin.BearerToken = nil, nil, options.AdminToken
		raw, err = clientcmd.Write(tokenKubeconfig(admin))
		if err != nil {
			t.Fatal(err)
		}
	} else {
		user, err := env.AddUser(envtest.User{Name: "sdo-admin", Groups: []string{"system:masters"}}, nil)
		if err != nil {
			t.Fatalf("add envtest admin: %v", err)
		}
		if raw, err = user.KubeConfig(); err != nil {
			t.Fatalf("envtest kubeconfig: %v", err)
		}
		if admin, err = clientcmd.RESTConfigFromKubeConfig(raw); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return &Env{Config: admin, Kubeconfig: path, Port: port, CA: admin.CAData, env: env}
}

// tokenKubeconfig is the kubeconfig for a bearer-token identity.
func tokenKubeconfig(cfg *rest.Config) clientcmdapi.Config {
	out := clientcmdapi.NewConfig()
	out.Clusters["envtest"] = &clientcmdapi.Cluster{Server: cfg.Host, CertificateAuthorityData: cfg.CAData}
	out.AuthInfos["admin"] = &clientcmdapi.AuthInfo{Token: cfg.BearerToken}
	out.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "admin"}
	out.CurrentContext = "envtest"
	return *out
}

// Client returns a typed client-go client for the control plane.
func (e *Env) Client(t testing.TB) kubernetes.Interface {
	t.Helper()
	client, err := kubernetes.NewForConfig(e.Config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// CreateNamespace creates a namespace. The server has no controller manager, so
// it never reports Terminating work: deleting a namespace leaves it Terminating.
func (e *Env) CreateNamespace(t testing.TB, name string) {
	t.Helper()
	_, err := e.Client(t).CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
}

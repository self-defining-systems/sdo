//go:build integration && controller_e2e

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Cluster is a real kube-apiserver and etcd on local ports. There are no nodes
// and no controllers: the fake kubelet supplies them.
type Cluster struct {
	Dir        string
	Port       int
	Token      string
	CAPath     string
	Kubeconfig string // direct to the API server
	Config     *rest.Config
	Client     kubernetes.Interface
	certPEM    []byte
	keyPEM     []byte
}

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// assets locates the envtest binaries the same way controller-runtime does.
func assets(t testing.TB) string {
	t.Helper()
	dir := os.Getenv("KUBEBUILDER_ASSETS")
	if dir == "" {
		matches, _ := filepath.Glob("/mnt/data/shli/cache/envtest/bin/k8s/*")
		if len(matches) > 0 {
			dir = matches[len(matches)-1]
		}
	}
	for _, name := range []string{"etcd", "kube-apiserver"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("envtest binary %s missing (set KUBEBUILDER_ASSETS, see setup-envtest): %v", name, err)
		}
	}
	return dir
}

func writeCerts(dir string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "sdo-e2e"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA: true, BasicConstraintsValid: true,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost", "kubernetes", "kubernetes.default", "kubernetes.default.svc"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	for name, content := range map[string][]byte{"ca.crt": certPEM, "tls.crt": certPEM, "tls.key": keyPEM} {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			return nil, nil, err
		}
	}
	return certPEM, keyPEM, nil
}

func startProcess(t testing.TB, logPath string, name string, args ...string) *exec.Cmd {
	t.Helper()
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = logFile.Close()
	})
	return cmd
}

// StartCluster launches etcd and kube-apiserver and waits until the API is ready.
func StartCluster(t testing.TB, dir string) *Cluster {
	t.Helper()
	bin := assets(t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	certPEM, keyPEM, err := writeCerts(dir)
	if err != nil {
		t.Fatal(err)
	}
	saKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	saPath := filepath.Join(dir, "sa.key")
	if err := os.WriteFile(saPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(saKey)}), 0o600); err != nil {
		t.Fatal(err)
	}
	token := "sdo-e2e-admin-token"
	tokenPath := filepath.Join(dir, "tokens.csv")
	if err := os.WriteFile(tokenPath, []byte(token+",admin,admin-uid,\"system:masters\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	etcdClient, etcdPeer, port := freePort(t), freePort(t), freePort(t)
	etcdURL := fmt.Sprintf("http://127.0.0.1:%d", etcdClient)
	startProcess(t, filepath.Join(dir, "etcd.log"), filepath.Join(bin, "etcd"),
		"--data-dir", filepath.Join(dir, "etcd"), "--listen-client-urls", etcdURL, "--advertise-client-urls", etcdURL,
		"--listen-peer-urls", fmt.Sprintf("http://127.0.0.1:%d", etcdPeer), "--unsafe-no-fsync=true")
	startProcess(t, filepath.Join(dir, "kube-apiserver.log"), filepath.Join(bin, "kube-apiserver"),
		"--etcd-servers", etcdURL, "--secure-port", fmt.Sprint(port), "--bind-address", "127.0.0.1",
		"--tls-cert-file", filepath.Join(dir, "tls.crt"), "--tls-private-key-file", filepath.Join(dir, "tls.key"),
		"--cert-dir", filepath.Join(dir, "apiserver-certs"), "--token-auth-file", tokenPath,
		"--authorization-mode=AlwaysAllow", "--disable-admission-plugins=ServiceAccount",
		"--service-account-key-file", saPath, "--service-account-signing-key-file", saPath,
		"--service-account-issuer", "https://localhost", "--service-cluster-ip-range", "10.96.0.0/16",
		"--allow-privileged=true", "--kubelet-preferred-address-types=InternalIP")
	config := &rest.Config{Host: fmt.Sprintf("https://127.0.0.1:%d", port), BearerToken: token, TLSClientConfig: rest.TLSClientConfig{CAFile: filepath.Join(dir, "ca.crt")}, QPS: 200, Burst: 400, WarningHandler: rest.NoWarnings{}}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	cluster := &Cluster{Dir: dir, Port: port, Token: token, CAPath: filepath.Join(dir, "ca.crt"), Config: config, Client: client, certPEM: certPEM, keyPEM: keyPEM}
	cluster.Kubeconfig = cluster.WriteKubeconfig(filepath.Join(dir, "kubeconfig"), port)
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		body, err := client.Discovery().RESTClient().Get().AbsPath("/readyz").DoRaw(ctx)
		cancel()
		if err == nil && string(body) == "ok" {
			return cluster
		}
		if time.Now().After(deadline) {
			t.Fatalf("kube-apiserver not ready: %v %s (see %s)", err, body, filepath.Join(dir, "kube-apiserver.log"))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// WriteKubeconfig writes an admin kubeconfig for the given local port.
func (c *Cluster) WriteKubeconfig(path string, port int) string {
	content := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: e2e
  cluster:
    server: https://127.0.0.1:%d
    certificate-authority: %s
users:
- name: admin
  user:
    token: %s
contexts:
- name: e2e
  context: {cluster: e2e, user: admin}
current-context: e2e
`, port, c.CAPath, c.Token)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		panic(err)
	}
	return path
}

var _ = http.StatusOK

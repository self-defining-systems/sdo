//go:build integration && controller_e2e

package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/runtime/envtestio"
)

// Cluster is a real kube-apiserver and etcd (envtest, no nodes, no controllers: the fake
// kubelet supplies them) plus the serving certificate the fault proxy and the fake kubelet present.
type Cluster struct {
	Dir        string
	Port       int
	Token      string // static cluster-admin token the Pods mount as their service account token
	CAPath     string // trust file for the API server and for the proxy and kubelet certificate
	Kubeconfig string // direct to the API server
	Config     *rest.Config
	Client     kubernetes.Interface
}

const adminToken = "sdo-e2e-admin-token"

func freePort(t testing.TB) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
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
	for name, content := range map[string][]byte{"tls.crt": certPEM, "tls.key": keyPEM} {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			return nil, nil, err
		}
	}
	return certPEM, keyPEM, nil
}

// StartCluster starts the control plane through the shared envtest helper and writes the
// proxy and kubelet certificate (tls.crt, tls.key) and the trust file (ca.crt) into dir. A
// client reaches the API server directly or through the proxy, so ca.crt holds the API
// server's certificate authority and the proxy's certificate.
func StartCluster(t testing.TB, dir string) *Cluster {
	t.Helper()
	env := envtestio.StartWith(t, envtestio.Options{
		AdminToken: adminToken,
		APIServerFlags: map[string]string{
			"service-cluster-ip-range":        "10.96.0.0/16",
			"kubelet-preferred-address-types": "InternalIP",
		},
	})
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	certPEM, _, err := writeCerts(dir)
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, append(append([]byte{}, env.CA...), certPEM...), 0o600); err != nil {
		t.Fatal(err)
	}
	config := rest.CopyConfig(env.Config)
	config.QPS, config.Burst, config.WarningHandler = 200, 400, rest.NoWarnings{}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	return &Cluster{Dir: dir, Port: env.Port, Token: adminToken, CAPath: caPath, Kubeconfig: env.Kubeconfig, Config: config, Client: client}
}

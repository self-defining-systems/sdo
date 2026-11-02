package deployment

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHealthVerdictParsesStdoutSeparatelyFromKubernetesWarnings(t *testing.T) {
	status, err := decodeHealthVerdict([]byte(`{"status":"clear"}`), []byte("W1018 00:00:00 client warning"))
	if err != nil || status != "clear" {
		t.Fatalf("warning contaminated detector verdict: status=%q err=%v", status, err)
	}
	_, err = decodeHealthVerdict([]byte("not JSON"), []byte(strings.Repeat("warning", 2000)))
	if err == nil || !strings.Contains(err.Error(), "not JSON") || len(err.Error()) > 5000 {
		t.Fatalf("invalid verdict omitted useful bounded diagnostics: %v", err)
	}
}

type unavailableClusterResolver struct{}

func (unavailableClusterResolver) LookupHost(context.Context, string) ([]string, error) {
	return nil, &net.DNSError{Name: "kubernetes.default.svc.cluster.local", Err: "no such host", IsNotFound: true}
}
func TestHostHealthProbeUnavailableDNSIsTransportError(t *testing.T) {
	err := (AcceptedDetectorProbe{Resolver: unavailableClusterResolver{}}).checkTransport(context.Background())
	if err == nil || !strings.Contains(err.Error(), "probe transport unavailable") || !strings.Contains(err.Error(), "no such host") {
		t.Fatalf("host DNS failure must remain a transport error: %v", err)
	}
}

func TestAcceptedHealthProbeRejectsUnavailableHostTransportBeforeBuild(t *testing.T) {
	root := candidateRepository(t)
	fixture := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo")
	if err := os.CopyFS(filepath.Join(root, ".sdo"), os.DirFS(fixture)); err != nil {
		t.Fatal(err)
	}
	gitDeployment(t, root, "add", "-A")
	gitDeployment(t, root, "commit", "-qm", "accepted detectors")
	operator, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	// A missing kubeconfig also keeps the regression hermetic if transport
	// preflight is accidentally removed and the generated evaluator is launched.
	probe := AcceptedDetectorProbe{Resolver: unavailableClusterResolver{}, Limit: 3 * time.Second, Kubeconfig: filepath.Join(t.TempDir(), "absent")}
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	status, err := probe.Evaluate(context.Background(), root, operator, "app")
	if status != "" || err == nil || !strings.Contains(err.Error(), "probe transport unavailable") {
		t.Fatalf("host DNS must not produce health status: %q %v", status, err)
	}
}

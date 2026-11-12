package hostio

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"golang.org/x/crypto/ssh"
	"k8s.io/client-go/kubernetes/fake"
	utilexec "k8s.io/client-go/util/exec"
	"sdo.dev/operator/sdk"
)

const identityMap = `version: 1
components:
  - {name: api, workload: Deployment/api, services: [api], holds_data: false}
edges: []
operations: []
probes: []
procedures:
  - id: probe-user
    component: api
    container: admin
    command: [appctl, users, ensure, "--name={{username}}", --password-stdin]
    params:
      - {name: username, type: string, required: true}
    effect: data
    probe_identity: true
    source: docs/admin.md
`

// recordingExec records each exec and blocks until release is closed.
type recordingExec struct {
	mu      sync.Mutex
	calls   []execCall
	release chan struct{}
	exit    int
}

type execCall struct {
	pod       string
	container string
	argv      []string
	stdin     string
}

func (e *recordingExec) Stream(_ context.Context, pod sdk.ObjectRef, container string, command []string, stdin string, _ io.Writer) error {
	e.mu.Lock()
	e.calls = append(e.calls, execCall{pod: pod.Name, container: container, argv: command, stdin: stdin})
	e.mu.Unlock()
	if e.release != nil {
		<-e.release
	}
	if e.exit != 0 {
		return utilexec.CodeExitError{Err: errors.New("exit"), Code: e.exit}
	}
	return nil
}

func identityCluster() *fake.Clientset {
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}
	pod := func(name string, ready bool) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Labels: map[string]string{"app": "api"}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "admin", Ready: ready}}}}
	}
	return fake.NewSimpleClientset(
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Selector: selector}},
		pod("api-a", false), pod("api-b", true),
	)
}

func waitAttempt(t *testing.T, users *ProbeUsers, user string) {
	t.Helper()
	users.mu.Lock()
	state := users.users[user]
	users.mu.Unlock()
	<-state.done
}

// The first need of a user starts one background run of the identity
// procedure in a ready Pod, with the derived password on stdin and never in
// argv; until it succeeds the user is pending, and concurrent needs (a
// continuous probe and a verify burst) share the one run.
func TestProbeUsersProvisionOnceThroughTheIdentityProcedure(t *testing.T) {
	exec := &recordingExec{release: make(chan struct{})}
	seedPath := filepath.Join(t.TempDir(), "probe-identity", "seed")
	users := NewProbeUsers(identityMap, seedPath, "app", "sdo-system", identityCluster(), exec)
	for range 3 {
		if _, err := users.Password(context.Background(), "sdoprobe-writer"); err == nil || !strings.Contains(err.Error(), "pending") {
			t.Fatalf("a user is pending until the procedure succeeds: %v", err)
		}
	}
	close(exec.release)
	waitAttempt(t, users, "sdoprobe-writer")
	password, err := users.Password(context.Background(), "sdoprobe-writer")
	if err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("procedure ran %d times, want once", len(exec.calls))
	}
	call := exec.calls[0]
	if call.pod != "api-b" || call.container != "admin" || strings.Join(call.argv, " ") != "appctl users ensure --name=sdoprobe-writer --password-stdin" {
		t.Fatalf("call = %+v", call)
	}
	if call.stdin != password+"\n" || strings.Contains(strings.Join(call.argv, " "), password) {
		t.Fatalf("the password travels on stdin only: %+v", call)
	}
	// Another memory host (evaluate-once, a restart) derives the same
	// password from the same seed, so it never rotates it under this one.
	other := NewProbeUsers(identityMap, seedPath, "app", "sdo-system", identityCluster(), &recordingExec{})
	if _, err := other.Password(context.Background(), "sdoprobe-writer"); err == nil {
		t.Fatal("the other host must provision before it answers")
	}
	waitAttempt(t, other, "sdoprobe-writer")
	if again, err := other.Password(context.Background(), "sdoprobe-writer"); err != nil || again != password {
		t.Fatalf("other host password differs: %v", err)
	}
	if second, _ := users.Password(context.Background(), "sdoprobe-reader"); second == password {
		t.Fatal("two users share a password")
	}
}

// A failed run makes the user unavailable, naming the exit, and is retried
// only after ProbeUserRetry.
func TestProbeUsersRetryAFailedRunAfterTheBackoff(t *testing.T) {
	now := time.Unix(1000, 0)
	exec := &recordingExec{exit: 3}
	users := NewProbeUsers(identityMap, filepath.Join(t.TempDir(), "seed"), "app", "", identityCluster(), exec)
	users.Now = func() time.Time { return now }
	_, _ = users.Password(context.Background(), "sdoprobe-writer")
	waitAttempt(t, users, "sdoprobe-writer")
	if _, err := users.Password(context.Background(), "sdoprobe-writer"); err == nil || !strings.Contains(err.Error(), "exited 3") {
		t.Fatalf("err = %v", err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("retried within the backoff: %d runs", len(exec.calls))
	}
	now = now.Add(ProbeUserRetry)
	exec.exit = 0
	_, _ = users.Password(context.Background(), "sdoprobe-writer")
	waitAttempt(t, users, "sdoprobe-writer")
	if _, err := users.Password(context.Background(), "sdoprobe-writer"); err != nil || len(exec.calls) != 2 {
		t.Fatalf("err = %v after %d runs", err, len(exec.calls))
	}
}

func TestProbeUsersWithoutAnIdentityProcedureAreUnavailable(t *testing.T) {
	withoutIdentity := strings.Replace(identityMap, "    probe_identity: true\n", "", 1)
	for name, document := range map[string]string{"no map": "", "no procedure": withoutIdentity} {
		users := NewProbeUsers(document, filepath.Join(t.TempDir(), "seed"), "app", "", identityCluster(), &recordingExec{})
		if _, err := users.Password(context.Background(), "sdoprobe-writer"); err == nil || !strings.Contains(err.Error(), "probe_identity") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestProbeUserPasswordShape(t *testing.T) {
	seed := []byte(strings.Repeat("s", probeIdentitySeedBytes))
	password := ProbeUserPassword(seed, "sdoprobe-writer")
	var upper, lower, digit bool
	for _, r := range password {
		upper, lower, digit = upper || unicode.IsUpper(r), lower || unicode.IsLower(r), digit || unicode.IsDigit(r)
		if r > unicode.MaxASCII || !(unicode.IsLetter(r) || unicode.IsDigit(r)) {
			t.Fatalf("password has %q", r)
		}
	}
	if len(password) != ProbeUserPasswordLength || !upper || !lower || !digit {
		t.Fatalf("password %d characters, classes %v %v %v", len(password), upper, lower, digit)
	}
	if ProbeUserPassword(seed, "sdoprobe-writer") != password || ProbeUserPassword(seed, "sdoprobe-reader") == password {
		t.Fatal("derivation must be stable per user and differ across users")
	}
}

// The seed is created once and then read back; a damaged seed is an error,
// never silently replaced.
func TestProbeIdentitySeedIsCreatedOnce(t *testing.T) {
	path := ProbeIdentitySeedPath(t.TempDir())
	first, err := LoadProbeIdentitySeed(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadProbeIdentitySeed(path)
	if err != nil || string(first) != string(second) {
		t.Fatalf("seed changed: %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("seed must be private to SDO: %v %v", info.Mode(), err)
	}
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadProbeIdentitySeed(path); err == nil {
		t.Fatal("a damaged seed was accepted")
	}
}

// A probe user's SSH key is derived from the seed like its password: the
// same on every host, different per user, served only once the user is
// provisioned, and its private half is reachable only as a signer.
func TestProbeUserSSHKeyIsDerivedFromTheSeed(t *testing.T) {
	seedPath := filepath.Join(t.TempDir(), "probe-identity", "seed")
	users := NewProbeUsers(identityMap, seedPath, "app", "", identityCluster(), &recordingExec{})
	if _, err := users.SSHPublicKey(context.Background(), "sdoprobe-writer"); err == nil {
		t.Fatal("a key is unavailable before the user is provisioned")
	}
	if _, err := users.SSHSigner("sdoprobe-writer"); err == nil {
		t.Fatal("a signer is unavailable before the user is provisioned")
	}
	_, _ = users.Password(context.Background(), "sdoprobe-writer")
	waitAttempt(t, users, "sdoprobe-writer")
	line, err := users.SSHPublicKey(context.Background(), "sdoprobe-writer")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, "ssh-ed25519 AAAA") || strings.ContainsAny(line, "\n") || strings.Count(line, " ") != 1 {
		t.Fatalf("public key line = %q", line)
	}
	signer, err := users.SSHSigner("sdoprobe-writer")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))); got != line {
		t.Fatalf("signer key %q differs from the public line %q", got, line)
	}
	other := NewProbeUsers(identityMap, seedPath, "app", "", identityCluster(), &recordingExec{})
	_, _ = other.Password(context.Background(), "sdoprobe-writer")
	waitAttempt(t, other, "sdoprobe-writer")
	if again, err := other.SSHPublicKey(context.Background(), "sdoprobe-writer"); err != nil || again != line {
		t.Fatalf("another host derives %q (%v), want %q", again, err, line)
	}
	seed := []byte(strings.Repeat("s", probeIdentitySeedBytes))
	writer, reader := ProbeUserSSHKey(seed, "sdoprobe-writer"), ProbeUserSSHKey(seed, "sdoprobe-reader")
	if writer.Equal(reader) || !writer.Equal(ProbeUserSSHKey(seed, "sdoprobe-writer")) {
		t.Fatal("derivation must be stable per user and differ across users")
	}
	if strings.Contains(line, ProbeUserPassword(seed, "sdoprobe-writer")) {
		t.Fatal("the key line must not carry the password")
	}
}

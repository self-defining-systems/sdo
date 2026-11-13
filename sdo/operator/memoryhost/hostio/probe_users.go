package hostio

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/traffic"
)

// ProbeUserRetry is the least time between two provisioning attempts of one
// probe user after a failure.
const ProbeUserRetry = 30 * time.Second

// ProbeUserPasswordLength is the length of a generated password: letters and
// digits only, so URL, form and JSON encoding leave it unchanged and the
// prober's redaction sees exactly what was sent.
const ProbeUserPasswordLength = 40

const probeIdentitySeedBytes = 32

// ProbeIdentitySeedEnv names the memory host's environment variable that
// holds the seed path; an environment variable, unlike a flag, is ignored by
// an older host binary.
const ProbeIdentitySeedEnv = "SDO_PROBE_IDENTITY_SEED"

// ProbeIdentitySeedPath is where the controller keeps the probe-identity seed
// on its repository volume: outside the published .sdo/ tree, and surviving a
// controller restart.
func ProbeIdentitySeedPath(repositoryMount string) string {
	return filepath.Join(repositoryMount, ".sdo-runtime", "probe-identity", "seed")
}

// LoadProbeIdentitySeed returns the seed at path, creating it with fresh
// random bytes when absent. Creation is exclusive, so concurrent memory hosts
// end up with one seed.
func LoadProbeIdentitySeed(path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("no probe-identity seed path is configured")
	}
	if seed, err := os.ReadFile(path); err == nil {
		return checkSeed(path, seed)
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read probe-identity seed: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create probe-identity seed directory: %w", err)
	}
	seed := make([]byte, probeIdentitySeedBytes)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".seed-")
	if err != nil {
		return nil, fmt.Errorf("write probe-identity seed: %w", err)
	}
	defer os.Remove(temporary.Name())
	if _, err = temporary.Write(seed); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, fmt.Errorf("write probe-identity seed: %w", err)
	}
	if err := os.Link(temporary.Name(), path); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("publish probe-identity seed: %w", err)
	}
	stored, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read probe-identity seed: %w", err)
	}
	return checkSeed(path, stored)
}

func checkSeed(path string, seed []byte) ([]byte, error) {
	if len(seed) != probeIdentitySeedBytes {
		return nil, fmt.Errorf("probe-identity seed %s holds %d bytes, want %d; delete it to start over", path, len(seed), probeIdentitySeedBytes)
	}
	return seed, nil
}

// ProbeUserPassword derives a probe user's password from the seed: every
// memory host computes the same one, so no host rotates it under another.
func ProbeUserPassword(seed []byte, user string) string {
	const (
		upper  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		lower  = "abcdefghijklmnopqrstuvwxyz"
		digits = "0123456789"
	)
	stream := keyStream{seed: seed, user: user}
	// The first three characters guarantee an upper, a lower and a digit.
	password := []byte{stream.pick(upper), stream.pick(lower), stream.pick(digits)}
	for len(password) < ProbeUserPasswordLength {
		password = append(password, stream.pick(upper+lower+digits))
	}
	return string(password)
}

// ProbeUserSSHKey derives a probe user's ed25519 key from the seed, with
// its own domain separation from the password, so every memory host signs
// with the same key and the application's registration of it stays valid.
func ProbeUserSSHKey(seed []byte, user string) ed25519.PrivateKey {
	mac := hmac.New(sha256.New, seed)
	mac.Write([]byte("sdo-probe-ssh\x00" + user + "\x00"))
	return ed25519.NewKeyFromSeed(mac.Sum(nil))
}

// keyStream draws uniformly distributed characters from HMAC-SHA256 blocks.
type keyStream struct {
	seed    []byte
	user    string
	block   []byte
	counter uint64
}

func (s *keyStream) pick(alphabet string) byte {
	limit := 256 - 256%len(alphabet)
	for {
		if len(s.block) == 0 {
			mac := hmac.New(sha256.New, s.seed)
			var counter [8]byte
			binary.BigEndian.PutUint64(counter[:], s.counter)
			s.counter++
			mac.Write([]byte("sdo-probe-user\x00" + s.user + "\x00"))
			mac.Write(counter[:])
			s.block = mac.Sum(nil)
		}
		value := int(s.block[0])
		s.block = s.block[1:]
		if value < limit {
			return alphabet[value%len(alphabet)]
		}
	}
}

// ProbeUsers is the prober's traffic.Passwords. The first time a
// scenario needs a user it runs the application map's probe-identity
// procedure once, in the background, in a ready Pod of the procedure's
// component, with the user's derived password on stdin; until that succeeds
// the user is pending. One value serves every engine of a prober, so each
// name has at most one run in flight. SDO reads no application Secret: the
// password is SDO's own, and neither it nor the command's output is logged.
type ProbeUsers struct {
	// Procedure is the probe-identity procedure; Workload is its component's
	// workload, Kind/name in Namespace.
	Procedure program.Procedure
	Workload  string
	Namespace string
	// ProtectedNamespace is the SDO control namespace, refused.
	ProtectedNamespace string
	Client             kubernetes.Interface
	Exec               kubereads.PodExec
	// Seed returns the probe-identity seed; it is called once per attempt
	// until it succeeds.
	Seed func() ([]byte, error)
	// Now is the retry clock; time.Now when nil.
	Now func() time.Time
	// Unavailable, when set, is why this host provisions no probe users.
	Unavailable string
	// ReadyDir, when set, holds one empty marker file per probe user the
	// procedure has provisioned, so the next memory host (every
	// evaluate-once starts a fresh one) recognises the user instead of
	// provisioning again. The marker name is an HMAC of the seed, the user and
	// the Pod that ran the procedure: it holds no secret, dies with the seed,
	// and a replaced Pod provisions again, which the procedure tolerates.
	ReadyDir string

	mu    sync.Mutex
	seed  []byte
	users map[string]*probeUser
}

type probeUser struct {
	ready   bool
	running bool
	failed  time.Time
	err     error
	// done is closed when the current attempt ends.
	done chan struct{}
}

// NewProbeUsers builds the memory host's probe-user source from the accepted
// application map (empty when the memory has none) and the seed path. Without
// a probe-identity procedure every probe user is unavailable, with the reason.
func NewProbeUsers(mapYAML, seedPath, namespace, protectedNamespace string, client kubernetes.Interface, exec kubereads.PodExec) *ProbeUsers {
	users := &ProbeUsers{Namespace: namespace, ProtectedNamespace: protectedNamespace, Client: client, Exec: exec,
		Seed: func() ([]byte, error) { return LoadProbeIdentitySeed(seedPath) }}
	if seedPath != "" {
		users.ReadyDir = filepath.Join(filepath.Dir(seedPath), "ready")
	}
	if strings.TrimSpace(mapYAML) == "" {
		users.Unavailable = "the accepted memory has no application map, so no probe_identity procedure provisions probe users"
		return users
	}
	parsed, err := appmap.Parse(mapYAML)
	if err != nil {
		users.Unavailable = "the accepted application map does not parse, so no probe user can be provisioned"
		return users
	}
	procedure, workload, ok := parsed.ProbeIdentity()
	if !ok {
		users.Unavailable = "the application map declares no probe_identity procedure, so no probe user can be provisioned"
		return users
	}
	users.Procedure, users.Workload = procedure, workload
	return users
}

// SSHPublicKey implements traffic.ProbeKeys: the user's authorized-keys line,
// once the user is provisioned. It starts no run; Password does.
func (p *ProbeUsers) SSHPublicKey(_ context.Context, user string) (string, error) {
	signer, err := p.SSHSigner(user)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))), nil
}

// SSHSigner signs the prober's SSH sessions as user, once the user is
// provisioned. The private key stays inside the signer.
func (p *ProbeUsers) SSHSigner(user string) (ssh.Signer, error) {
	if p.Unavailable != "" {
		return nil, errors.New(p.Unavailable)
	}
	p.mu.Lock()
	state, seed := p.users[user], p.seed
	ready := state != nil && state.ready
	p.mu.Unlock()
	if !ready || seed == nil {
		return nil, p.pending(fmt.Sprintf("probe user %s is not provisioned yet", user))
	}
	return ssh.NewSignerFromKey(ProbeUserSSHKey(seed, user))
}

// pending is the typed environment wait: a run of the procedure may take its
// whole declared timeout.
func (p *ProbeUsers) pending(reason string) error {
	return &traffic.IdentityPendingError{Reason: reason, Wait: p.Procedure.IdentityTimeout()}
}

// Password implements traffic.Passwords. It never waits for a run.
func (p *ProbeUsers) Password(_ context.Context, user string) (string, error) {
	if p.Unavailable != "" {
		return "", errors.New(p.Unavailable)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.users == nil {
		p.users = map[string]*probeUser{}
	}
	state := p.users[user]
	if state == nil {
		state = &probeUser{}
		p.users[user] = state
	}
	switch {
	case state.ready:
		return ProbeUserPassword(p.seed, user), nil
	case state.running:
		return "", p.pending(fmt.Sprintf("procedure %s is provisioning the user", p.Procedure.ID))
	case !state.failed.IsZero() && p.now().Sub(state.failed) < ProbeUserRetry:
		return "", fmt.Errorf("identity unavailable: procedure %s failed (%v); retrying %s after the failure", p.Procedure.ID, state.err, ProbeUserRetry)
	}
	state.running, state.done = true, make(chan struct{})
	go p.attempt(user, state)
	return "", p.pending(fmt.Sprintf("procedure %s started provisioning the user", p.Procedure.ID))
}

func (p *ProbeUsers) attempt(user string, state *probeUser) {
	ctx, cancel := context.WithTimeout(context.Background(), p.Procedure.IdentityTimeout())
	defer cancel()
	err := p.provision(ctx, user)
	p.mu.Lock()
	defer p.mu.Unlock()
	state.running, state.ready, state.err = false, err == nil, err
	if err != nil {
		state.failed = p.now()
	}
	close(state.done)
}

// provision runs the procedure for user once.
func (p *ProbeUsers) provision(ctx context.Context, user string) error {
	p.mu.Lock()
	seed := p.seed
	p.mu.Unlock()
	if seed == nil {
		if p.Seed == nil {
			return errors.New("no probe-identity seed source")
		}
		loaded, err := p.Seed()
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.seed, seed = loaded, loaded
		p.mu.Unlock()
	}
	argv, err := p.Procedure.Bind(map[string]any{program.ProbeIdentityParam: user})
	if err != nil {
		return err
	}
	pod, uid, err := p.readyPod(ctx)
	if err != nil {
		return err
	}
	marker := p.readyMarker(seed, user, pod.Name, uid)
	if marker != "" {
		if _, err := os.Stat(marker); err == nil {
			return nil
		}
	}
	command := program.Command{Pod: pod, Container: p.Procedure.Container, Args: argv, Stdin: ProbeUserPassword(seed, user) + "\n"}
	code, err := kubereads.RunCommand(ctx, p.Exec, p.ProtectedNamespace, command)
	if err != nil {
		return fmt.Errorf("run in Pod %s: %w", pod.Name, err)
	}
	if code != 0 {
		return fmt.Errorf("exited %d in Pod %s container %s", code, pod.Name, p.Procedure.Container)
	}
	if marker != "" {
		// Best effort: without the marker the next host provisions again.
		if os.MkdirAll(p.ReadyDir, 0o700) == nil {
			_ = os.WriteFile(marker, nil, 0o600)
		}
	}
	return nil
}

// readyMarker is the path of user's readiness marker for the Pod that ran the
// procedure; empty when markers are off.
func (p *ProbeUsers) readyMarker(seed []byte, user, pod, uid string) string {
	if p.ReadyDir == "" {
		return ""
	}
	mac := hmac.New(sha256.New, seed)
	mac.Write([]byte("sdo-probe-ready\x00" + user + "\x00" + pod + "\x00" + uid))
	return filepath.Join(p.ReadyDir, fmt.Sprintf("%x", mac.Sum(nil)))
}

// readyPod picks, by name, a running Pod of the workload whose procedure
// container is ready.
func (p *ProbeUsers) readyPod(ctx context.Context) (sdk.ObjectRef, string, error) {
	if p.Client == nil {
		return sdk.ObjectRef{}, "", errors.New("no Kubernetes client")
	}
	kind, name, ok := strings.Cut(p.Workload, "/")
	if !ok || name == "" {
		return sdk.ObjectRef{}, "", fmt.Errorf("workload %q is not Kind/name", p.Workload)
	}
	var selector *metav1.LabelSelector
	apps := p.Client.AppsV1()
	switch kind {
	case "Deployment":
		object, err := apps.Deployments(p.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return sdk.ObjectRef{}, "", err
		}
		selector = object.Spec.Selector
	case "StatefulSet":
		object, err := apps.StatefulSets(p.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return sdk.ObjectRef{}, "", err
		}
		selector = object.Spec.Selector
	case "DaemonSet":
		object, err := apps.DaemonSets(p.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return sdk.ObjectRef{}, "", err
		}
		selector = object.Spec.Selector
	default:
		return sdk.ObjectRef{}, "", fmt.Errorf("workload %s: a probe-identity procedure runs in a Deployment, StatefulSet or DaemonSet", p.Workload)
	}
	parsed, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil || parsed.Empty() {
		return sdk.ObjectRef{}, "", fmt.Errorf("workload %s has no usable selector", p.Workload)
	}
	pods, err := p.Client.CoreV1().Pods(p.Namespace).List(ctx, metav1.ListOptions{LabelSelector: parsed.String()})
	if err != nil {
		return sdk.ObjectRef{}, "", err
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	for _, pod := range pods.Items {
		if containerReady(pod, p.Procedure.Container) {
			return sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, string(pod.UID), nil
		}
	}
	return sdk.ObjectRef{}, "", fmt.Errorf("workload %s has no running Pod with container %s ready", p.Workload, p.Procedure.Container)
}

func containerReady(pod corev1.Pod, container string) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container {
			return status.Ready
		}
	}
	return false
}

func (p *ProbeUsers) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

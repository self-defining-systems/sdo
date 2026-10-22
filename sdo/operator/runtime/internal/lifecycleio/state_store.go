// Package lifecycleio owns the controller's durable storage I/O. It is internal
// to runtime; other subsystems cannot write lifecycle state through this edge.
package lifecycleio

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"strconv"
	"strings"
	"sync"
)

const (
	StateDataKey            = "runtime-state.json"
	StateRevisionAnnotation = "sdo.dev/state-revision"
)

var ErrStateConflict = errors.New("controller state conflict")

// encodeControllerState is the one persisted encoding of controller state:
// canonical ProtoJSON of the generated ControllerState message.
func encodeControllerState(state *contracts.ControllerState) ([]byte, error) {
	if state == nil {
		return nil, errors.New("controller state is required")
	}
	if err := wire.Validate(state); err != nil {
		return nil, err
	}
	payload, err := wire.Canonical(state)
	if err != nil {
		return nil, fmt.Errorf("encode controller state: %w", err)
	}
	return payload, nil
}

func decodeControllerState(payload []byte) (*contracts.ControllerState, error) {
	document := &contracts.ControllerState{}
	// Drop retired rollback-only fields at this durable read edge.
	if err := wire.UnmarshalDurable(payload, document); err != nil {
		return nil, fmt.Errorf("decode controller state: %w", err)
	}
	return document, nil
}

// errStaleResourceVersion reports an Update rejected because the ConfigMap
// changed since the store last read or wrote it. The state revision decides
// whether another writer took over or only metadata moved.
var errStaleResourceVersion = fmt.Errorf("%w: stale ConfigMap resourceVersion", ErrStateConflict)

// ConfigMapStateStore persists ControllerState in one ConfigMap. The
// sdo.dev/state-revision annotation is the controller's compare-and-swap
// token. The store caches the ConfigMap it last read or wrote, so an unchanged
// state costs no request and a changed state costs one Update carrying the
// cached resourceVersion. Only a resourceVersion conflict falls back to Get
// plus Update, and that path still rejects a stale state revision.
type ConfigMapStateStore struct {
	client    kubernetes.Interface
	namespace string
	name      string

	mu sync.Mutex
	// cached is the ConfigMap as the API server last returned it, or nil when
	// no object is known or a write's outcome is unknown.
	cached *corev1.ConfigMap
	// durableDigest is the SHA-256 of cached's serialized state.
	durableDigest [sha256.Size]byte
}

func NewConfigMapStateStore(client kubernetes.Interface, namespace string, name string) *ConfigMapStateStore {
	return &ConfigMapStateStore{client: client, namespace: namespace, name: name}
}

func (s *ConfigMapStateStore) Load(ctx context.Context) (*contracts.ControllerState, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cached = nil
	configMap, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("get controller state ConfigMap: %w", err)
	}
	payload, ok := configMap.Data[StateDataKey]
	if !ok {
		return nil, "", fmt.Errorf("controller state ConfigMap has no %s", StateDataKey)
	}
	state, err := decodeControllerState([]byte(payload))
	if err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return nil, "", fmt.Errorf("runtime-state ConfigMap %s/%s predates the ProtoJSON cutover and cannot be read (%w); delete the ConfigMap (kubectl -n %s delete configmap %s) and restart the controller so it recreates its state", s.namespace, s.name, err, s.namespace, s.name)
		}
		return nil, "", err
	}
	revision := configMap.Annotations[StateRevisionAnnotation]
	if _, err := strconv.ParseUint(revision, 10, 64); err != nil {
		return nil, "", fmt.Errorf("invalid controller state revision %q", revision)
	}
	s.remember(configMap)
	return state, revision, nil
}

// Save durably writes state if it differs from the last durable state and
// returns the new revision. A nil error means the state is durable at the
// returned revision. After an error nothing new is known to be durable, and
// the next Save re-reads the ConfigMap instead of trusting the cache.
func (s *ConfigMapStateStore) Save(ctx context.Context, state *contracts.ControllerState, expectedRevision string) (string, error) {
	if state == nil {
		return "", fmt.Errorf("controller state is required")
	}
	if err := s.archiveFailures(ctx, state.GetFailedClosures()); err != nil {
		return "", err
	}
	document := state
	if len(state.GetFailedClosures()) > 0 {
		document = proto.Clone(state).(*contracts.ControllerState)
		document.FailedClosures = nil
	}
	payload, err := encodeControllerState(document)
	if err != nil {
		return "", err
	}
	if err := checkBudget(s.name, payload, document); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil && expectedRevision != "" &&
		s.cached.Annotations[StateRevisionAnnotation] == expectedRevision {
		if sha256.Sum256(payload) == s.durableDigest {
			return expectedRevision, nil
		}
		revision, updateErr := s.update(ctx, s.cached, payload)
		if !errors.Is(updateErr, errStaleResourceVersion) {
			return revision, updateErr
		}
	}
	return s.saveFromServer(ctx, payload, expectedRevision)
}

// saveFromServer is the compare-and-swap path against a freshly read ConfigMap.
func (s *ConfigMapStateStore) saveFromServer(ctx context.Context, payload []byte, expectedRevision string) (string, error) {
	s.cached = nil
	configMaps := s.client.CoreV1().ConfigMaps(s.namespace)
	current, err := configMaps.Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if expectedRevision != "" {
			return "", ErrStateConflict
		}
		created, createErr := configMaps.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: s.name, Namespace: s.namespace,
				Annotations: map[string]string{StateRevisionAnnotation: "1"},
			},
			Data: map[string]string{StateDataKey: string(payload)},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(createErr) {
			return "", ErrStateConflict
		}
		if createErr != nil {
			return "", fmt.Errorf("create controller state ConfigMap: %w", createErr)
		}
		s.remember(created)
		return created.Annotations[StateRevisionAnnotation], nil
	}
	if err != nil {
		return "", fmt.Errorf("get controller state ConfigMap: %w", err)
	}
	s.remember(current)
	if expectedRevision == "" || current.Annotations[StateRevisionAnnotation] != expectedRevision {
		return "", ErrStateConflict
	}
	return s.update(ctx, current, payload)
}

// update replaces base's state payload, preserving its other metadata, with
// base's resourceVersion as the API server's precondition. Callers hold s.mu.
func (s *ConfigMapStateStore) update(ctx context.Context, base *corev1.ConfigMap, payload []byte) (string, error) {
	currentRevision := base.Annotations[StateRevisionAnnotation]
	revision, err := strconv.ParseUint(currentRevision, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid controller state revision %q", currentRevision)
	}
	nextRevision := strconv.FormatUint(revision+1, 10)
	updated := base.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[StateRevisionAnnotation] = nextRevision
	updated.Data = map[string]string{StateDataKey: string(payload)}
	s.cached = nil
	written, err := s.client.CoreV1().ConfigMaps(s.namespace).Update(ctx, updated, metav1.UpdateOptions{})
	if err != nil {
		if apierrors.IsConflict(err) {
			return "", errStaleResourceVersion
		}
		return "", fmt.Errorf("update controller state ConfigMap: %w", err)
	}
	if written == nil {
		written = updated
	}
	s.remember(written)
	return nextRevision, nil
}

// remember caches configMap as the durable object. Callers hold s.mu.
func (s *ConfigMapStateStore) remember(configMap *corev1.ConfigMap) {
	s.cached = configMap.DeepCopy()
	s.durableDigest = sha256.Sum256([]byte(configMap.Data[StateDataKey]))
}

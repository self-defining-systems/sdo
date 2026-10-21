package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sdo.dev/operator/contracts/wire"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// RuntimeStateVersion is the const ControllerState.version declares.
var RuntimeStateVersion = wire.StringConst(&contracts.ControllerState{}, "version")

const (
	stateDataKey            = "runtime-state.json"
	stateRevisionAnnotation = "sdo.dev/state-revision"
)

var ErrStateConflict = errors.New("controller state conflict")

type RuntimeState struct {
	ResponderToolStateJSON   []byte
	Version                  string
	SchedulerDeadlines       map[string]time.Time
	FindingStates            map[string]FindingState
	PendingBatch             BatcherState
	History                  []DetectorEvaluation
	DriftEvents              []sdk.DriftEvent
	DriftAdmission           *contracts.DriftAdmissionState
	ProgramSweepCursors      map[string]string
	OwnershipAdoption        OwnershipAdoptionStatus
	IncidentOpen             bool
	ResponderDone            bool
	IncidentRequest          *IncidentRequest
	IncidentResult           *IncidentResult
	DispatchError            string
	IncidentDetectedAt       time.Time
	IncidentDispatchedAt     time.Time
	ResponderCompletedAt     time.Time
	DetectorReviewRequired   bool
	DetectorReviewRequiredAt time.Time
	DetectorReviewReason     string
	// DetectorClearSince is the start of each detector's current clear
	// streak, so a restarted controller keeps HealthClearedAt exact.
	DetectorClearSince map[string]time.Time
	// IncidentObservedChanges is every object seen changed while the open
	// incident lasted, so a restart keeps first observations exact.
	IncidentObservedChanges    []ObservedStateChange
	DispatchState              string
	IncidentFindingKeys        []string
	PendingClosure             *IncidentClosure
	ClosureState               string
	ClosureReceipt             *ClosureReceipt
	ClosureFailure             *ClosureFailure
	LastAcknowledgedIncidentID string
	// PendingReleases are incident IDs whose prepared worktree was stranded by a
	// supersede and must still be reaped through the broker.
	PendingReleases     []string
	EvaluationIteration int
	DetectorTimeline    []DetectorTimelineEntry
	// IncidentView is published for responders and never restored.
	IncidentView *IncidentView
}

// validateRuntimeState runs the ControllerState contract's rules over state.
func validateRuntimeState(state RuntimeState) error {
	document, err := controllerStateContract(state)
	if err != nil {
		return fmt.Errorf("project controller state: %w", err)
	}
	return wire.Validate(document)
}

// encodeControllerState is the one persisted encoding of controller state:
// canonical ProtoJSON of the generated ControllerState message.
func encodeControllerState(state RuntimeState) ([]byte, error) {
	document, err := controllerStateContract(state)
	if err != nil {
		return nil, fmt.Errorf("project controller state: %w", err)
	}
	document.ResponderToolStateJson = append([]byte(nil), state.ResponderToolStateJSON...)
	payload, err := wire.Canonical(document)
	if err != nil {
		return nil, fmt.Errorf("encode controller state: %w", err)
	}
	return payload, nil
}

func decodeControllerState(payload []byte) (RuntimeState, error) {
	document := &contracts.ControllerState{}
	if err := wire.Unmarshal(payload, document); err != nil {
		return RuntimeState{}, fmt.Errorf("decode controller state: %w", err)
	}
	state := controllerStateState(document)
	state.ResponderToolStateJSON = append([]byte(nil), document.GetResponderToolStateJson()...)
	return state, nil
}

type StateStore interface {
	Load(context.Context) (RuntimeState, string, error)
	Save(context.Context, RuntimeState, string) (string, error)
}

// errStaleResourceVersion reports an Update rejected because the ConfigMap
// changed since the store last read or wrote it. The state revision decides
// whether another writer took over or only metadata moved.
var errStaleResourceVersion = fmt.Errorf("%w: stale ConfigMap resourceVersion", ErrStateConflict)

// ConfigMapStateStore persists RuntimeState in one ConfigMap. The
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

func (s *ConfigMapStateStore) Load(ctx context.Context) (RuntimeState, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cached = nil
	configMap, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return RuntimeState{}, "", nil
	}
	if err != nil {
		return RuntimeState{}, "", fmt.Errorf("get controller state ConfigMap: %w", err)
	}
	payload, ok := configMap.Data[stateDataKey]
	if !ok {
		return RuntimeState{}, "", fmt.Errorf("controller state ConfigMap has no %s", stateDataKey)
	}
	state, err := decodeControllerState([]byte(payload))
	if err != nil {
		if isPreCutoverFormat(err) {
			return RuntimeState{}, "", fmt.Errorf("runtime-state ConfigMap %s/%s predates the ProtoJSON cutover and cannot be read (%w); delete the ConfigMap (kubectl -n %s delete configmap %s) and restart the controller so it recreates its state", s.namespace, s.name, err, s.namespace, s.name)
		}
		return RuntimeState{}, "", err
	}
	revision := configMap.Annotations[stateRevisionAnnotation]
	if _, err := strconv.ParseUint(revision, 10, 64); err != nil {
		return RuntimeState{}, "", fmt.Errorf("invalid controller state revision %q", revision)
	}
	s.remember(configMap)
	return state, revision, nil
}

// Save durably writes state if it differs from the last durable state and
// returns the new revision. A nil error means the state is durable at the
// returned revision. After an error nothing new is known to be durable, and
// the next Save re-reads the ConfigMap instead of trusting the cache.
func (s *ConfigMapStateStore) Save(ctx context.Context, state RuntimeState, expectedRevision string) (string, error) {
	payload, err := encodeControllerState(state)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cached != nil && expectedRevision != "" &&
		s.cached.Annotations[stateRevisionAnnotation] == expectedRevision {
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
				Annotations: map[string]string{stateRevisionAnnotation: "1"},
			},
			Data: map[string]string{stateDataKey: string(payload)},
		}, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(createErr) {
			return "", ErrStateConflict
		}
		if createErr != nil {
			return "", fmt.Errorf("create controller state ConfigMap: %w", createErr)
		}
		s.remember(created)
		return created.Annotations[stateRevisionAnnotation], nil
	}
	if err != nil {
		return "", fmt.Errorf("get controller state ConfigMap: %w", err)
	}
	s.remember(current)
	if expectedRevision == "" || current.Annotations[stateRevisionAnnotation] != expectedRevision {
		return "", ErrStateConflict
	}
	return s.update(ctx, current, payload)
}

// update replaces base's state payload, preserving its other metadata, with
// base's resourceVersion as the API server's precondition. Callers hold s.mu.
func (s *ConfigMapStateStore) update(ctx context.Context, base *corev1.ConfigMap, payload []byte) (string, error) {
	currentRevision := base.Annotations[stateRevisionAnnotation]
	revision, err := strconv.ParseUint(currentRevision, 10, 64)
	if err != nil {
		return "", fmt.Errorf("invalid controller state revision %q", currentRevision)
	}
	nextRevision := strconv.FormatUint(revision+1, 10)
	updated := base.DeepCopy()
	if updated.Annotations == nil {
		updated.Annotations = make(map[string]string)
	}
	updated.Annotations[stateRevisionAnnotation] = nextRevision
	updated.Data = map[string]string{stateDataKey: string(payload)}
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
	s.durableDigest = sha256.Sum256([]byte(configMap.Data[stateDataKey]))
}

func cloneIncidentRequest(request *IncidentRequest) *IncidentRequest {
	if request == nil {
		return nil
	}
	copy := *request
	// Lists stay lists when empty: the responder's and the broker's schemas
	// reject null (a cold incident surfaces no playbook and no outcome).
	copy.Findings = append(make([]sdk.Finding, 0, len(request.Findings)), request.Findings...)
	copy.DriftEvents = append([]sdk.DriftEvent(nil), request.DriftEvents...)
	copy.DetectorHistory = cloneEvaluations(request.DetectorHistory)
	copy.SurfacedPlaybooks = append(make([]SurfacedPlaybook, 0, len(request.SurfacedPlaybooks)), request.SurfacedPlaybooks...)
	copy.RelevantOutcomes = append(make([]PriorOutcomeEvidence, 0, len(request.RelevantOutcomes)), request.RelevantOutcomes...)
	copy.StateChanges = cloneStateChanges(request.StateChanges)
	if request.FollowUp != nil {
		followUp := *request.FollowUp
		copy.FollowUp = &followUp
	}
	return &copy
}

// cloneEvaluations copies detector evaluations; the list and each
// evaluation's fingerprints stay lists when empty, never null.
func cloneEvaluations(evaluations []DetectorEvaluation) []DetectorEvaluation {
	copied := make([]DetectorEvaluation, 0, len(evaluations))
	for _, evaluation := range evaluations {
		evaluation.Fingerprints = append(make([]string, 0, len(evaluation.Fingerprints)), evaluation.Fingerprints...)
		copied = append(copied, evaluation)
	}
	return copied
}

func cloneIncidentResult(result *IncidentResult) *IncidentResult {
	if result == nil {
		return nil
	}
	copy := *result
	copy.DriftEvents = driftEventsState(driftEventsContract(result.DriftEvents))
	copy.ConfirmedRootCauses = append(make([]ConfirmedRootCause, 0, len(result.ConfirmedRootCauses)), result.ConfirmedRootCauses...)
	for index := range copy.ConfirmedRootCauses {
		copy.ConfirmedRootCauses[index].Resources = append(
			make([]sdk.ObjectRef, 0, len(result.ConfirmedRootCauses[index].Resources)),
			result.ConfirmedRootCauses[index].Resources...,
		)
	}
	copy.AppliedPlaybooks = append(make([]AppliedPlaybook, 0, len(result.AppliedPlaybooks)), result.AppliedPlaybooks...)
	for index := range copy.AppliedPlaybooks {
		copy.AppliedPlaybooks[index].ScriptNames = append(
			make([]string, 0, len(result.AppliedPlaybooks[index].ScriptNames)), result.AppliedPlaybooks[index].ScriptNames...,
		)
		copy.AppliedPlaybooks[index].ParameterBindings = make(map[string]sdk.ObjectRef)
		for name, resource := range result.AppliedPlaybooks[index].ParameterBindings {
			copy.AppliedPlaybooks[index].ParameterBindings[name] = resource
		}
	}
	copy.RepairChanges = append(make([]string, 0, len(result.RepairChanges)), result.RepairChanges...)
	copy.RepairActions = append(
		make([]RepairActionReceipt, 0, len(result.RepairActions)), result.RepairActions...,
	)
	copy.FinalDetectorStates = cloneEvaluations(result.FinalDetectorStates)
	copy.ProposedArtifactIDs = append(
		make([]string, 0, len(result.ProposedArtifactIDs)), result.ProposedArtifactIDs...,
	)
	copy.VerificationEvidence = append(
		make([]VerificationEvidence, 0, len(result.VerificationEvidence)), result.VerificationEvidence...,
	)
	return &copy
}

func cloneIncidentClosure(closure *IncidentClosure) *IncidentClosure {
	if closure == nil {
		return nil
	}
	copy := *closure
	copy.Request = *cloneIncidentRequest(&closure.Request)
	copy.Result = cloneIncidentResult(closure.Result)
	copy.FinalDetectorStates = cloneEvaluations(closure.FinalDetectorStates)
	copy.IncidentDetectorStates = cloneEvaluations(closure.IncidentDetectorStates)
	copy.DetectorTimeline = cloneTimeline(closure.DetectorTimeline)
	copy.FinalStateChanges = cloneStateChanges(closure.FinalStateChanges)
	if closure.ObservedStateChanges != nil {
		copy.ObservedStateChanges = append(
			make([]ObservedStateChange, 0, len(closure.ObservedStateChanges)), closure.ObservedStateChanges...,
		)
	}
	if closure.HealthClearedAt != nil {
		clearedAt := *closure.HealthClearedAt
		copy.HealthClearedAt = &clearedAt
	}
	if closure.DetectorReviewRequiredAt != nil {
		reviewAt := *closure.DetectorReviewRequiredAt
		copy.DetectorReviewRequiredAt = &reviewAt
	}
	return &copy
}

// cloneStateChanges deep-copies a StateChanges so a published or persisted
// snapshot cannot be mutated through a shared slice. Changes stays a list
// when empty: the broker's and the responder's schemas reject null.
func cloneStateChanges(changes *StateChanges) *StateChanges {
	if changes == nil {
		return nil
	}
	copy := *changes
	copy.Changes = append(make([]StateChange, 0, len(changes.Changes)), changes.Changes...)
	copy.UnobservedKinds = append([]string(nil), changes.UnobservedKinds...)
	return &copy
}

func cloneClosureReceipt(receipt *ClosureReceipt) *ClosureReceipt {
	if receipt == nil {
		return nil
	}
	copy := *receipt
	return &copy
}

func cloneClearSince(since map[string]time.Time) map[string]time.Time {
	if len(since) == 0 {
		return nil
	}
	copy := make(map[string]time.Time, len(since))
	for id, at := range since {
		copy[id] = at
	}
	return copy
}

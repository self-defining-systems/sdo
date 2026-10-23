package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var errMenuSafety = errors.New("menu safety predicate refused")

// ResponderMenuSafety is supplied by the trusted controller, never a worker.
// AuthorizeRead proves collection/content provenance before any fetch.
// AuthorizeObject attests exact content, images, identity and consumer closure;
// a source revision or image digest alone is insufficient. AuthorizeEffect must
// additionally prove routing closure and actual admission enforcement, including
// compensation. Missing capabilities fail closed; this adds no grants.
type ResponderMenuSafety struct {
	Digest          string
	AuthorizeRead   func(context.Context, sdk.WatchKind) error
	AuthorizeObject func(context.Context, sdk.ObjectRef, *unstructured.Unstructured) error
	AuthorizeEffect func(context.Context, sdk.ObjectRef, *unstructured.Unstructured, *unstructured.Unstructured) error
}

// ResponderMenuConfig binds menu execution to current reads and independent
// health. Dependencies must cover every diagnosis, health and affected consumer.
// Plans are controller-built SDK plans; workers cannot submit arbitrary actions.
type ResponderMenuConfig struct {
	// Direct selects the convenience menu; legacy evidence fields are unused.
	Direct bool
	// Refusal supplies a content-free reason before any collection is fetched.
	Refusal      func() string
	Reader       DirectObservationReader
	Dependencies []sdk.WatchKind
	Safety       *ResponderMenuSafety
	Healthy      func(context.Context, sdk.DetectionContext) (bool, error)
	Plans        []ResponderMenuPlan
}

type ResponderMenuPlan struct {
	ID        string
	ProgramID string
	Bindings  map[string]sdk.ObjectRef
	Plan      program.Plan
}

func (c *ResponderMenuConfig) valid() bool {
	return c != nil && c.Direct || c != nil && c.Reader != nil && len(c.Dependencies) > 0 && c.Healthy != nil && c.Safety != nil && c.Safety.Digest != "" && c.Safety.AuthorizeRead != nil && c.Safety.AuthorizeObject != nil && c.Safety.AuthorizeEffect != nil
}
func (s *ResponderMenuSafety) object(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) error {
	if object == nil || ref.Kind == "Secret" || object.GetKind() == "Secret" || s.AuthorizeObject == nil {
		return errMenuSafety
	}
	// Exact nonsecret content, consumer closure and admission require trusted
	// provenance. Effects use the executor's single image-delta predicate below.
	if s.AuthorizeObject(ctx, ref, object.DeepCopy()) != nil {
		return errMenuSafety
	}
	return nil
}

type menuSafeReader struct {
	config    *ResponderMenuConfig
	namespace string
}

func (r menuSafeReader) ListCurrent(ctx context.Context, kind sdk.WatchKind) (CurrentObjectList, error) {
	c := r.config
	if !c.valid() || kind.Namespace != r.namespace || kind.Kind == "Secret" || !causalSupportedKind(kind) || c.Safety.AuthorizeRead(ctx, kind) != nil {
		return CurrentObjectList{}, errMenuSafety
	}
	list, err := c.Reader.ListCurrent(ctx, kind)
	if err != nil || !list.Complete || list.ResourceVersion == "" {
		return CurrentObjectList{}, errCausalObservation
	}
	for i := range list.Items {
		obj := &list.Items[i]
		if obj.GetKind() == "" {
			obj.SetKind(kind.Kind)
		}
		if obj.GetAPIVersion() == "" {
			obj.SetAPIVersion(kind.APIVersion)
		}
		ref := sdk.ObjectRef{APIVersion: kind.APIVersion, Kind: kind.Kind, Namespace: kind.Namespace, Name: obj.GetName()}
		if obj.GetNamespace() != kind.Namespace || obj.GetKind() != kind.Kind || obj.GetAPIVersion() != kind.APIVersion || c.Safety.object(ctx, ref, obj) != nil {
			return CurrentObjectList{}, errMenuSafety
		}
	}
	return list, nil
}
func (l *ResponderLoop) menuSnapshot(ctx context.Context) (sdk.DetectionContext, error) {
	if !l.MenuConfig.valid() {
		return nil, errMenuSafety
	}
	if l.MenuConfig.Refusal != nil && l.MenuConfig.Refusal() != "" {
		return nil, errMenuSafety
	}
	// Preflight the full closure before reading its first collection.
	for _, kind := range l.MenuConfig.Dependencies {
		if l.MenuConfig.Safety.AuthorizeRead(ctx, kind) != nil {
			return nil, errMenuSafety
		}
	}
	snapshot := core.DetectionSnapshot{NamespaceName: l.Request.Namespace}
	listed := map[sdk.WatchKind]bool{}
	reader := menuSafeReader{l.MenuConfig, l.Request.Namespace}
	for _, kind := range l.MenuConfig.Dependencies {
		if listed[kind] {
			return nil, errMenuSafety
		}
		listed[kind] = true
		list, err := reader.ListCurrent(ctx, kind)
		if err != nil {
			return nil, err
		}
		for _, object := range list.Items {
			if causalAppendObject(&snapshot, kind.Kind, object.Object) != nil {
				return nil, errMenuSafety
			}
		}
	}
	return causalDetectionView{DetectionSnapshot: snapshot, listed: listed}, nil
}

// This edge wraps the existing executor. It guards both forward patches and
// the executor's automatic/explicit rollback; no second action engine exists.
type menuPlanClient struct {
	PlanObjectClient
	loop         *ResponderLoop
	attempt      string
	round        uint32
	phase        string
	next         int
	receipts     map[string]string
	compensation []string
}

func (c *menuPlanClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	kind := sdk.WatchKind{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}
	if ref.Namespace != c.loop.Request.Namespace || ref.Kind == "Secret" || c.loop.MenuConfig.Safety.AuthorizeRead(ctx, kind) != nil {
		return nil, errMenuSafety
	}
	object, err := c.PlanObjectClient.Get(ctx, ref)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, apierrors.NewNotFound(schema.GroupResource{Resource: ref.Kind}, ref.Name)
		}
		return nil, errMenuSafety
	}
	if c.loop.MenuConfig.Safety.object(ctx, ref, object) != nil {
		return nil, errMenuSafety
	}
	return object, nil
}
func (c *menuPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	l := c.loop
	if l.menuFence(ctx) != nil {
		return errSelectionFenced
	}
	if _, err := l.menuSnapshot(ctx); err != nil {
		return errMenuSafety
	}
	before, err := c.Get(ctx, ref)
	if err != nil {
		return err
	}
	if kind != types.MergePatchType {
		return errMenuSafety
	}
	encoded, _ := json.Marshal(before.Object)
	merged, err := jsonpatch.MergePatch(encoded, patch)
	if err != nil {
		return errMenuSafety
	}
	after := &unstructured.Unstructured{}
	if json.Unmarshal(merged, &after.Object) != nil || c.validatePostImage(ctx, before, after) != nil || l.MenuConfig.Safety.object(ctx, ref, after) != nil || l.MenuConfig.Safety.AuthorizeEffect(ctx, ref, before.DeepCopy(), after.DeepCopy()) != nil {
		return errMenuSafety
	}
	id := ""
	if c.phase == "compensation" {
		if len(c.compensation) == 0 {
			return errMenuSafety
		}
		id = c.compensation[0]
		c.compensation = c.compensation[1:]
	} else {
		id = c.receipts[string(patch)]
		if id == "" {
			c.next++
			id = menuActionID(c.attempt, c.round, c.next)
			c.receipts[string(patch)] = id
		}
	}
	receipt := menuActionReceipt(id, ref)
	receipt.ActionKind = "sdk-plan-patch"
	receipt.ChangedFields = menuChangedFields(patch)
	if err := l.selection.beginAction(ctx, c.attempt, c.round, c.phase, receipt); err != nil {
		return err
	}
	if l.menuFence(ctx) != nil {
		return errSelectionFenced
	}
	if err := c.PlanObjectClient.Patch(ctx, ref, kind, patch); err != nil {
		return errMenuSafety
	}
	live, err := c.Get(ctx, ref)
	if err != nil {
		return err
	}
	return l.selection.completeAction(ctx, c.attempt, id, c.phase == "compensation", string(live.GetUID()), live.GetResourceVersion())
}

// The plan path owns credential/identity delta semantics for every action.
// Path spellings and the structured/legacy distinction never grant an exemption.
func (c *menuPlanClient) validatePostImage(ctx context.Context, before, after *unstructured.Unstructured) error {
	if before == nil {
		before = &unstructured.Unstructured{Object: map[string]any{}}
	}
	executor := c.loop.Executor
	executor.Client = c
	return executor.validateCredentialDelta(ctx, before, after)
}

func (c *menuPlanClient) TokenlessServiceAccount(ctx context.Context, ref sdk.ObjectRef) (string, error) {
	inspector, ok := c.PlanObjectClient.(PlanIdentityInspector)
	if !ok {
		return "", errMenuSafety
	}
	return inspector.TokenlessServiceAccount(ctx, ref)
}

// Source lookup stays behind the same provenance gate as live pre-image reads.
type menuPlanSource struct {
	PlanSource
	client *menuPlanClient
}

func (s menuPlanSource) AcceptedManifest(ctx context.Context, commit string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "Secret" || ref.Namespace != s.client.loop.Request.Namespace || s.client.loop.MenuConfig.Safety.AuthorizeRead(ctx, sdk.WatchKind{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}) != nil {
		return nil, errMenuSafety
	}
	object, err := s.PlanSource.AcceptedManifest(ctx, commit, ref)
	if errors.Is(err, ErrAcceptedManifestAbsent) {
		return nil, ErrAcceptedManifestAbsent
	}
	if err != nil {
		return nil, errMenuSafety
	}
	if s.client.loop.MenuConfig.Safety.object(ctx, ref, object) != nil {
		return nil, errMenuSafety
	}
	return object, nil
}
func (s menuPlanSource) Revision(ctx context.Context, ref sdk.ObjectRef, revision string) (*unstructured.Unstructured, error) {
	if ref.Kind == "Secret" || ref.Namespace != s.client.loop.Request.Namespace || s.client.loop.MenuConfig.Safety.AuthorizeRead(ctx, sdk.WatchKind{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}) != nil {
		return nil, errMenuSafety
	}
	object, err := s.PlanSource.Revision(ctx, ref, revision)
	if err != nil {
		return nil, errMenuSafety
	}
	if s.client.loop.MenuConfig.Safety.object(ctx, ref, object) != nil {
		return nil, errMenuSafety
	}
	return object, nil
}
func menuChangedFields(patch []byte) []string {
	var values map[string]any
	if json.Unmarshal(patch, &values) != nil {
		return nil
	}
	var fields []string
	var visit func(map[string]any, string)
	visit = func(values map[string]any, prefix string) {
		for key, value := range values {
			if prefix == "" && key == "metadata" {
				continue
			}
			path := key
			if prefix != "" {
				path = prefix + "." + key
			}
			if nested, ok := value.(map[string]any); ok {
				visit(nested, path)
			} else {
				fields = append(fields, path)
			}
		}
	}
	visit(values, "")
	sort.Strings(fields)
	return fields
}

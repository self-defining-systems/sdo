package runtime

import (
	"context"
	"encoding/json"
	"errors"

	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
	return c != nil && c.Reader != nil && len(c.Dependencies) > 0 && c.Healthy != nil && c.Safety != nil && c.Safety.Digest != "" && c.Safety.AuthorizeRead != nil && c.Safety.AuthorizeObject != nil && c.Safety.AuthorizeEffect != nil
}
func (s *ResponderMenuSafety) object(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) error {
	if object == nil || ref.Kind == "Secret" || object.GetKind() == "Secret" || s.AuthorizeObject == nil {
		return errMenuSafety
	}
	// Share the executor's structural Secret predicate. Exact nonsecret content,
	// consumer closure and admission remain mandatory independent capabilities.
	if planSecretPredicate(object) != nil || s.AuthorizeObject(ctx, ref, object.DeepCopy()) != nil {
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
		return nil, errMenuSafety
	}
	if c.loop.MenuConfig.Safety.object(ctx, ref, object) != nil {
		return nil, errMenuSafety
	}
	return object, nil
}
func (c *menuPlanClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	l := c.loop
	if l.selection.authorizeMutation(ctx, l.selection.snapshot().DispatchEpoch) != nil {
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
	if json.Unmarshal(merged, &after.Object) != nil || l.MenuConfig.Safety.object(ctx, ref, after) != nil || l.MenuConfig.Safety.AuthorizeEffect(ctx, ref, before.DeepCopy(), after.DeepCopy()) != nil {
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
	if err := l.selection.beginAction(ctx, c.attempt, c.round, c.phase, receipt); err != nil {
		return err
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

func (c *menuPlanClient) TokenlessServiceAccount(ctx context.Context, ref sdk.ObjectRef) (string, error) {
	inspector, ok := c.PlanObjectClient.(PlanIdentityInspector)
	if !ok {
		return "", errMenuSafety
	}
	return inspector.TokenlessServiceAccount(ctx, ref)
}

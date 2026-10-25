package replay

import (
	"context"
	"encoding/json"
	"fmt"
	jsonpatch "github.com/evanphx/json-patch"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
)

func (c *cluster) KubernetesRequest(ctx context.Context, q program.KubernetesRequest) (json.RawMessage, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	args, _ := json.Marshal([]any{q})
	if q.Method == "GET" || q.Method == "HEAD" {
		if ref, ok := c.ordinaryAPIRef(q); ok && len(q.Query) == 0 {
			object, err := c.Get(ctx, ref)
			if err != nil {
				return nil, err
			}
			if q.Method == "HEAD" {
				return json.RawMessage("null"), nil
			}
			return json.Marshal(object.Object)
		}
		for _, call := range c.externalCalls {
			if call.Method == "KubernetesRead" && call.Error == "" && sameRecordedJSON(call.Arguments, args) {
				return append(json.RawMessage(nil), call.Result...), nil
			}
		}
		return nil, fmt.Errorf("Kubernetes read was not recorded")
	}
	for i, call := range c.externalCalls {
		if call.Method != "KubernetesEffect" || c.apiCallsUsed[i] || !sameRecordedJSON(call.Arguments, args) {
			continue
		}
		var effect program.KubernetesEffect
		if call.Error != "" || utiljson.Unmarshal(call.Result, &effect) != nil || !effect.BeforeKnown || !effect.AfterKnown {
			return nil, fmt.Errorf("raw API effect lacks measured images")
		}
		request, _ := json.Marshal([]any{effect.Request})
		if !sameRecordedJSON(args, request) {
			return nil, fmt.Errorf("API effect identity differs from recording")
		}
		live, err := c.Get(ctx, q.Ref)
		if effect.Before == nil {
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("API effect absence precondition differs")
			}
		} else {
			if err != nil {
				return nil, err
			}
			recorded := &unstructured.Unstructured{Object: effect.Before}
			if uid := recorded.GetUID(); uid != "" && uid != live.GetUID() {
				return nil, fmt.Errorf("API effect UID precondition differs")
			}
			if version := recorded.GetResourceVersion(); version != "" && version != live.GetResourceVersion() {
				return nil, fmt.Errorf("API effect version precondition differs")
			}
			if err = validateAPIRequestPreconditions(q, live); err != nil {
				return nil, err
			}
			left, e1 := comparable(live)
			right, e2 := comparable(&unstructured.Unstructured{Object: effect.Before})
			if e1 != nil || e2 != nil || string(left) != string(right) {
				return nil, fmt.Errorf("API effect pre-image differs")
			}
		}
		switch {
		case effect.After == nil && live != nil:
			err = c.Delete(ctx, q.Ref, live.GetUID(), live.GetResourceVersion())
		case effect.After != nil && effect.Before == nil:
			_, err = c.Create(ctx, q.Ref, &unstructured.Unstructured{Object: effect.After})
		case effect.After != nil:
			raw, _ := json.Marshal(effect.After)
			// A full JSON document via merge patch does not remove omitted keys. Use
			// the same diff operation as the production object executor instead.
			before, _ := json.Marshal(live.Object)
			patch, patchErr := jsonpatch.CreateMergePatch(before, raw)
			if patchErr != nil {
				return nil, patchErr
			}
			err = c.Patch(ctx, q.Ref, types.MergePatchType, patch)
		}
		if err != nil {
			return nil, err
		}
		if c.apiCallsUsed == nil {
			c.apiCallsUsed = map[int]bool{}
		}
		c.apiCallsUsed[i] = true
		return append(json.RawMessage(nil), effect.Response...), nil
	}
	return nil, fmt.Errorf("raw Kubernetes operation has no exact recorded effect")
}

func (c *cluster) ordinaryAPIRef(q program.KubernetesRequest) (sdk.ObjectRef, bool) {
	parts := strings.Split(strings.TrimPrefix(q.Path, "/"), "/")
	if len(parts) < 4 {
		return sdk.ObjectRef{}, false
	}
	group, version, index := "", parts[1], 2
	if parts[0] == "apis" {
		if len(parts) < 5 {
			return sdk.ObjectRef{}, false
		}
		group, version, index = parts[1], parts[2], 3
	}
	namespace := ""
	if len(parts) > index+2 && parts[index] == "namespaces" {
		namespace = parts[index+1]
		index += 2
	}
	if len(parts) != index+2 {
		return sdk.ObjectRef{}, false
	}
	resource := schema.GroupVersionResource{Group: group, Version: version, Resource: parts[index]}
	if q.Ref.Name != "" && q.Ref.Namespace == namespace && q.Ref.Name == parts[index+1] && fixtureResource(q.Ref) == resource {
		return q.Ref, true
	}
	for ref := range c.initial {
		if ref.Namespace == namespace && ref.Name == parts[index+1] && fixtureResource(ref) == resource {
			return ref, true
		}
	}
	return sdk.ObjectRef{}, false
}
func (v *readView) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	if v.external != nil {
		return v.external.KubernetesRequest(v.ctx, q)
	}
	return v.ReplayReadView.KubernetesRead(q)
}

// Recorded success does not authorize bypassing conditions in the request body.
func validateAPIRequestPreconditions(q program.KubernetesRequest, live *unstructured.Unstructured) error {
	if len(q.Body) == 0 {
		return nil
	}
	if q.ContentType == string(types.JSONPatchType) {
		patch, err := jsonpatch.DecodePatch(q.Body)
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(live.Object)
		_, err = patch.Apply(raw)
		return err
	}
	var body map[string]any
	if err := utiljson.Unmarshal(q.Body, &body); err != nil {
		return nil
	}
	check := func(value any) error {
		pre, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		if uid, ok := pre["uid"]; ok && uid != string(live.GetUID()) {
			return fmt.Errorf("request UID precondition differs")
		}
		if version, ok := pre["resourceVersion"]; ok && version != live.GetResourceVersion() {
			return fmt.Errorf("request version precondition differs")
		}
		return nil
	}
	if err := check(body["preconditions"]); err != nil {
		return err
	}
	if options, ok := body["deleteOptions"].(map[string]any); ok {
		if err := check(options["preconditions"]); err != nil {
			return err
		}
	}
	if q.Method == "PUT" || q.Method == "PATCH" {
		if err := check(body["metadata"]); err != nil {
			return err
		}
	}
	return nil
}

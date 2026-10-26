package operatorcli

import (
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"net/url"
	"strings"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func secretCapturePath(path string) bool {
	parts := strings.Split(strings.Split(path, "#")[0], "/")
	i := 3
	if len(parts) > 1 && parts[1] == "apis" {
		i = 4
	}
	if len(parts) > i+1 && parts[i] == "namespaces" {
		i += 2
	}
	return len(parts) > i && parts[i] == "secrets"
}
func scriptMutation(e apihistory.Event) bool {
	switch e.Subresource {
	case "exec", "attach", "portforward":
		return true
	}
	switch e.Verb {
	case "get", "head", "options", "list", "watch":
		return false
	}
	return true
}
func scriptWriteKey(path string, e apihistory.Event) string { return path + "#request=" + e.RequestID }
func (r *scriptRecorder) beginWrite(path string, e apihistory.Event) {
	if secretCapturePath(path) || e.Subresource == "exec" || e.Subresource == "attach" || e.Subresource == "portforward" {
		return
	}
	key := scriptWriteKey(path, e)
	if e.Name == "" {
		if e.Verb == "post" {
			r.pending[key] = []program.ScriptObjectCapture{{BeforeAbsent: true}}
		} else if e.Verb == "delete" {
			r.beginCollectionWrite(path, e)
		} else {
			r.gap("collection write preimages unavailable")
		}
		return
	}
	object, err := r.read(path)
	c := program.ScriptObjectCapture{}
	if err != nil {
		r.gap("write preimage unavailable: " + path)
	} else if object == nil {
		c.BeforeAbsent = true
	} else {
		c.Ref = sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
		c.Before = object.DeepCopy()
	}
	// Keep the original aggregate image for existing script replay, and a distinct
	// immutable image for every request in the same capture store.
	if !r.written[path] {
		r.written[path] = true
		if object != nil {
			r.retain(path, object, nil)
		} else {
			copy := c
			r.objects[path] = &copy
		}
	}
	if r.persistImage != nil && (c.Before != nil || c.BeforeAbsent) {
		ref, err := r.persistImage(r.ctx, key, c)
		if err != nil {
			r.gap("private write preimage storage unavailable")
		} else {
			c.ImagesRef = ref
			c.Before = nil
		}
	}
	r.pending[key] = []program.ScriptObjectCapture{c}
}
func (r *scriptRecorder) finishWrite(path string, e apihistory.Event, raw []byte, receipt *program.ScriptAPIReceipt) {
	if secretCapturePath(path) {
		receipt.UndoUnavailable = "Secret data is never read or recorded"
		return
	}
	if e.Subresource == "exec" || e.Subresource == "attach" || e.Subresource == "portforward" {
		receipt.UndoUnavailable = "exec and in-container effects are recorded, never undone"
		return
	}
	key := scriptWriteKey(path, e)
	images := r.pending[key]
	delete(r.pending, key)
	for i := range images {
		c := &images[i]
		target := path
		if e.Name == "" && e.Verb == "delete" {
			target = path + "/" + url.PathEscape(c.Ref.Name)
		}
		imageKey := key
		if len(images) > 1 || e.Verb == "delete" && e.Name == "" {
			imageKey += "#object=" + url.PathEscape(c.Ref.Name)
		}
		if e.Name == "" && e.Verb == "post" && e.Status >= 200 && e.Status < 300 {
			object := &unstructured.Unstructured{}
			if json.Unmarshal(raw, object) == nil && object.GetName() != "" {
				c.After = object
				c.Ref = sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
			}
		}
		if e.Status >= 200 && e.Status < 300 {
			if e.Name != "" || e.Verb == "delete" {
				after, err := r.read(target)
				if err != nil {
					r.gap("write postimage unavailable: " + path)
				} else if after == nil {
					c.AfterAbsent = true
				} else {
					c.After = after
					c.Ref = sdk.ObjectRef{APIVersion: after.GetAPIVersion(), Kind: after.GetKind(), Namespace: after.GetNamespace(), Name: after.GetName()}
				}
			}
			c.Written = true
		}
		if r.persistImage != nil && (c.ImagesRef != "" || c.Before != nil || c.BeforeAbsent) {
			ref, err := r.persistImage(r.ctx, imageKey, *c)
			if err != nil {
				r.gap("private write postimage storage unavailable")
			} else {
				c.ImagesRef = ref
				c.Before = nil
				c.After = nil
			}
		}
	}
	receipt.Objects = images
	if len(images) == 0 {
		receipt.UndoUnavailable = fmt.Sprintf("%s object images unavailable", e.Verb)
	}
}

func (r *scriptRecorder) beginCollectionWrite(path string, e apihistory.Event) {
	query, _ := url.ParseQuery(e.CaptureQuery)
	query.Set("limit", "500")
	key := scriptWriteKey(path, e)
	seen := map[string]bool{}
	for {
		list, err := r.readQuery(path, query.Encode())
		if err != nil || list == nil {
			r.gap("collection write preimages unavailable")
			return
		}
		items, ok, err := unstructured.NestedSlice(list.Object, "items")
		if err != nil || !ok {
			r.gap("collection write list unavailable")
			return
		}
		for _, item := range items {
			m, ok := item.(map[string]any)
			if !ok {
				r.gap("collection write image unavailable")
				continue
			}
			object := &unstructured.Unstructured{Object: m}
			if object.GetName() == "" || strings.EqualFold(object.GetKind(), "Secret") {
				r.gap("collection write object identity unavailable")
				continue
			}
			target := path + "/" + url.PathEscape(object.GetName())
			r.written[target] = true
			r.retain(target, object, nil)
			c := program.ScriptObjectCapture{Ref: sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}, Before: object}
			if r.persistImage != nil {
				ref, err := r.persistImage(r.ctx, key+"#object="+url.PathEscape(object.GetName()), c)
				if err != nil {
					r.gap("private collection preimage storage unavailable")
				} else {
					c.ImagesRef = ref
					c.Before = nil
				}
			}
			r.pending[key] = append(r.pending[key], c)
		}
		token := list.GetContinue()
		if token == "" {
			return
		}
		if seen[token] {
			r.gap("collection write pagination repeated")
			return
		}
		seen[token] = true
		query.Set("continue", token)
	}
}

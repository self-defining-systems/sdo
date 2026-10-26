package operatorcli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// scriptRecorder is a synchronous first-write edge around the existing proxy.
// It never stores request bodies or Secret data. Explicit gaps survive in receipts.
type scriptRecorder struct {
	persistReceipt func(context.Context, program.ScriptAPIReceipt) error
	persistImage   func(context.Context, string, program.ScriptObjectCapture) (string, error)
	checkpoint     func([]program.ScriptObjectCapture, []program.ScriptAPIReceipt) error
	pending        map[string][]program.ScriptObjectCapture
	written        map[string]bool
	ctx            context.Context
	upstream       *url.URL
	transport      http.RoundTripper
	mu             sync.Mutex
	objects        map[string]*program.ScriptObjectCapture
	receipts       []program.ScriptAPIReceipt
	gaps           []string
	bytes          int
}

func newScriptRecorder(ctx context.Context, u *url.URL, t http.RoundTripper) *scriptRecorder {
	return &scriptRecorder{ctx: ctx, upstream: u, transport: t, objects: map[string]*program.ScriptObjectCapture{}, written: map[string]bool{}, pending: map[string][]program.ScriptObjectCapture{}}
}
func (r *scriptRecorder) Gap() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gap("Kubernetes proxy recording incomplete")
}
func (r *scriptRecorder) gap(message string) {
	for _, old := range r.gaps {
		if old == message {
			return
		}
	}
	r.gaps = append(r.gaps, message)
}
func scriptEventPath(e apihistory.Event) string {
	if e.Resource == "" || e.Version == "" {
		return ""
	}
	path := "/api/" + e.Version
	if e.Group != "" {
		path = "/apis/" + e.Group + "/" + e.Version
	}
	if e.Namespace != "" {
		path += "/namespaces/" + url.PathEscape(e.Namespace)
	}
	path += "/" + e.Resource
	if e.Name != "" {
		path += "/" + url.PathEscape(e.Name)
	}
	return path
}
func (r *scriptRecorder) Record(e apihistory.Event, raw []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.persist()
	path := scriptEventPath(e)
	if path == "" {
		return
	}
	write := scriptMutation(e)
	if e.Phase == "start" && write && !e.DryRun {
		r.beginWrite(path, e)
	}
	if e.Phase != "end" {
		return
	}
	receipt := program.ScriptAPIReceipt{RequestID: e.RequestID, Method: e.Verb, Path: path, Subresource: e.Subresource, Status: e.Status, StartedAt: e.Started, CompletedAt: e.Finished, Omission: e.Omission}
	if write && !e.DryRun {
		r.finishWrite(path, e, raw, &receipt)
	}
	if secretCapturePath(path) {
		raw = nil
		receipt.Omission = "secret_data_not_recorded"
		receipt.UndoUnavailable = "Secret data is never read or recorded"
	}
	if r.persistReceipt != nil {
		if err := r.persistReceipt(r.ctx, receipt); err != nil {
			r.gap("private API receipt storage unavailable")
		}
	}
	if len(r.receipts) < 512 {
		r.receipts = append(r.receipts, receipt)
	} else {
		r.gap("inline API receipt inventory clipped; complete receipts remain in private capture storage")
	}
	if e.Omission != "" && e.Omission != "upgrade_metadata_only" && e.Omission != "redacted_stream_metadata_only" && e.Omission != "redacted_log_metadata_only" {
		r.gap("API response capture omitted: " + e.Omission)
	}
	if len(raw) > 0 && e.Status >= 200 && e.Status < 300 {
		object := &unstructured.Unstructured{}
		if object.UnmarshalJSON(raw) == nil && object.GetName() != "" && object.GetKind() != "Status" {
			if e.Name == "" {
				path += "/" + url.PathEscape(object.GetName())
				if write && e.Verb == "post" {
					r.written[path] = true
					if r.objects[path] == nil {
						r.objects[path] = &program.ScriptObjectCapture{BeforeAbsent: true}
					}
				}
			}
			// Collection create has no preimage. Read-only objects retain their measured image.
			r.retain(path, nil, object)
		}
	}
	if write && (e.Status >= 200 && e.Status < 300 || e.Status == 101) {
		for target, capture := range r.objects {
			if target == path || e.Verb == "delete" && e.Name == "" && strings.HasPrefix(target, path+"/") {
				capture.Written = true
			}
		}
	}

}
func (r *scriptRecorder) retain(path string, before, after *unstructured.Unstructured) {
	if secretCapturePath(path) || before != nil && strings.EqualFold(before.GetKind(), "Secret") || after != nil && strings.EqualFold(after.GetKind(), "Secret") {
		return
	}
	object := after
	if object == nil {
		object = before
	}
	if object == nil {
		return
	}
	ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
	capture := r.objects[path]
	if capture == nil {
		capture = &program.ScriptObjectCapture{Ref: ref}
	}
	old, _ := json.Marshal(capture)
	candidate := *capture
	if candidate.Ref.Kind == "" {
		candidate.Ref = ref
	}
	if candidate.Before == nil && before != nil {
		candidate.Before = before.DeepCopy()
	}
	if after != nil {
		candidate.After = after.DeepCopy()
		candidate.AfterAbsent = false
	}
	if r.persistImage != nil {
		ref, err := r.persistImage(r.ctx, path, candidate)
		if err != nil {
			r.gap("private object image storage unavailable")
		} else {
			candidate.ImagesRef = ref
			candidate.Before = nil
			candidate.After = nil
		}
	}
	next, _ := json.Marshal(candidate)
	delta := len(next) - len(old)
	if r.bytes+delta > 512<<10 {
		r.gap("object preimage/postimage capture budget exceeded")
		candidate.Before = nil
		candidate.After = nil
		r.objects[path] = &candidate
		return
	}
	r.bytes += delta
	r.objects[path] = &candidate
}
func (r *scriptRecorder) read(path string) (*unstructured.Unstructured, error) {
	return r.readQuery(path, "")
}
func (r *scriptRecorder) readQuery(path, query string) (*unstructured.Unstructured, error) {
	if secretCapturePath(path) {
		return nil, fmt.Errorf("Secret data is never read or recorded")
	}
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()
	target := *r.upstream
	target.Path = strings.TrimSuffix(target.Path, "/") + path
	target.RawQuery = query
	req, err := http.NewRequestWithContext(ctx, "GET", target.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	response, err := r.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == 404 {
		return nil, nil
	}
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("object read status %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, apihistory.MaxBodyBytes+1))
	if err != nil || len(raw) > apihistory.MaxBodyBytes {
		return nil, fmt.Errorf("object image exceeds capture bound")
	}
	object := &unstructured.Unstructured{}
	if err = object.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	return object, nil
}
func (r *scriptRecorder) settle(out *program.ScriptExecutionResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.objects))
	for key := range r.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, path := range keys {
		object, err := r.read(path)
		if err != nil {
			r.gap("settled postimage unavailable: " + path)
		} else if object != nil {
			r.retain(path, nil, object)
		} else {
			r.objects[path].After = nil
			r.objects[path].AfterAbsent = true
			if r.persistImage != nil {
				ref, err := r.persistImage(r.ctx, path, *r.objects[path])
				if err != nil {
					r.gap("settled absence storage unavailable")
				} else {
					r.objects[path].ImagesRef = ref
				}
			}
		}
		if r.objects[path].Ref.Kind != "" {
			out.Objects = append(out.Objects, *r.objects[path])
		}
	}
	out.Receipts = append(out.Receipts, r.receipts...)
	out.CaptureGaps = append(out.CaptureGaps, r.gaps...)
}

func (r *scriptRecorder) persist() {
	if r.checkpoint == nil {
		return
	}
	objects := make([]program.ScriptObjectCapture, 0, len(r.objects))
	for _, object := range r.objects {
		if object.Ref.Kind != "" {
			objects = append(objects, *object)
		}
	}
	if err := r.checkpoint(objects, r.receipts); err != nil {
		r.gap("durable script capture unavailable")
	}
}

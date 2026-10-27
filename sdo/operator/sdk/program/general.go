package program

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"sdo.dev/operator/sdk"
)

// PatchObject patches any nonsecret Kubernetes object. Type is json (RFC 6902),
// merge (RFC 7396), or strategic (Kubernetes built-in schema). Edits resolve list
// identities against the current object before producing JSON patch operations.
type PatchObject struct {
	Ref    sdk.ObjectRef
	Type   string
	Patch  json.RawMessage `json:",omitempty"`
	Edits  []ListEdit
	Reason string
}

func (PatchObject) action() {}

// ListEdit selects exactly one list element by matching all fields in Match.
// Op is remove, replace, or insert (immediately before the matched element).
// Use ordinary JSON patch's /- path to append, including to an empty list.
type ListEdit struct {
	Path  string         `json:"path"`
	Match map[string]any `json:"match"`
	Op    string         `json:"op"`
	Value any            `json:"value,omitempty"`
}

// ApplyObject creates a missing object or merges desired fields into an existing
// object. Mode create refuses an existing object; replace replaces its document.
// For server-side apply and field ownership, use KubernetesRequest with the
// application/apply-patch+yaml content type and fieldManager query parameter.
type ApplyObject struct {
	Ref    sdk.ObjectRef
	Object map[string]any
	Mode   string
	Reason string
}

func (ApplyObject) action() {}

// KubernetesRequest exposes the Kubernetes HTTP API, including subresources,
// cluster resources and discovered kinds. Query carries API options. Only the
// plan checks bound operations (SDO identities hold a cluster-wide grant, so RBAC does not). Secret endpoints are never accessed.
type KubernetesRequest struct {
	Ref         sdk.ObjectRef
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Query       map[string]string `json:"query,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Body        []byte            `json:"body,omitempty"`
	Reason      string            `json:"reason,omitempty"`
}

func (KubernetesRequest) action() {}

// WaitFor observes a JSON pointer until it equals Equals. Timeout and Interval
// bound polling; zero uses 30 seconds / 250 milliseconds respectively.
type WaitFor struct {
	Ref      sdk.ObjectRef
	Path     string
	Equals   any
	Timeout  time.Duration
	Interval time.Duration
}

func (WaitFor) action() {}

func ValidateGeneralRef(ref sdk.ObjectRef) error {
	if ref.APIVersion == "" || ref.Kind == "" || ref.Name == "" {
		return fmt.Errorf("operation requires a named versioned object")
	}
	if strings.EqualFold(ref.Kind, "Secret") {
		return fmt.Errorf("Secret data is unavailable; use credential references")
	}
	return nil
}
func (r KubernetesRequest) Validate() error {
	switch r.Method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE":
	default:
		return fmt.Errorf("invalid Kubernetes request method")
	}
	u, err := url.Parse(r.Path)
	if !strings.HasPrefix(r.Path, "/") || err != nil || u.IsAbs() || u.Host != "" || u.RawQuery != "" || u.Fragment != "" || path.Clean(r.Path) != r.Path || strings.ContainsAny(r.Path, "%\\") || strings.Contains(r.Path, "//") {
		return fmt.Errorf("Kubernetes request requires a canonical API path")
	}
	// Discovery is part of the read API, including CRD discovery.
	if r.Method == "GET" || r.Method == "HEAD" {
		if r.Path == "/api" || r.Path == "/apis" || r.Path == "/version" || r.Path == "/openapi/v2" || r.Path == "/openapi/v3" || r.Path == "/api/v1" || strings.HasPrefix(r.Path, "/apis/") && len(strings.Split(strings.TrimPrefix(r.Path, "/"), "/")) <= 3 {
			return nil
		}
	}
	parts := strings.Split(strings.TrimPrefix(r.Path, "/"), "/")
	index := 2
	if len(parts) < 3 {
		return fmt.Errorf("Kubernetes API resource path is required")
	}
	switch parts[0] {
	case "api":
		if parts[1] != "v1" {
			return fmt.Errorf("invalid core API version")
		}
	case "apis":
		index = 3
		if len(parts) < 4 {
			return fmt.Errorf("group API resource path is required")
		}
	default:
		return fmt.Errorf("Kubernetes API path is required")
	}
	if len(parts) > index && parts[index] == "watch" {
		index++
	}
	if len(parts) > index+2 && parts[index] == "namespaces" {
		index += 2
	}
	if parts[0] == "api" && len(parts) > index && strings.EqualFold(parts[index], "secrets") {
		return fmt.Errorf("Secret endpoints are unavailable; use credential references")
	}
	return nil
}
func (a PatchObject) Validate() error {
	if err := ValidateGeneralRef(a.Ref); err != nil {
		return err
	}
	if len(a.Edits) > 0 {
		if len(a.Patch) > 0 || a.Type != "" {
			return fmt.Errorf("list edits and raw patch are exclusive")
		}
		for _, e := range a.Edits {
			if !strings.HasPrefix(e.Path, "/") || len(e.Match) == 0 {
				return fmt.Errorf("list edit requires a pointer and identity match")
			}
			switch e.Op {
			case "remove", "replace", "insert":
			default:
				return fmt.Errorf("invalid list edit operation")
			}
		}
		return nil
	}
	switch a.Type {
	case "json", "merge", "strategic":
	default:
		return fmt.Errorf("patch type must be json, merge or strategic")
	}
	if !json.Valid(a.Patch) {
		return fmt.Errorf("patch must be valid JSON")
	}
	return nil
}
func (a ApplyObject) Validate() error {
	if err := ValidateGeneralRef(a.Ref); err != nil {
		return err
	}
	switch a.Mode {
	case "", "apply", "create", "replace":
	default:
		return fmt.Errorf("invalid object mode")
	}
	metadata, _ := a.Object["metadata"].(map[string]any)
	namespace, _ := metadata["namespace"].(string)
	if a.Object["apiVersion"] != a.Ref.APIVersion || a.Object["kind"] != a.Ref.Kind || metadata["name"] != a.Ref.Name || namespace != a.Ref.Namespace {
		return fmt.Errorf("desired object identity must match reference")
	}
	if _, err := json.Marshal(a.Object); err != nil {
		return fmt.Errorf("object must contain JSON values")
	}
	return nil
}
func (w WaitFor) Validate() error {
	if err := ValidateGeneralRef(w.Ref); err != nil {
		return err
	}
	if !strings.HasPrefix(w.Path, "/") || w.Timeout < 0 || w.Interval < 0 {
		return fmt.Errorf("wait requires a JSON pointer and nonnegative timing")
	}
	return nil
}

var patchScheme = func() *runtime.Scheme {
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, autoscalingv2.AddToScheme, batchv1.AddToScheme, networkingv1.AddToScheme, policyv1.AddToScheme, rbacv1.AddToScheme} {
		_ = add(s)
	}
	return s
}()

// ProjectPatch is the shared atomic projection used by production and replay.
// Errors leave the input untouched. The API server still validates its schema.
func ProjectPatch(object map[string]any, a PatchObject) (map[string]any, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	if len(a.Edits) > 0 {
		for _, edit := range a.Edits {
			var current map[string]any
			if err = utiljson.Unmarshal(raw, &current); err != nil {
				return nil, err
			}
			value, e := FieldValue(current, edit.Path)
			if e != nil {
				return nil, e
			}
			list, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("list edit pointer does not select a list")
			}
			count, index := 0, -1
			for i, item := range list {
				m, ok := item.(map[string]any)
				if !ok {
					continue
				}
				match := true
				for key, want := range edit.Match {
					v, exists := m[key]
					x, _ := json.Marshal(v)
					y, _ := json.Marshal(want)
					match = match && exists && bytes.Equal(x, y)
				}
				if match {
					count++
					index = i
				}
			}
			if count != 1 {
				return nil, fmt.Errorf("list identity must match exactly once (matched %d)", count)
			}
			op := edit.Op
			if op == "insert" {
				op = "add"
			}
			operation := map[string]any{"op": op, "path": fmt.Sprintf("%s/%d", edit.Path, index)}
			if op != "remove" {
				operation["value"] = edit.Value
			}
			patch, _ := json.Marshal([]any{operation})
			decoded, e := jsonpatch.DecodePatch(patch)
			if e != nil {
				return nil, e
			}
			raw, err = decoded.Apply(raw)
			if err != nil {
				return nil, err
			}
		}
	} else {
		switch a.Type {
		case "json":
			var p jsonpatch.Patch
			p, err = jsonpatch.DecodePatch(a.Patch)
			if err == nil {
				raw, err = p.Apply(raw)
			}
		case "merge":
			raw, err = jsonpatch.MergePatch(raw, a.Patch)
		case "strategic":
			var typed runtime.Object
			typed, err = patchScheme.New(schema.FromAPIVersionAndKind(a.Ref.APIVersion, a.Ref.Kind))
			if err == nil {
				raw, err = strategicpatch.StrategicMergePatch(raw, a.Patch, typed)
			}
		}
	}
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err = utiljson.Unmarshal(raw, &out); err != nil || out == nil {
		return nil, fmt.Errorf("patch must produce an object")
	}
	// Object identity belongs to the URL, and must not be changed by a patch.
	desired := ApplyObject{Ref: a.Ref, Object: out}
	if err = desired.Validate(); err != nil {
		return nil, err
	}
	return out, nil
}

func validateGeneralAction(action Action) error {
	switch a := action.(type) {
	case PatchObject:
		return a.Validate()
	case ApplyObject:
		return a.Validate()
	case KubernetesRequest:
		return a.Validate()
	case WaitFor:
		return a.Validate()
	case *PatchObject:
		if a != nil {
			return a.Validate()
		}
	case *ApplyObject:
		if a != nil {
			return a.Validate()
		}
	case *KubernetesRequest:
		if a != nil {
			return a.Validate()
		}
	case *WaitFor:
		if a != nil {
			return a.Validate()
		}
	default:
		return validateExternalAction(action)
	}
	return fmt.Errorf("nil general action")
}

// KubernetesEffect records a raw API mutation and measured object images. Known
// distinguishes confirmed absence from an unavailable read. Replay rejects
// missing images instead of inventing a subresource or admission effect.
type KubernetesEffect struct {
	Request     KubernetesRequest `json:"request"`
	Before      map[string]any    `json:"before,omitempty"`
	After       map[string]any    `json:"after,omitempty"`
	BeforeKnown bool              `json:"before_known"`
	AfterKnown  bool              `json:"after_known"`
	Response    json.RawMessage   `json:"response"`
}

// CredentialResponse identifies API operations whose response is a credential,
// rather than cluster evidence. Their bodies are never read by the controller.
// Use a container command when the generated value must be consumed there.
func (r KubernetesRequest) CredentialResponse() bool {
	parts := strings.Split(strings.Trim(r.Path, "/"), "/")
	return len(parts) >= 3 && parts[len(parts)-3] == "serviceaccounts" && parts[len(parts)-1] == "token"
}

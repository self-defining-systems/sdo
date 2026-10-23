package program

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var listIdentity = `(?:[0-9]+|name=[A-Za-z0-9_-]+)`
var allowedFieldPaths = []*regexp.Regexp{
	regexp.MustCompile(`^data\[[A-Za-z0-9_.-]+\]$`),
	regexp.MustCompile(`^spec\.ports\[` + listIdentity + `\]\.(port|targetPort|protocol|name)$`),
	regexp.MustCompile(`^spec\.selector(\.[A-Za-z0-9_./-]+)?$`),
	regexp.MustCompile(`^spec\.(replicas|minReplicas|maxReplicas|minAvailable|maxUnavailable)$`),
	regexp.MustCompile(`^spec\.template\.spec\.(nodeSelector|tolerations|affinity|serviceAccountName)$`),
	regexp.MustCompile(`^spec\.template\.spec\.(containers|initContainers)\[` + listIdentity + `\]\.(image|command|args|volumeMounts|ports\[` + listIdentity + `\]\.(containerPort|hostPort|protocol|name)|((liveness|readiness|startup)Probe|env\[` + listIdentity + `\]|resources)(\.[A-Za-z0-9_./-]+)*)$`),
}

// ExpandedSetField reports whether an action uses the structured vocabulary.
func ExpandedSetField(f SetField) bool {
	return f.ValueFromSource || f.DocumentFormat != "" || strings.HasPrefix(f.Path, "data[") || strings.Contains(f.Path, "[name=") || f.Path == "spec" || strings.HasSuffix(f.Path, ".command") || strings.HasSuffix(f.Path, ".args") || strings.HasSuffix(f.Path, ".volumeMounts") || strings.HasSuffix(f.Path, ".nodeSelector") || strings.HasSuffix(f.Path, ".tolerations") || strings.HasSuffix(f.Path, ".affinity") || strings.HasSuffix(f.Path, ".serviceAccountName") || f.Ref.Kind == "HorizontalPodAutoscaler" || f.Ref.Kind == "PodDisruptionBudget"
}

// ValidateSetField checks the explicit path allowlist and Kubernetes schema kind.
// An absent API version retains legacy program validation; executors require it.
func ValidateSetField(f SetField) error {
	allowed := f.Ref.Kind == "NetworkPolicy" && f.Path == "spec"
	for _, pattern := range allowedFieldPaths {
		allowed = allowed || pattern.MatchString(f.Path)
	}
	if !allowed {
		return fmt.Errorf("SetField path %q is not allowed", f.Path)
	}
	if f.ValueFromSource && f.Value != nil {
		return fmt.Errorf("SetField accepts a literal or accepted-source value, never both")
	}
	if f.DocumentFormat != "" || len(f.DocumentPath) > 0 {
		if f.Ref.Kind != "ConfigMap" || !strings.HasPrefix(f.Path, "data[") || (f.DocumentFormat != "json" && f.DocumentFormat != "yaml") || len(f.DocumentPath) == 0 {
			return fmt.Errorf("document entry requires a ConfigMap data key, json/yaml format and nonempty document path")
		}
		for _, key := range f.DocumentPath {
			if key == "" {
				return fmt.Errorf("empty document path key")
			}
		}
		if !f.ValueFromSource {
			if _, err := json.Marshal(f.Value); err != nil {
				return fmt.Errorf("invalid document value")
			}
		}
		return nil
	}
	if f.Ref.APIVersion == "" {
		if ExpandedSetField(f) {
			return fmt.Errorf("structured SetField requires an API version")
		}
		return nil
	}
	typ, err := fieldSchema(f.Ref.APIVersion, f.Ref.Kind, f.Path)
	if err != nil {
		return err
	}
	if f.ValueFromSource {
		return nil
	}
	if err := validateSchemaValue(f.Value, typ); err != nil {
		return fmt.Errorf("SetField %s: invalid schema kind: %w", f.Path, err)
	}
	return nil
}

func fieldSchema(api, kind, path string) (reflect.Type, error) {
	var root any
	switch api + "/" + kind {
	case "v1/ConfigMap":
		root = corev1.ConfigMap{}
	case "v1/Service":
		root = corev1.Service{}
	case "apps/v1/Deployment":
		root = appsv1.Deployment{}
	case "networking.k8s.io/v1/NetworkPolicy":
		root = networkingv1.NetworkPolicy{}
	case "autoscaling/v2/HorizontalPodAutoscaler":
		root = autoscalingv2.HorizontalPodAutoscaler{}
	case "policy/v1/PodDisruptionBudget":
		root = policyv1.PodDisruptionBudget{}
	default:
		return nil, fmt.Errorf("SetField kind %s/%s is unsupported", api, kind)
	}
	parts, err := parseFieldPath(path)
	if err != nil {
		return nil, err
	}
	typ := reflect.TypeOf(root)
	for _, p := range parts {
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ.Kind() == reflect.Map {
			typ = typ.Elem()
		} else {
			found := false
			if typ.Kind() == reflect.Struct {
				for i := 0; i < typ.NumField(); i++ {
					f := typ.Field(i)
					if strings.Split(f.Tag.Get("json"), ",")[0] == p.key {
						typ = f.Type
						found = true
						break
					}
				}
			}
			if !found {
				return nil, fmt.Errorf("SetField path is outside %s schema", kind)
			}
		}
		if p.selector != "" {
			for typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}
			if typ.Kind() != reflect.Slice && typ.Kind() != reflect.Map {
				return nil, fmt.Errorf("field selector requires list or map")
			}
			typ = typ.Elem()
		}
	}
	return typ, nil
}

func validateSchemaValue(value any, typ reflect.Type) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if value == nil {
		return fmt.Errorf("null is not a field value")
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("value is not JSON")
	}
	// Kubernetes scalar unions have strict custom JSON decoders.
	if typ == reflect.TypeOf(intstr.IntOrString{}) || typ == reflect.TypeOf(resource.Quantity{}) {
		return json.Unmarshal(raw, reflect.New(typ).Interface())
	}
	// Disallow unknown members recursively and reject fractional integer values.
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(reflect.New(typ).Interface()); err != nil {
		return fmt.Errorf("expected %s", typ)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return fmt.Errorf("trailing value")
	}
	return nil
}

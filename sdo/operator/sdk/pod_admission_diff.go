package sdk

import (
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// maxFieldChangeValue bounds one recorded FieldChange value.
const maxFieldChangeValue = 256

// admissionDefaults are the fields the API server fills in by default. A value
// added at one of these paths is defaulting, not a mutation; a value changed
// or removed there still is. "*" matches one path element.
var admissionDefaults = []string{
	"spec.dnsPolicy", "spec.restartPolicy", "spec.schedulerName", "spec.terminationGracePeriodSeconds",
	"spec.serviceAccountName", "spec.serviceAccount", "spec.enableServiceLinks", "spec.preemptionPolicy",
	"spec.priority", "spec.securityContext.*",
	"spec.volumes[kube-api-access-*]",
	"spec.tolerations[node.kubernetes.io/not-ready:NoExecute]",
	"spec.tolerations[node.kubernetes.io/unreachable:NoExecute]",
	"spec.*[*].terminationMessagePath", "spec.*[*].terminationMessagePolicy", "spec.*[*].imagePullPolicy",
	"spec.*[*].volumeMounts[kube-api-access-*]",
	"spec.*[*].ports[*].protocol",
	"spec.*[*].*Probe.timeoutSeconds", "spec.*[*].*Probe.periodSeconds",
	"spec.*[*].*Probe.successThreshold", "spec.*[*].*Probe.failureThreshold",
	"spec.*[*].*Probe.*.scheme",
	"spec.*[*].env[*].valueFrom.fieldRef.apiVersion",
	"spec.volumes[*].*.defaultMode", "spec.volumes[*].hostPath.type",
}

// AdmissionMutations lists the fields admission changed in admitted compared
// with submitted: labels, annotations and the Pod spec, with containers,
// volumes, environment variables, ports and tolerations matched by name, and
// resource quantities compared by value. Values the API server sets by
// default are not mutations. A container, volume or toleration admission
// added is one change. The result is sorted by path and bounded by
// MaxAdmissionMutations.
func AdmissionMutations(submitted, admitted corev1.Pod) []FieldChange {
	before := podAdmissionFields(submitted)
	after := podAdmissionFields(admitted)
	var changes []FieldChange
	diffAdmissionFields("", before, after, &changes)
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	if len(changes) > MaxAdmissionMutations {
		changes = changes[:MaxAdmissionMutations]
	}
	return changes
}

func podAdmissionFields(pod corev1.Pod) map[string]any {
	raw, _ := json.Marshal(map[string]any{"metadata": map[string]any{"labels": pod.Labels, "annotations": pod.Annotations}, "spec": pod.Spec})
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return keyAdmissionLists(out, "").(map[string]any)
}

// keyAdmissionLists turns lists of named elements into maps keyed by name, so
// a change is reported at the element that changed.
func keyAdmissionLists(value any, field string) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			out[key] = keyAdmissionLists(child, key)
		}
		return out
	case []any:
		keyed := map[string]any{}
		for _, element := range typed {
			object, ok := element.(map[string]any)
			key := admissionListKey(field, object)
			if !ok || key == "" {
				return typed
			}
			if _, duplicate := keyed["["+key+"]"]; duplicate {
				return typed
			}
			keyed["["+key+"]"] = keyAdmissionLists(object, "")
		}
		return keyed
	}
	return value
}

func admissionListKey(field string, object map[string]any) string {
	text := func(key string) string { value, _ := object[key].(string); return value }
	switch field {
	case "containers", "initContainers", "ephemeralContainers", "volumes", "env", "volumeMounts":
		if field == "volumeMounts" {
			return text("name") + "@" + text("mountPath")
		}
		return text("name")
	case "ports":
		if port, ok := object["containerPort"].(float64); ok {
			raw, _ := json.Marshal(port)
			return string(raw)
		}
	case "tolerations":
		return text("key") + ":" + text("effect")
	}
	return ""
}

func diffAdmissionFields(prefix string, before, after map[string]any, changes *[]FieldChange) {
	keys := map[string]bool{}
	for key := range before {
		keys[key] = true
	}
	for key := range after {
		keys[key] = true
	}
	for key := range keys {
		at := key
		if prefix != "" && strings.HasPrefix(key, "[") {
			at = prefix + key
		} else if prefix != "" {
			at = prefix + "." + key
		}
		old, hadOld := before[key]
		current, hasCurrent := after[key]
		oldMap, oldIsMap := old.(map[string]any)
		currentMap, currentIsMap := current.(map[string]any)
		switch {
		case oldIsMap && currentIsMap:
			diffAdmissionFields(at, oldMap, currentMap, changes)
		case !hadOld && currentIsMap && !strings.HasPrefix(key, "["):
			diffAdmissionFields(at, map[string]any{}, currentMap, changes)
		case !hadOld && hasCurrent:
			if !admissionDefault(at) && !emptyAdmissionValue(current) {
				*changes = append(*changes, FieldChange{Path: at, After: admissionValue(current)})
			}
		case hadOld && !hasCurrent:
			if !emptyAdmissionValue(old) {
				*changes = append(*changes, FieldChange{Path: at, Before: admissionValue(old)})
			}
		default:
			if !sameAdmissionValue(at, old, current) {
				*changes = append(*changes, FieldChange{Path: at, Before: admissionValue(old), After: admissionValue(current)})
			}
		}
	}
}

func sameAdmissionValue(at string, old, current any) bool {
	left, _ := json.Marshal(old)
	right, _ := json.Marshal(current)
	if string(left) == string(right) {
		return true
	}
	if !strings.Contains(at, ".resources.") && !strings.Contains(at, ".overhead.") {
		return false
	}
	a, aok := old.(string)
	b, bok := current.(string)
	if !aok || !bok {
		return false
	}
	qa, errA := resource.ParseQuantity(a)
	qb, errB := resource.ParseQuantity(b)
	return errA == nil && errB == nil && qa.Cmp(qb) == 0
}

func emptyAdmissionValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case map[string]any:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	}
	return false
}

// admissionDefaultGlobs are admissionDefaults compiled per path element; "*"
// matches any text, slashes and dots in a key included.
var admissionDefaultGlobs = func() [][]*regexp.Regexp {
	out := make([][]*regexp.Regexp, 0, len(admissionDefaults))
	for _, pattern := range admissionDefaults {
		var globs []*regexp.Regexp
		for _, element := range admissionPathElements(pattern) {
			globs = append(globs, regexp.MustCompile("^"+strings.ReplaceAll(regexp.QuoteMeta(element), `\*`, ".*")+"$"))
		}
		out = append(out, globs)
	}
	return out
}()

func admissionDefault(at string) bool {
	elements := admissionPathElements(at)
	for _, globs := range admissionDefaultGlobs {
		if len(globs) != len(elements) {
			continue
		}
		matched := true
		for index, glob := range globs {
			if !glob.MatchString(elements[index]) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// admissionPathElements splits "spec.containers[api].image" into spec,
// containers, [api] and image; a dot inside brackets does not split.
func admissionPathElements(at string) []string {
	var out []string
	var current strings.Builder
	depth := 0
	flush := func() {
		if current.Len() > 0 {
			out = append(out, current.String())
			current.Reset()
		}
	}
	for _, r := range at {
		switch {
		case r == '[' && depth == 0:
			flush()
			depth++
			current.WriteRune(r)
		case r == ']' && depth == 1:
			current.WriteRune(r)
			depth--
			flush()
		case r == '.' && depth == 0:
			flush()
		default:
			current.WriteRune(r)
		}
	}
	flush()
	return out
}

func admissionValue(value any) string {
	raw, _ := json.Marshal(value)
	if len(raw) > maxFieldChangeValue {
		return string(raw[:maxFieldChangeValue]) + "…"
	}
	return string(raw)
}

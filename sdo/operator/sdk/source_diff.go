package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// ErrNoFieldComparison means a diff holds no accepted/observed field comparison,
// such as a baseline diff or a diff read from an unavailable source.
var ErrNoFieldComparison = errors.New("diff holds no accepted/observed field comparison")

const (
	sourceComparisonAccepted = "accepted: "
	sourceComparisonObserved = "\nobserved: "
)

// SourceComparisonText renders the accepted and observed comparable fields of
// one object as SourceDiff.Text. Each side is one JSON document, or null when
// that side is absent. Sides, Field and ContainerChanges parse this form.
func SourceComparisonText(accepted, observed []byte) string {
	return sourceComparisonAccepted + string(accepted) + sourceComparisonObserved + string(observed)
}

// Sides returns the accepted-source and live comparable fields. A nil side is
// an absent object.
func (d SourceDiff) Sides() (accepted, observed map[string]any, err error) {
	rest, ok := strings.CutPrefix(d.Text, sourceComparisonAccepted)
	if !ok {
		return nil, nil, ErrNoFieldComparison
	}
	left, right, ok := strings.Cut(rest, sourceComparisonObserved)
	if !ok {
		return nil, nil, ErrNoFieldComparison
	}
	// Later lines annotate the comparison (for example fields changed since acceptance).
	right, _, _ = strings.Cut(right, "\n")
	if err := json.Unmarshal([]byte(left), &accepted); err != nil {
		return nil, nil, fmt.Errorf("accepted side: %w", err)
	}
	if err := json.Unmarshal([]byte(right), &observed); err != nil {
		return nil, nil, fmt.Errorf("observed side: %w", err)
	}
	return accepted, observed, nil
}

// FieldValues is one field on both sides of a source comparison.
type FieldValues struct {
	Accepted any
	Observed any
	// Declared is true when accepted source sets the field.
	Declared bool
	// Live is true when the live object has the field.
	Live bool
}

// Differs is true only for a field accepted source declares whose live value
// differs or is gone. A live field source does not declare is a server default
// or an addition; Differs never reports it.
func (f FieldValues) Differs() bool {
	return f.Declared && (!f.Live || !reflect.DeepEqual(f.Accepted, f.Observed))
}

// Field compares one field path, such as "spec.replicas", on both sides. The path takes the same selectors
// as the repair actions: "[name=X]" picks the one list entry with that name ("containers[name=app]"),
// "[N]" a list index and "[key]" a map entry ("data[config.yaml]"). A selector that matches nothing is an
// absent field; a malformed path is an error, never a silently absent field.
// Compare containers by name with ContainerChanges, never by list position.
func (d SourceDiff) Field(path string) (FieldValues, error) {
	accepted, observed, err := d.Sides()
	if err != nil {
		return FieldValues{}, err
	}
	steps, err := parseFieldSteps(path)
	if err != nil {
		return FieldValues{}, fmt.Errorf("field path %q: %w", path, err)
	}
	result := FieldValues{}
	result.Accepted, result.Declared = lookupField(accepted, steps)
	result.Observed, result.Live = lookupField(observed, steps)
	return result, nil
}

type fieldStep struct {
	key      string
	selector string
}

func parseFieldSteps(path string) ([]fieldStep, error) {
	var steps []fieldStep
	depth, start := 0, 0
	flush := func(end int) error {
		text := path[start:end]
		step := fieldStep{key: text}
		if open := strings.IndexByte(text, '['); open >= 0 {
			if open == 0 || !strings.HasSuffix(text, "]") || open+1 == len(text)-1 {
				return errors.New("invalid selector")
			}
			step = fieldStep{key: text[:open], selector: text[open+1 : len(text)-1]}
		}
		if step.key == "" {
			return errors.New("empty path segment")
		}
		steps = append(steps, step)
		return nil
	}
	for i := 0; i < len(path); i++ {
		switch path[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth < 0 {
				return nil, errors.New("unbalanced selector")
			}
		case '.':
			if depth == 0 {
				if err := flush(i); err != nil {
					return nil, err
				}
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, errors.New("unbalanced selector")
	}
	if err := flush(len(path)); err != nil {
		return nil, err
	}
	return steps, nil
}

func lookupField(object map[string]any, steps []fieldStep) (any, bool) {
	var value any = object
	for _, step := range steps {
		fields, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		if value, ok = fields[step.key]; !ok {
			return nil, false
		}
		if step.selector != "" {
			if value, ok = selectField(value, step.selector); !ok {
				return nil, false
			}
		}
	}
	return value, object != nil
}

func selectField(value any, selector string) (any, bool) {
	if entries, ok := value.(map[string]any); ok {
		entry, ok := entries[selector]
		return entry, ok
	}
	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	if name, named := strings.CutPrefix(selector, "name="); named {
		var found any
		count := 0
		for _, item := range items {
			if fields, ok := item.(map[string]any); ok && fields["name"] == name {
				found = item
				count++
			}
		}
		return found, count == 1
	}
	index, err := strconv.Atoi(selector)
	if err != nil || index < 0 || index >= len(items) {
		return nil, false
	}
	return items[index], true
}

// ContainerKind is the pod-spec list a container belongs to.
type ContainerKind string

const (
	AppContainer       ContainerKind = "app"
	InitContainer      ContainerKind = "init"
	EphemeralContainer ContainerKind = "ephemeral"
)

var containerLists = []struct {
	kind ContainerKind
	key  string
}{{AppContainer, "containers"}, {InitContainer, "initContainers"}, {EphemeralContainer, "ephemeralContainers"}}

// ContainerChange is one pod-template container that differs from accepted
// source, matched by kind and name.
type ContainerChange struct {
	Kind ContainerKind
	Name string
	// Added is a live container accepted source does not have; Removed the reverse.
	Added   bool
	Removed bool
	// Fields are the container fields accepted source declares whose live
	// values differ, such as "image" or "command". Undeclared live fields
	// (server defaults) never appear.
	Fields []string
}

// Changed reports whether field is one of the container's differing declared fields.
func (c ContainerChange) Changed(field string) bool {
	for _, name := range c.Fields {
		if name == field {
			return true
		}
	}
	return false
}

// ContainerChanges compares a workload's or Pod's containers with accepted
// source by kind and name: an added init container is never an app image change.
func (d SourceDiff) ContainerChanges() ([]ContainerChange, error) {
	accepted, observed, err := d.Sides()
	if err != nil {
		return nil, err
	}
	var changes []ContainerChange
	for _, list := range containerLists {
		before := namedContainers(podSpecFields(accepted), list.key)
		after := namedContainers(podSpecFields(observed), list.key)
		for _, name := range unionNames(before, after) {
			left, declared := before[name]
			right, live := after[name]
			change := ContainerChange{Kind: list.kind, Name: name, Added: !declared, Removed: !live}
			if declared && live {
				for _, field := range sortedKeys(left) {
					if value, ok := right[field]; !ok || !reflect.DeepEqual(left[field], value) {
						change.Fields = append(change.Fields, field)
					}
				}
			}
			if change.Added || change.Removed || len(change.Fields) > 0 {
				changes = append(changes, change)
			}
		}
	}
	return changes, nil
}

// podSpecFields finds the pod spec of a Pod, a pod-template workload or a CronJob.
func podSpecFields(object map[string]any) map[string]any {
	for _, path := range [][]fieldStep{{{key: "spec"}, {key: "jobTemplate"}, {key: "spec"}, {key: "template"}, {key: "spec"}}, {{key: "spec"}, {key: "template"}, {key: "spec"}}, {{key: "spec"}}} {
		if value, ok := lookupField(object, path); ok {
			if spec, ok := value.(map[string]any); ok {
				if _, isTemplate := spec["template"]; !isTemplate {
					if _, isJob := spec["jobTemplate"]; !isJob {
						return spec
					}
				}
			}
		}
	}
	return nil
}

func namedContainers(spec map[string]any, key string) map[string]map[string]any {
	result := map[string]map[string]any{}
	items, _ := spec[key].([]any)
	for _, item := range items {
		container, _ := item.(map[string]any)
		if name, _ := container["name"].(string); name != "" {
			result[name] = container
		}
	}
	return result
}

func unionNames(left, right map[string]map[string]any) []string {
	seen := map[string]bool{}
	for name := range left {
		seen[name] = true
	}
	for name := range right {
		seen[name] = true
	}
	return sortedKeys(seen)
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

package settings

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Field describes one configuration key for the generated reference.
type Field struct {
	Key     string // dotted YAML path, for example response.timeout
	Env     string // environment variable that sets it
	Type    string
	Default string
	Doc     string
}

// workerTransport lists variables that share a section prefix with the knobs but are internal worker or
// installer transport, never operator configuration.
var workerTransport = []string{
	"SDO_AGENT_GUIDANCE", "SDO_RESPONSE_CONTINUATION", "SDO_DEPLOYMENT_MODEL", "SDO_TELEMETRY_POLICY",
}

// workerTransportPrefixes are families of such variables.
var workerTransportPrefixes = []string{"SDO_AGENT_SCRIPT"}

// MemoryCheckTimeoutEnvironment carries agent.memory_check_timeout, in whole seconds, to the workers that run
// `sdo-operator memory check`.
const MemoryCheckTimeoutEnvironment = "SDO_MEMORY_CHECK_TIMEOUT_SECONDS"

// LifecycleTimeoutEnvironment carries agent.lifecycle_timeout, in whole seconds, to lifecycle workers.
const LifecycleTimeoutEnvironment = "SDO_LIFECYCLE_TIMEOUT_SECONDS"

// WorkerVariables names the internal worker and installer variables for the generated reference.
func WorkerVariables() []string {
	return append(append([]string{}, workerTransport...), "SDO_AGENT_SCRIPT_*", "SDO_TASK_PATH", "SDO_INCIDENT_ID", ReplayEnvironment, "SDO_CODE_PATH",
		LifecycleTimeoutEnvironment, MemoryCheckTimeoutEnvironment, "SDO_RESPONDER_MODEL", "SDO_REFLECTION_MODEL", "SDO_LIFECYCLE_MODEL")
}

// renamedEnvironment maps host environment names that used to select a knob to their configuration names. They stay
// as internal worker variables, so only the host entry points refuse them, and only for one release.
var renamedEnvironment = map[string]string{
	"SDO_CODE_PATH":      "SDO_AGENT_CODE_PATH",
	"SDO_RESPONDER_MENU": "SDO_RESPONSE_MENU",
}

// RefuseRenamedEnvironment is the one-release guard of host and install-existing: while an old knob name is set in
// their environment they refuse to start and name its replacement, so a silently ignored setting cannot change a run.
func RefuseRenamedEnvironment(environ []string) error {
	var names []string
	for name := range renamedEnvironment {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, line := range environ {
			if key, _, _ := strings.Cut(line, "="); key == name {
				return fmt.Errorf("%s is set but is no longer read by the host: set %s instead (or unset %s)", name, renamedEnvironment[name], name)
			}
		}
	}
	return nil
}

type leaf struct {
	key   string
	env   string
	value reflect.Value
	doc   string
}

// walk lists every settable key: scalars and lists directly, a struct nested in a section as one JSON value.
func walk(cfg *Config) (sections map[string]bool, leaves []leaf) {
	sections = map[string]bool{}
	root := reflect.ValueOf(cfg).Elem()
	for i := 0; i < root.NumField(); i++ {
		field, value := root.Type().Field(i), root.Field(i)
		tag := yamlName(field)
		if value.Kind() != reflect.Struct {
			leaves = append(leaves, leaf{key: tag, env: "SDO_" + strings.ToUpper(tag), value: value, doc: field.Tag.Get("doc")})
			continue
		}
		sections[strings.ToUpper(tag)] = true
		for j := 0; j < value.NumField(); j++ {
			sub := value.Type().Field(j)
			name := yamlName(sub)
			leaves = append(leaves, leaf{key: tag + "." + name, env: "SDO_" + strings.ToUpper(tag) + "_" + strings.ToUpper(name), value: value.Field(j), doc: sub.Tag.Get("doc")})
		}
	}
	return sections, leaves
}

func yamlName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
	return name
}

func overlayEnvironment(cfg *Config, environ []string) error {
	sections, leaves := walk(cfg)
	byEnv := map[string]leaf{}
	for _, l := range leaves {
		byEnv[l.env] = l
	}
	values := map[string]string{}
	for _, line := range environ {
		name, value, _ := strings.Cut(line, "=")
		if _, known := byEnv[name]; known {
			values[name] = value
			continue
		}
		section, _, hasKey := strings.Cut(strings.TrimPrefix(name, "SDO_"), "_")
		if !strings.HasPrefix(name, "SDO_") || !hasKey || !sections[section] || isWorkerTransport(name) {
			continue
		}
		return fmt.Errorf("environment %s names no configuration key%s", name, suggestion(name, byEnv))
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := setValue(byEnv[name].value, values[name]); err != nil {
			return fmt.Errorf("environment %s (%s): %w", name, byEnv[name].key, err)
		}
	}
	return nil
}

func isWorkerTransport(name string) bool {
	for _, exact := range workerTransport {
		if name == exact {
			return true
		}
	}
	for _, prefix := range workerTransportPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// suggestion names the configuration variable closest to a mistyped one.
func suggestion(name string, known map[string]leaf) string {
	best, bestDistance := "", 4
	for candidate := range known {
		if d := distance(name, candidate); d < bestDistance {
			best, bestDistance = candidate, d
		}
	}
	if best == "" {
		return ""
	}
	return " (did you mean " + best + "?)"
}

func distance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}

func setValue(target reflect.Value, text string) error {
	switch target.Kind() {
	case reflect.String:
		target.SetString(text)
	case reflect.Bool:
		parsed, err := strconv.ParseBool(text)
		if err != nil {
			return err
		}
		target.SetBool(parsed)
	case reflect.Int:
		parsed, err := strconv.Atoi(text)
		if err != nil {
			return err
		}
		target.SetInt(int64(parsed))
	case reflect.Int64:
		if target.Type() == reflect.TypeOf(Duration(0)) {
			parsed, err := time.ParseDuration(text)
			if err != nil {
				return err
			}
			target.SetInt(int64(parsed))
			return nil
		}
		fallthrough
	default:
		// A list or a struct is JSON (YAML flow syntax is a superset), decoded strictly.
		decoder := yaml.NewDecoder(bytes.NewReader([]byte(text)))
		decoder.KnownFields(true)
		fresh := reflect.New(target.Type())
		if err := decoder.Decode(fresh.Interface()); err != nil {
			return err
		}
		target.Set(fresh.Elem())
	}
	return nil
}

// Fields lists every key with its environment name, type, default and doc, in declaration order.
func Fields() []Field {
	cfg := Default()
	_, leaves := walk(&cfg)
	fields := make([]Field, 0, len(leaves))
	for _, l := range leaves {
		fields = append(fields, Field{Key: l.key, Env: l.env, Type: typeName(l.value), Default: renderDefault(l.value), Doc: l.doc})
	}
	return fields
}

func typeName(v reflect.Value) string {
	switch {
	case v.Type() == reflect.TypeOf(Duration(0)):
		return "duration"
	case v.Type() == reflect.TypeOf(Mode("")):
		return "enum"
	case v.Kind() == reflect.Int:
		return "int"
	case v.Kind() == reflect.Bool:
		return "bool"
	case v.Kind() == reflect.Slice:
		return "list (JSON)"
	case v.Kind() == reflect.Struct:
		return "object (JSON)"
	}
	return "string"
}

func renderDefault(v reflect.Value) string {
	switch v.Kind() {
	case reflect.Slice:
		if v.Len() == 0 {
			return "[]"
		}
	case reflect.Struct:
		return "{}"
	case reflect.String:
		if v.String() == "" {
			return `""`
		}
	}
	if d, ok := v.Interface().(Duration); ok {
		return d.String()
	}
	return fmt.Sprint(v.Interface())
}

// Markdown renders one table per section (the generated reference's configuration part).
func Markdown() string {
	var out strings.Builder
	section := ""
	for _, f := range Fields() {
		head, _, nested := strings.Cut(f.Key, ".")
		title := "`" + head + "`"
		if !nested {
			head, title = "top level", "Top level"
		}
		if head != section {
			section = head
			fmt.Fprintf(&out, "\n### %s\n\n| Key | Env | Type | Default | Description |\n|---|---|---|---|---|\n", title)
		}
		fmt.Fprintf(&out, "| `%s` | `%s` | %s | `%s` | %s |\n", f.Key, f.Env, f.Type, f.Default, f.Doc)
	}
	return out.String()
}

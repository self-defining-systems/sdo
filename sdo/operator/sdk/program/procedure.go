package program

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Effect classes of an application procedure. The class is recorded on every
// run; it never gates one.
const (
	// ProcedureCheck only reads: a self-check or status report.
	ProcedureCheck = "check"
	// ProcedureRepair is idempotent: running it again changes nothing more.
	ProcedureRepair = "repair"
	// ProcedureData changes application records.
	ProcedureData = "data"
	// ProcedureLong runs for a long time, such as a migration or a backup.
	ProcedureLong = "long"
)

// Parameter types of a procedure.
const (
	ProcedureString = "string"
	ProcedureInt    = "int"
	ProcedureBool   = "bool"
)

// Bounds of a procedure declaration and its bound command.
const (
	MaxProcedureArgs     = 32
	MaxProcedureArgBytes = 1024
)

// procedurePlaceholder wraps a parameter name inside a command argument, as
// in "--scope={{scope}}".
var procedurePlaceholder = regexp.MustCompile(`\{\{([^{}]*)\}\}`)

var (
	procedureID        = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	procedureParamName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	// defaultProcedureValue is the alphabet of a string parameter without its
	// own pattern: no space, quote, glob or shell character, and no leading
	// '-', so a value is one inert argument.
	defaultProcedureValue = regexp.MustCompile(`^[A-Za-z0-9_.:@/+=,][A-Za-z0-9_.:@/+=,-]{0,255}$`)
	procedureJob          = regexp.MustCompile(`^Job/[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`)
)

// Procedure is one operations procedure the application itself ships: a
// self-check, a repair task, a data task or a long job, recorded under
// `procedures` in the accepted application map. It runs either in a
// component's workload (Component, Container, Command) or by re-creating a
// Job of the accepted source (Job). Command arguments may hold {{name}}
// placeholders for declared parameters.
type Procedure struct {
	ID        string           `json:"id"`
	Component string           `json:"component,omitempty"`
	Container string           `json:"container,omitempty"`
	Job       string           `json:"job,omitempty"`
	Command   []string         `json:"command,omitempty"`
	Params    []ProcedureParam `json:"params,omitempty"`
	Effect    string           `json:"effect"`
	// Verify says how to tell the procedure worked; it is advice for agents.
	Verify  string        `json:"verify,omitempty"`
	Timeout time.Duration `json:"timeout,omitempty"`
	// Source is where the procedure is declared: a source file, a doc or a
	// manifest object.
	Source string `json:"source"`
	// ProbeIdentity marks the procedure SDO runs to provision the test users
	// health probes sign in as: it makes sure a non-admin user named by its
	// only parameter, a required string username, exists, can sign in and has
	// the password written to its stdin (the value, a newline, then EOF). It
	// leaves a user whose password already matches untouched. At most one
	// procedure of a map is the probe identity.
	ProbeIdentity bool `json:"probe_identity,omitempty"`
}

// DefaultProbeIdentityTimeout bounds a probe-identity procedure that declares
// no timeout: an admin CLI may take a minute to boot.
const DefaultProbeIdentityTimeout = 2 * time.Minute

// ProbeIdentityParam is the probe-identity procedure's only parameter.
const ProbeIdentityParam = "username"

// IdentityTimeout is how long one run of a probe-identity procedure may take.
func (p Procedure) IdentityTimeout() time.Duration {
	if p.Timeout > 0 {
		return p.Timeout
	}
	return DefaultProbeIdentityTimeout
}

// ProcedureParam is one typed parameter of a procedure. Pattern is an RE2
// that a string value must match in full; without it a string value is one
// plain word.
type ProcedureParam struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Pattern  string `json:"pattern,omitempty"`
	Required bool   `json:"required,omitempty"`
	Default  any    `json:"default,omitempty"`
}

// Validate checks the procedure's own fields. Whether its component and Job
// exist is the application map's check.
func (p Procedure) Validate() error {
	if !procedureID.MatchString(p.ID) {
		return fmt.Errorf("procedure id %q is not a logical ID (lowercase letters, digits and '-')", p.ID)
	}
	switch p.Effect {
	case ProcedureCheck, ProcedureRepair, ProcedureData, ProcedureLong:
	default:
		return fmt.Errorf("procedure %s: effect %q is not check, repair, data or long", p.ID, p.Effect)
	}
	if strings.TrimSpace(p.Source) == "" {
		return fmt.Errorf("procedure %s has no source; cite the file, doc or manifest object that declares it", p.ID)
	}
	if p.Timeout < 0 || p.Timeout > MaxScriptTimeout {
		return fmt.Errorf("procedure %s: timeout must be between zero and %s", p.ID, MaxScriptTimeout)
	}
	if (p.Component == "") == (p.Job == "") {
		return fmt.Errorf("procedure %s runs either in a component (component, container, command) or as a Job (job: Job/<name>)", p.ID)
	}
	if p.ProbeIdentity {
		if err := p.validateProbeIdentity(); err != nil {
			return err
		}
	}
	if p.Job != "" {
		if !procedureJob.MatchString(p.Job) {
			return fmt.Errorf("procedure %s: job %q is not Job/<name>", p.ID, p.Job)
		}
		if p.Container != "" || len(p.Command) > 0 || len(p.Params) > 0 {
			return fmt.Errorf("procedure %s re-creates a Job from its template; it takes no container, command or params", p.ID)
		}
		return nil
	}
	if strings.TrimSpace(p.Container) == "" {
		return fmt.Errorf("procedure %s names no container of component %s", p.ID, p.Component)
	}
	if len(p.Command) == 0 || len(p.Command) > MaxProcedureArgs || strings.TrimSpace(p.Command[0]) == "" {
		return fmt.Errorf("procedure %s needs a command of 1 to %d arguments, executable first", p.ID, MaxProcedureArgs)
	}
	declared := map[string]ProcedureParam{}
	for _, param := range p.Params {
		if err := param.validate(p.ID); err != nil {
			return err
		}
		if _, dup := declared[param.Name]; dup {
			return fmt.Errorf("procedure %s: parameter %s is declared more than once", p.ID, param.Name)
		}
		declared[param.Name] = param
	}
	used := map[string]bool{}
	for _, arg := range p.Command {
		if len(arg) > MaxProcedureArgBytes || strings.ContainsRune(arg, 0) {
			return fmt.Errorf("procedure %s: an argument exceeds %d bytes or holds NUL", p.ID, MaxProcedureArgBytes)
		}
		for _, match := range procedurePlaceholder.FindAllStringSubmatch(arg, -1) {
			if _, ok := declared[match[1]]; !ok {
				return fmt.Errorf("procedure %s: command placeholder {{%s}} is not a declared parameter", p.ID, match[1])
			}
			used[match[1]] = true
		}
	}
	for name := range declared {
		if !used[name] {
			return fmt.Errorf("procedure %s: parameter %s appears in no command argument", p.ID, name)
		}
	}
	return nil
}

// validateProbeIdentity checks the shape SDO runs a probe-identity procedure
// with: a component command, a data effect and exactly one required string
// username parameter, so the password can only travel on stdin.
func (p Procedure) validateProbeIdentity() error {
	const shape = "runs in a component, has effect data and exactly one parameter, a required string username without a default; the password arrives on stdin"
	if p.Component == "" || p.Effect != ProcedureData || len(p.Params) != 1 {
		return fmt.Errorf("procedure %s: a probe identity procedure %s", p.ID, shape)
	}
	param := p.Params[0]
	if param.Name != ProbeIdentityParam || param.Type != ProcedureString || !param.Required || param.Default != nil {
		return fmt.Errorf("procedure %s: a probe identity procedure %s", p.ID, shape)
	}
	return nil
}

func (q ProcedureParam) validate(id string) error {
	if !procedureParamName.MatchString(q.Name) {
		return fmt.Errorf("procedure %s: parameter name %q is not lowercase letters, digits and '_'", id, q.Name)
	}
	switch q.Type {
	case ProcedureString:
		if q.Pattern != "" {
			if _, err := regexp.Compile(q.Pattern); err != nil {
				return fmt.Errorf("procedure %s: parameter %s pattern: %w", id, q.Name, err)
			}
		}
	case ProcedureInt, ProcedureBool:
		if q.Pattern != "" {
			return fmt.Errorf("procedure %s: parameter %s: only a string parameter takes a pattern", id, q.Name)
		}
	default:
		return fmt.Errorf("procedure %s: parameter %s type %q is not string, int or bool", id, q.Name, q.Type)
	}
	if q.Default != nil {
		if _, err := q.text(q.Default); err != nil {
			return fmt.Errorf("procedure %s: parameter %s default: %w", id, q.Name, err)
		}
	}
	return nil
}

// text validates one value against the parameter's type and renders it as an
// argument.
func (q ProcedureParam) text(value any) (string, error) {
	switch q.Type {
	case ProcedureInt:
		switch v := value.(type) {
		case int:
			return strconv.Itoa(v), nil
		case int32:
			return strconv.FormatInt(int64(v), 10), nil
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			if v == float64(int64(v)) {
				return strconv.FormatInt(int64(v), 10), nil
			}
		}
		return "", fmt.Errorf("value %v is not an integer", value)
	case ProcedureBool:
		if v, ok := value.(bool); ok {
			return strconv.FormatBool(v), nil
		}
		return "", fmt.Errorf("value %v is not a bool", value)
	case ProcedureString:
		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("value %v is not a string", value)
		}
		pattern := defaultProcedureValue
		if q.Pattern != "" {
			compiled, err := regexp.Compile(`^(?:` + q.Pattern + `)$`)
			if err != nil {
				return "", err
			}
			pattern = compiled
		}
		if !pattern.MatchString(v) || strings.ContainsAny(v, "\x00\n\r") {
			return "", fmt.Errorf("value %q does not match the parameter's pattern", v)
		}
		return v, nil
	}
	return "", fmt.Errorf("unknown parameter type %q", q.Type)
}

// Bind validates values against the declared parameters and returns the
// command with every placeholder replaced. A missing value takes the
// parameter's default; a required parameter without one is an error, and an
// optional one without a default is an error only when the command uses it.
func (p Procedure) Bind(values map[string]any) ([]string, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	declared := map[string]ProcedureParam{}
	for _, param := range p.Params {
		declared[param.Name] = param
	}
	for name := range values {
		if _, ok := declared[name]; !ok {
			return nil, fmt.Errorf("procedure %s has no parameter %q", p.ID, name)
		}
	}
	texts := map[string]string{}
	for _, param := range p.Params {
		value, ok := values[param.Name]
		if !ok {
			value = param.Default
		}
		if value == nil {
			if param.Required {
				return nil, fmt.Errorf("procedure %s: required parameter %s is unbound", p.ID, param.Name)
			}
			continue
		}
		text, err := param.text(value)
		if err != nil {
			return nil, fmt.Errorf("procedure %s: parameter %s: %w", p.ID, param.Name, err)
		}
		texts[param.Name] = text
	}
	argv := make([]string, len(p.Command))
	for i, arg := range p.Command {
		var missing string
		argv[i] = procedurePlaceholder.ReplaceAllStringFunc(arg, func(match string) string {
			name := match[2 : len(match)-2]
			text, ok := texts[name]
			if !ok {
				missing = name
			}
			return text
		})
		if missing != "" {
			return nil, fmt.Errorf("procedure %s: parameter %s is unbound and has no default", p.ID, missing)
		}
		if len(argv[i]) > MaxProcedureArgBytes {
			return nil, fmt.Errorf("procedure %s: bound argument %d exceeds %d bytes", p.ID, i+1, MaxProcedureArgBytes)
		}
	}
	return argv, nil
}

// Procedure returns the procedure with id.
func (m AppMap) Procedure(id string) (Procedure, bool) {
	for _, p := range m.Procedures {
		if p.ID == id {
			return p, true
		}
	}
	return Procedure{}, false
}

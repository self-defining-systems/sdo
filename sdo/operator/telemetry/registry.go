package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
)

var (
	adapterName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)
	// SourceIDPattern is the grammar of a bound source id.
	SourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)
)

// Adapter is what an adapter package registers. C is the adapter's options
// type; it is decoded strictly (unknown fields are an error), so a binding can
// never carry an option the adapter ignores.
type Adapter[C any] struct {
	Name    string
	Kinds   []Kind
	Dialect string
	Caps    []Capability
	// Validate is pure: no I/O, no clock. The broker calls it.
	Validate func(C, Endpoint) error
	// Open builds a client. Only the runtime calls it. A client for an adapter
	// with CapAlerts must implement AlertReader.
	Open func(C, Endpoint, Env) (Client, error)
}

// Descriptor is the I/O-free description of a registered adapter.
type Descriptor struct {
	Name    string
	Kinds   []Kind
	Dialect string
	Caps    []Capability
}

// Binding binds one source id to an adapter, endpoint and options. It is the
// decoded form of one entry in a sources file.
type Binding struct {
	ID       string
	Adapter  string
	Kinds    []Kind
	Endpoint Endpoint
	Default  bool
	Origin   Origin
	// Options is the adapter's options as a JSON object; empty means defaults.
	Options json.RawMessage
}

// Registry maps adapter names to registered adapters. Build it explicitly with
// Register; there is no init() registration. After wiring it is read-only and
// safe for concurrent use.
type Registry struct {
	entries map[string]entry
}

type entry struct {
	desc     Descriptor
	validate func(json.RawMessage, Endpoint) error
	open     func(json.RawMessage, Endpoint, Env) (Client, error)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{entries: map[string]entry{}} }

// Register adds a to r. The single place options are decoded is here.
func Register[C any](r *Registry, a Adapter[C]) error {
	if r == nil || r.entries == nil {
		return errors.New("telemetry: register into a registry made by NewRegistry")
	}
	if !adapterName.MatchString(a.Name) {
		return fmt.Errorf("telemetry: adapter name %q must match %s", a.Name, adapterName)
	}
	if _, dup := r.entries[a.Name]; dup {
		return fmt.Errorf("telemetry: adapter %q is already registered", a.Name)
	}
	if len(a.Kinds) == 0 {
		return fmt.Errorf("telemetry: adapter %q declares no kinds", a.Name)
	}
	seen := map[Kind]bool{}
	for _, k := range a.Kinds {
		if !k.Valid() {
			return fmt.Errorf("telemetry: adapter %q declares unknown kind %q", a.Name, k)
		}
		if seen[k] {
			return fmt.Errorf("telemetry: adapter %q declares kind %q twice", a.Name, k)
		}
		seen[k] = true
	}
	for _, c := range a.Caps {
		if !c.Valid() {
			return fmt.Errorf("telemetry: adapter %q declares unknown capability %q", a.Name, c)
		}
	}
	if a.Dialect == "" {
		return fmt.Errorf("telemetry: adapter %q needs a dialect description", a.Name)
	}
	if a.Validate == nil || a.Open == nil {
		return fmt.Errorf("telemetry: adapter %q needs Validate and Open", a.Name)
	}
	name := a.Name
	r.entries[name] = entry{
		desc: Descriptor{Name: name, Kinds: slices.Clone(a.Kinds), Dialect: a.Dialect, Caps: slices.Clone(a.Caps)},
		validate: func(raw json.RawMessage, ep Endpoint) error {
			c, err := decodeOptions[C](name, raw)
			if err != nil {
				return err
			}
			return a.Validate(c, ep)
		},
		open: func(raw json.RawMessage, ep Endpoint, env Env) (Client, error) {
			c, err := decodeOptions[C](name, raw)
			if err != nil {
				return nil, err
			}
			return a.Open(c, ep, env)
		},
	}
	return nil
}

func decodeOptions[C any](adapter string, raw json.RawMessage) (C, error) {
	var c C
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return c, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("telemetry: adapter %q options: %w", adapter, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return c, fmt.Errorf("telemetry: adapter %q options: trailing data after the object", adapter)
	}
	return c, nil
}

// Describe returns the descriptor of adapter without any I/O.
func (r *Registry) Describe(adapter string) (Descriptor, bool) {
	e, ok := r.entries[adapter]
	if !ok {
		return Descriptor{}, false
	}
	d := e.desc
	d.Kinds, d.Caps = slices.Clone(d.Kinds), slices.Clone(d.Caps)
	return d, true
}

// Adapters lists registered adapter names in sorted order.
func (r *Registry) Adapters() []string {
	names := make([]string, 0, len(r.entries))
	for n := range r.entries {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Check validates a binding against its adapter without I/O: the id grammar,
// that the adapter exists, that Kinds is a non-empty subset of the adapter's,
// and that the adapter accepts the options and endpoint.
func (r *Registry) Check(b Binding) error {
	_, err := r.check(b)
	return err
}

func (r *Registry) check(b Binding) (entry, error) {
	if !SourceIDPattern.MatchString(b.ID) {
		return entry{}, fmt.Errorf("telemetry source id %q must match %s", b.ID, SourceIDPattern)
	}
	e, ok := r.entries[b.Adapter]
	if !ok {
		return entry{}, fmt.Errorf("telemetry source %q: unknown adapter %q (registered: %v)", b.ID, b.Adapter, r.Adapters())
	}
	if len(b.Kinds) == 0 {
		return entry{}, fmt.Errorf("telemetry source %q declares no kinds", b.ID)
	}
	seen := map[Kind]bool{}
	for _, k := range b.Kinds {
		if !slices.Contains(e.desc.Kinds, k) {
			return entry{}, fmt.Errorf("telemetry source %q: adapter %q does not serve kind %q (serves %v)", b.ID, b.Adapter, k, e.desc.Kinds)
		}
		if seen[k] {
			return entry{}, fmt.Errorf("telemetry source %q lists kind %q twice", b.ID, k)
		}
		seen[k] = true
	}
	if b.Origin != "" && !b.Origin.Valid() {
		return entry{}, fmt.Errorf("telemetry source %q has unknown origin %q", b.ID, b.Origin)
	}
	if err := e.validate(b.Options, b.Endpoint); err != nil {
		return entry{}, fmt.Errorf("telemetry source %q: %w", b.ID, err)
	}
	return e, nil
}

// Source returns the detector- and agent-facing description of a checked
// binding. An unset Origin is reported as OriginOperator.
func (r *Registry) Source(b Binding) (Source, error) {
	e, err := r.check(b)
	if err != nil {
		return Source{}, err
	}
	origin := b.Origin
	if origin == "" {
		origin = OriginOperator
	}
	return Source{
		ID:           b.ID,
		Kinds:        slices.Clone(b.Kinds),
		Adapter:      b.Adapter,
		Dialect:      e.desc.Dialect,
		Default:      b.Default,
		Capabilities: slices.Clone(e.desc.Caps),
		Origin:       origin,
	}, nil
}

// Open checks b, then builds its client. Only the runtime calls Open.
func (r *Registry) Open(b Binding, env Env) (Client, error) {
	e, err := r.check(b)
	if err != nil {
		return nil, err
	}
	c, err := e.open(b.Options, b.Endpoint, env)
	if err != nil {
		return nil, fmt.Errorf("telemetry source %q: open %s: %w", b.ID, b.Adapter, err)
	}
	if c == nil {
		return nil, fmt.Errorf("telemetry source %q: adapter %q returned a nil client", b.ID, b.Adapter)
	}
	if slices.Contains(e.desc.Caps, CapAlerts) {
		if _, ok := c.(AlertReader); !ok {
			return nil, fmt.Errorf("telemetry source %q: adapter %q advertises alerts but its client is not an AlertReader", b.ID, b.Adapter)
		}
	}
	return c, nil
}

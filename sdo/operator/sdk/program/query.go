package program

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storeops"
)

// Bounds of one structured read.
const (
	// MaxDiagnoseReads is the most Read calls one Diagnose makes. A Diagnose
	// shares one diagnosis deadline, so a later read returns ErrDeadline.
	MaxDiagnoseReads = 3
	// MaxProbeFields is the most named fields one probe query extracts.
	MaxProbeFields = 16
	// MaxRows, MaxRowFields and MaxScalarBytes bound an Observation.
	MaxRows        = storeops.MaxReadRows
	MaxRowFields   = storeops.MaxReadFields
	MaxScalarBytes = storeops.MaxReadValueBytes
)

// ErrDeadline is returned by a Read past a Diagnose's read budget. A Diagnose
// turns it into Unknown.
var ErrDeadline = errors.New("diagnosis read budget exhausted")

// Query is one structured read; exactly one kind is set.
//
// A Diagnose gets typed rows instead of exit codes: the records of a store the
// accepted application map grounds, or named fields of an unauthenticated HTTP
// status response. A read never returns a credential: credential catalogs and
// credential-named fields are refused, and a password hash counts as a
// credential.
type Query struct {
	Store *StoreQuery `json:"store,omitempty"`
	Probe *ProbeQuery `json:"probe,omitempty"`
}

// StoreQuery reads named fields of the records a scope selects, over the same
// target and DataScope that RepairData writes. The selector is restricted to
// column names, comparison and arithmetic operators, AND/OR/NOT, IS [NOT] NULL,
// IN (literals) and literals (for mongodb, comparison and logical operators):
// a function call is refused. Fields are plain column names (mongodb: dotted
// paths to scalars), at most 16. redis and memcached have no rows: use Count.
type StoreQuery struct {
	Target StoreTarget `json:"target"`
	Scope  DataScope   `json:"scope"`
	Fields []string    `json:"fields"`
}

// ProbeQuery sends one GET, HEAD or OPTIONS request without credentials to a
// Service and extracts the scalars at the JSON pointers (RFC 6901) in
// Req.Fields from the response body (read to at most 64 KiB, then dropped:
// only the fields are kept). Field names and every pointer segment must not be
// credential-named.
type ProbeQuery struct {
	To  sdk.ObjectRef `json:"to"`
	Req HTTPProbe     `json:"req"`
}

// Scalar is one value of a Row: int64, float64, bool, string of at most 256
// bytes, or nil.
type Scalar = any

// Row maps a requested field to its value. A field absent from the source is
// absent from the row.
type Row map[string]Scalar

// Observation is the typed answer of a Read.
type Observation struct {
	// Status is the HTTP status of a probe query; 0 for a store query.
	Status int `json:"status,omitempty"`
	// Count is the number of items a store query's scope matches (more than
	// Rows when the scope holds more than MaxRows items). It is 0 for a probe.
	Count int `json:"count,omitempty"`
	// Rows holds at most 32 rows: one per matching item for a store query,
	// one holding the extracted fields for a probe query.
	Rows []Row `json:"rows,omitempty"`
}

// Int reads an integer field.
func (r Row) Int(name string) (int64, bool) {
	switch v := r[name].(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	}
	return 0, false
}

// Float reads a numeric field.
func (r Row) Float(name string) (float64, bool) {
	switch v := r[name].(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	}
	return 0, false
}

// String reads a string field.
func (r Row) String(name string) (string, bool) {
	v, ok := r[name].(string)
	return v, ok
}

// Bool reads a boolean field.
func (r Row) Bool(name string) (bool, bool) {
	v, ok := r[name].(bool)
	return v, ok
}

// IsNull reports whether the field is present and null.
func (r Row) IsNull(name string) bool {
	v, ok := r[name]
	return ok && v == nil
}

// Validate requires exactly one kind and checks it.
func (q Query) Validate() error {
	switch {
	case q.Store != nil && q.Probe != nil:
		return fmt.Errorf("a query sets exactly one of Store and Probe")
	case q.Store != nil:
		return q.Store.Validate()
	case q.Probe != nil:
		return q.Probe.Validate()
	}
	return fmt.Errorf("a query sets exactly one of Store and Probe")
}

// Validate checks the target, scope, selector grammar, catalog and fields.
func (q StoreQuery) Validate() error {
	if err := q.Target.Validate(); err != nil {
		return err
	}
	return storeops.ValidateRead(q.Target.EngineOps(), q.Scope, q.Fields)
}

// Validate checks that the request is a credential-free read of a Service and
// that every field is a bounded pointer to a non-credential name.
func (q ProbeQuery) Validate() error {
	if q.To.Kind != "Service" || q.To.Name == "" || q.To.Namespace == "" {
		return fmt.Errorf("a probe query targets a named Service")
	}
	switch q.Req.Method {
	case "", "GET", "HEAD", "OPTIONS":
	default:
		return fmt.Errorf("a probe query reads: method %q is not GET, HEAD or OPTIONS", q.Req.Method)
	}
	if !strings.HasPrefix(q.Req.Path, "/") || strings.HasPrefix(q.Req.Path, "//") {
		return fmt.Errorf("a probe query path is absolute within the Service")
	}
	parsed, err := url.ParseRequestURI(q.Req.Path)
	if err != nil {
		return fmt.Errorf("a probe query path is not a valid path: %w", err)
	}
	for name := range parsed.Query() {
		if credentialHTTPName(name) || storeops.CredentialName(name) {
			return fmt.Errorf("a probe query carries no credentials: query parameter %q", name)
		}
	}
	for name, value := range q.Req.Headers {
		if credentialHTTPName(name) || storeops.CredentialName(name) || strings.ContainsAny(name+value, "\r\n") {
			return fmt.Errorf("a probe query carries no credentials: header %q", name)
		}
	}
	if len(q.Req.Fields) == 0 || len(q.Req.Fields) > MaxProbeFields {
		return fmt.Errorf("a probe query extracts 1..%d fields, got %d", MaxProbeFields, len(q.Req.Fields))
	}
	for name, pointer := range q.Req.Fields {
		if name == "" || len(name) > 64 || storeops.CredentialName(name) {
			return fmt.Errorf("probe field name %q is empty, long or named like a credential", name)
		}
		if err := ValidateJSONPointer(pointer); err != nil {
			return fmt.Errorf("probe field %q: %w", name, err)
		}
	}
	return nil
}

// ValidateJSONPointer checks an RFC 6901 pointer to a value inside a body: it
// begins with "/", has at most 8 segments and no segment is named like a
// credential.
func ValidateJSONPointer(pointer string) error {
	if !strings.HasPrefix(pointer, "/") || len(pointer) > 256 {
		return fmt.Errorf("pointer %q must be an RFC 6901 pointer to a scalar, such as /status", pointer)
	}
	segments := strings.Split(pointer[1:], "/")
	if len(segments) > 8 {
		return fmt.Errorf("pointer %q has more than 8 segments", pointer)
	}
	for _, segment := range segments {
		if storeops.CredentialName(strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")) {
			return fmt.Errorf("pointer %q passes a credential-named segment", pointer)
		}
	}
	return nil
}

// Normalize checks the bounds and returns a copy whose numbers have one Go
// type each: a whole number is int64, any other number float64. It makes a
// live answer and the same answer read back from a recording equal.
func (o Observation) Normalize() (Observation, error) {
	if len(o.Rows) > MaxRows {
		return Observation{}, fmt.Errorf("an observation holds at most %d rows", MaxRows)
	}
	out := Observation{Status: o.Status, Count: o.Count}
	for _, row := range o.Rows {
		clean, err := row.normalize()
		if err != nil {
			return Observation{}, err
		}
		out.Rows = append(out.Rows, clean)
	}
	return out, nil
}

func (r Row) normalize() (Row, error) {
	if len(r) > MaxRowFields {
		return nil, fmt.Errorf("a row holds at most %d fields", MaxRowFields)
	}
	out := make(Row, len(r))
	for name, value := range r {
		switch v := value.(type) {
		case nil, bool, int64:
			out[name] = v
		case int:
			out[name] = int64(v)
		case float64:
			if v == float64(int64(v)) && v > -(1<<53) && v < 1<<53 {
				out[name] = int64(v)
			} else {
				out[name] = v
			}
		case string:
			if len(v) > MaxScalarBytes {
				return nil, fmt.Errorf("field %q holds a string of more than %d bytes", name, MaxScalarBytes)
			}
			out[name] = v
		default:
			return nil, fmt.Errorf("field %q is not a scalar", name)
		}
	}
	return out, nil
}

// ReadProbe serves a probe query through ProbeHTTP: the request's Fields ride
// on the probe, and the answer's fields become the only row.
func ReadProbe(v ReadView, q ProbeQuery) (Observation, error) {
	if err := q.Validate(); err != nil {
		return Observation{}, err
	}
	result, err := v.ProbeHTTP(q.To, q.Req)
	if err != nil {
		return Observation{}, err
	}
	if result.Status == 0 {
		return Observation{}, fmt.Errorf("probe of %s/%s answered no HTTP response: %s", q.To.Namespace, q.To.Name, result.Detail)
	}
	return Observation{Status: result.Status, Rows: []Row{result.Fields}}.Normalize()
}

func (v snapshotReadView) Read(q Query) (Observation, error) {
	if err := q.Validate(); err != nil {
		return Observation{}, err
	}
	if source, ok := v.DetectionContext.(interface {
		Read(Query) (Observation, error)
	}); ok {
		return source.Read(q)
	}
	return Observation{}, fmt.Errorf("structured read unavailable")
}

func (v *checkedReadView) Read(q Query) (Observation, error) {
	if v.maxReads > 0 {
		v.mu.Lock()
		v.reads++
		over := v.reads > v.maxReads
		v.mu.Unlock()
		if over {
			err := fmt.Errorf("%w: a Diagnose makes at most %d reads", ErrDeadline, v.maxReads)
			v.check(err)
			return Observation{}, err
		}
	}
	o, err := v.ReadView.Read(q)
	v.check(err)
	return o, err
}

// GroundStoreTarget checks a store query's target against the accepted
// application map and the snapshot, as the controller does for a store action:
// the component declares data with this engine, the Pod belongs to its
// workload, and the named container runs the engine's image.
func GroundStoreTarget(v ReadView, t StoreTarget) error {
	m, err := v.AppMap()
	if err != nil {
		return fmt.Errorf("store read refused: %w", err)
	}
	component, ok := m.Component(t.Component)
	switch {
	case !ok:
		return fmt.Errorf("store read refused: the accepted application map has no component %q", t.Component)
	case component.Data == nil || component.Data.Engine != t.Engine:
		return fmt.Errorf("store read refused: component %s declares no %s data in the accepted application map", t.Component, t.Engine)
	case WorkloadOf(v, t.Pod) != component.Workload:
		return fmt.Errorf("store read refused: Pod %s does not belong to component %s", t.Pod.Name, t.Component)
	}
	image := regexp.MustCompile(t.EngineOps().Image())
	for _, pod := range v.Pods() {
		if pod.Namespace != t.Pod.Namespace || pod.Name != t.Pod.Name {
			continue
		}
		for _, container := range pod.Spec.Containers {
			if container.Name == t.Container && image.MatchString(container.Image) {
				return nil
			}
		}
		return fmt.Errorf("store read refused: Pod %s has no %s container running a %s image", t.Pod.Name, t.Container, t.Engine)
	}
	return fmt.Errorf("store read refused: Pod %s is absent from the snapshot", t.Pod.Name)
}

// MaxProbeBodyBytes bounds the response body a probe query reads.
const MaxProbeBodyBytes = 64 << 10

// ExtractJSONFields reads the scalars at the pointers from a JSON body and
// drops the body. A pointer to nothing leaves the field out of the row; a
// pointer to an array or object is an error.
func ExtractJSONFields(body []byte, fields map[string]string) (Row, error) {
	var root any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, fmt.Errorf("probe response is not JSON")
	}
	row := Row{}
	for name, pointer := range fields {
		value, found := root, true
		for _, segment := range strings.Split(pointer[1:], "/") {
			segment = strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")
			switch node := value.(type) {
			case map[string]any:
				value, found = node[segment]
			case []any:
				i, err := strconv.Atoi(segment)
				if err != nil || i < 0 || i >= len(node) {
					found = false
				} else {
					value = node[i]
				}
			default:
				found = false
			}
			if !found {
				break
			}
		}
		if !found {
			continue
		}
		switch v := value.(type) {
		case json.Number:
			if i, err := v.Int64(); err == nil {
				row[name] = i
			} else if f, err := v.Float64(); err == nil {
				row[name] = f
			}
		case nil, bool, string:
			row[name] = v
		default:
			return nil, fmt.Errorf("probe field %q is an array or object; a read returns scalars", name)
		}
	}
	return row.normalize()
}

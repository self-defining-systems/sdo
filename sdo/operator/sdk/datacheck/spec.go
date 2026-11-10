// Package datacheck checks the records a data store holds: a bounded,
// read-only sample of one store, read with the store's own CLI inside its own
// container through the podread path, and assertions of a closed set of kinds
// over it. A check is data, never code: the detection designer writes it as a
// YAML file, and Go builds the read from it, so a designer can name a
// collection, a key pattern or a filter but cannot write a command.
//
// Assertion kinds:
//   - shape: sampled records have the required fields with the required types;
//   - bounds: numeric fields, or a collection's member count, stay in a range;
//   - reference: every value of a field resolves to a record of a second store;
//   - equivalence: a derived copy (a cache entry) equals its source record on
//     the selected fields.
//
// Reference and equivalence read the second store as a podread lookup that
// runs in the same collection cycle with the keys the first read printed.
// Every read is capped (records, members, value bytes, output) and an
// unavailable or incomplete read is a detector error, never a clear.
package datacheck

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/sdk/podread"
)

// Engine names a store engine; the values match the application map's
// data.engine.
type Engine string

// Engines a data check can read.
const (
	EngineRedis     Engine = "redis"
	EngineMongoDB   Engine = "mongodb"
	EnginePostgres  Engine = "postgres"
	EngineMemcached Engine = "memcached"
)

// Engines lists the engines a data check can read.
func Engines() []Engine {
	return []Engine{EngineMemcached, EngineMongoDB, EnginePostgres, EngineRedis}
}

// Bounds of one check.
const (
	// MaxRecords bounds the records one read samples.
	MaxRecords = 32
	// MaxMembers bounds the members of one collection value a read returns.
	MaxMembers = 64
	// MaxAssertions bounds the assertions of one check.
	MaxAssertions = 16
	// MaxFields bounds the distinct top-level fields one read projects.
	MaxFields = 16
	// MinInterval is the shortest interval between two reads of a check.
	MinInterval = time.Minute
	// MaxInterval is the longest interval between two reads of a check.
	MaxInterval = time.Hour
)

// Shape types.
const (
	TypeString  = "string"
	TypeNumber  = "number"
	TypeInteger = "integer"
	// TypeDigits is an integer, or a string of decimal digits: an id however
	// the store spells it.
	TypeDigits  = "digits"
	TypeBoolean = "boolean"
	TypeObject  = "object"
	TypeArray   = "array"
	TypeNull    = "null"
	TypeAny     = "any"
)

var shapeTypes = []string{TypeAny, TypeArray, TypeBoolean, TypeDigits, TypeInteger, TypeNull, TypeNumber, TypeObject, TypeString}

// Redis value types.
var redisTypes = []string{"hash", "list", "set", "string", "zset"}

// Check is one data check: a sampled read of one store and assertions over
// the sample. Its ID is its file's stem.
type Check struct {
	Store `yaml:",inline"`
	// Lookup is the second store a reference or equivalence assertion reads.
	Lookup *Store `yaml:"lookup,omitempty"`
	// MinRecords is the fewest records the read must find; default 1. A cache
	// that may be empty sets 0.
	MinRecords *int `yaml:"min_records,omitempty"`
	// Interval between reads; default MinInterval.
	Interval string      `yaml:"interval,omitempty"`
	Assert   []Assertion `yaml:"assert"`
}

// Store names one store component, the Pods that run it and what to read.
type Store struct {
	Component string `yaml:"component"`
	Engine    Engine `yaml:"engine"`
	Target    Target `yaml:"target"`
	// Port is where the store listens in its container; 0 is the default.
	Port int  `yaml:"port,omitempty"`
	Read Read `yaml:"read"`
}

// Target selects the store's Pods by labels and its container by image.
type Target struct {
	Labels map[string]string `yaml:"labels,omitempty"`
	Image  string            `yaml:"image,omitempty"`
}

// Read says which records to read. Database, Collection, Key, KeyType and
// Filter are for mongodb; Database, Table, Key, KeyType and Where for
// postgres; Pattern, Type and Decode for redis; Pattern and Decode for
// memcached. Decode reads a string value as text (the field value), json
// (the fields of one JSON document) or ndjson (one JSON document per line,
// as the list lines: lines[].field is each line's field). A memcached read
// lists its keys for first with lru_crawler metadump. The first store selects records with exactly one of IDs, Range
// or First; a lookup selects by the keys the first store printed.
type Read struct {
	Database   string         `yaml:"database,omitempty"`
	Collection string         `yaml:"collection,omitempty"`
	Table      string         `yaml:"table,omitempty"`
	Key        string         `yaml:"key,omitempty"`
	KeyType    string         `yaml:"key_type,omitempty"`
	Filter     map[string]any `yaml:"filter,omitempty"`
	Where      string         `yaml:"where,omitempty"`
	Pattern    string         `yaml:"pattern,omitempty"`
	Type       string         `yaml:"type,omitempty"`
	Decode     string         `yaml:"decode,omitempty"`
	IDs        []string       `yaml:"ids,omitempty"`
	Range      *Range         `yaml:"range,omitempty"`
	First      int            `yaml:"first,omitempty"`
}

// Range selects the integer ids From through To.
type Range struct {
	From int64 `yaml:"from"`
	To   int64 `yaml:"to"`
}

// Assertion is exactly one assertion kind.
type Assertion struct {
	// Shape maps a field path to its required type.
	Shape map[string]string `yaml:"shape,omitempty"`
	// Bounds requires a numeric field, or a member count, in a range.
	Bounds *Bounds `yaml:"bounds,omitempty"`
	// Reference requires every value of a field to resolve in the lookup.
	Reference *Reference `yaml:"reference,omitempty"`
	// Equivalence requires each record to equal its lookup record on the
	// listed field pairs.
	Equivalence []FieldPair `yaml:"equivalence,omitempty"`
}

// Bounds is an inclusive range. Count judges the number of elements of an
// array field instead of its value.
type Bounds struct {
	Field string   `yaml:"field"`
	Min   *float64 `yaml:"min,omitempty"`
	Max   *float64 `yaml:"max,omitempty"`
	Count bool     `yaml:"count,omitempty"`
}

// Reference names the field whose values are keys of the lookup store.
type Reference struct {
	Field string `yaml:"field"`
}

// FieldPair compares a field of the record with a field of its source.
type FieldPair struct {
	Field  string `yaml:"field"`
	Source string `yaml:"source"`
}

// Kind of an assertion.
const (
	KindShape       = "shape"
	KindBounds      = "bounds"
	KindReference   = "reference"
	KindEquivalence = "equivalence"
)

// Kind returns the assertion's kind, or "" when it sets none or several.
func (a Assertion) Kind() string {
	var kinds []string
	if len(a.Shape) > 0 {
		kinds = append(kinds, KindShape)
	}
	if a.Bounds != nil {
		kinds = append(kinds, KindBounds)
	}
	if a.Reference != nil {
		kinds = append(kinds, KindReference)
	}
	if len(a.Equivalence) > 0 {
		kinds = append(kinds, KindEquivalence)
	}
	if len(kinds) != 1 {
		return ""
	}
	return kinds[0]
}

// Parse decodes a check strictly: unknown fields are errors.
func Parse(data []byte) (Check, error) {
	var c Check
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&c); err != nil {
		return Check{}, fmt.Errorf("data check does not parse: %w", err)
	}
	return c, nil
}

var (
	idPattern         = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
	namePattern       = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$`)
	identPattern      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
	tablePattern      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}(\.[A-Za-z_][A-Za-z0-9_]{0,62})?$`)
	pathPattern       = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}(\[\])?(\.[A-Za-z_][A-Za-z0-9_-]{0,63}(\[\])?){0,7}$`)
	patternLiteral    = regexp.MustCompile(`^[A-Za-z0-9_.:@/+=-]*$`)
	labelValuePattern = regexp.MustCompile(`^[A-Za-z0-9_./-]{0,253}$`)
)

// ValidID reports whether id names a data check.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// Validate checks the check against its schema, the read-only allowlist of
// its engine and the bounds. It names every problem it finds.
func (c Check) Validate(id string) error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if !ValidID(id) {
		add("data check id %q must be lowercase letters, digits and dashes, at most 48", id)
	}
	if _, err := c.interval(); err != nil {
		add("%v", err)
	}
	if c.MinRecords != nil && (*c.MinRecords < 0 || *c.MinRecords > MaxRecords) {
		add("min_records must be 0 to %d", MaxRecords)
	}
	errs = append(errs, c.Store.validate("", true)...)
	if c.Lookup != nil {
		errs = append(errs, c.Lookup.validate("lookup ", false)...)
		if c.Lookup.Component == c.Component {
			add("lookup reads the checked component %s again; name the other store", c.Component)
		}
	}
	errs = append(errs, c.validateAssertions()...)
	return errors.Join(errs...)
}

func (c Check) interval() (time.Duration, error) {
	if c.Interval == "" {
		return MinInterval, nil
	}
	d, err := time.ParseDuration(c.Interval)
	if err != nil || d < MinInterval || d > MaxInterval {
		return 0, fmt.Errorf("interval %q must be a duration from %s to %s", c.Interval, MinInterval, MaxInterval)
	}
	return d, nil
}

// ReadInterval is the interval between two reads of a valid check.
func (c Check) ReadInterval() time.Duration {
	d, err := c.interval()
	if err != nil {
		return MinInterval
	}
	return d
}

func (c Check) minRecords() int {
	if c.MinRecords == nil {
		return 1
	}
	return *c.MinRecords
}

func (s Store) validate(prefix string, subject bool) []error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(prefix+format, args...)) }
	if s.Component == "" {
		add("component is required: the application-map component of the store")
	}
	if !slices.Contains(Engines(), s.Engine) {
		add("engine %q is not one of %v", s.Engine, Engines())
	}
	if len(s.Target.Labels) == 0 && strings.TrimSpace(s.Target.Image) == "" {
		add("target needs labels or an image pattern that select the store's Pods")
	}
	for k, v := range s.Target.Labels {
		if k == "" || !labelValuePattern.MatchString(k) || !labelValuePattern.MatchString(v) {
			add("target label %q=%q is not a label", k, v)
		}
	}
	if s.Target.Image != "" {
		if _, err := regexp.Compile(s.Target.Image); err != nil {
			add("target image: %v", err)
		}
	}
	if s.Port < 0 || s.Port > 65535 {
		add("port %d is out of range", s.Port)
	}
	r := s.Read
	selections := 0
	if len(r.IDs) > 0 {
		selections++
	}
	if r.Range != nil {
		selections++
	}
	if r.First != 0 {
		selections++
	}
	switch {
	case subject && selections != 1:
		add("read selects records with exactly one of ids, range or first")
	case !subject && selections != 0:
		add("read of a lookup takes its keys from the checked records; remove ids, range and first")
	}
	if len(r.IDs) > MaxRecords {
		add("ids lists %d records, more than %d", len(r.IDs), MaxRecords)
	}
	for _, id := range r.IDs {
		if !podread.ValidKey(id) {
			add("id %q is not a key: letters, digits and _.:@/+=- only", id)
		}
	}
	if r.Range != nil && (r.Range.From < 0 || r.Range.To < r.Range.From || r.Range.To-r.Range.From >= MaxRecords) {
		add("range must be from <= to, non-negative, at most %d ids", MaxRecords)
	}
	if r.First < 0 || r.First > MaxRecords {
		add("first must be 1 to %d", MaxRecords)
	}
	if r.KeyType != "" && r.KeyType != "string" && r.KeyType != "number" {
		add("key_type is string or number")
	}
	if r.Decode != "" && r.Decode != "text" && r.Decode != "json" && r.Decode != "ndjson" {
		add("decode is text, json or ndjson")
	}
	field := func(name, value string) {
		if value != "" {
			add("%s is not a read field of engine %s", name, s.Engine)
		}
	}
	switch s.Engine {
	case EngineMongoDB:
		if !namePattern.MatchString(r.Database) || !namePattern.MatchString(r.Collection) {
			add("mongodb read needs a database and a collection (letters, digits and _.-)")
		}
		if r.Key != "" && !pathPattern.MatchString(r.Key) || strings.Contains(r.Key, "[]") {
			add("key %q is not a field path", r.Key)
		}
		if err := validateFilter(r.Filter, 0); err != nil {
			add("filter: %v", err)
		}
		field("table", r.Table)
		field("where", r.Where)
		field("pattern", r.Pattern)
		field("type", r.Type)
		field("decode", r.Decode)
	case EnginePostgres:
		if r.Database != "" && !identPattern.MatchString(r.Database) {
			add("database %q is not an identifier", r.Database)
		}
		if !tablePattern.MatchString(r.Table) {
			add("postgres read needs a table: [schema.]name")
		}
		if !identPattern.MatchString(r.Key) {
			add("postgres read needs key: the column that identifies a record")
		}
		if r.Where != "" {
			if err := validateWhere(r.Where); err != nil {
				add("where: %v", err)
			}
		}
		if r.Filter != nil {
			add("filter is not a read field of engine postgres; use where")
		}
		field("collection", r.Collection)
		field("pattern", r.Pattern)
		field("type", r.Type)
		field("decode", r.Decode)
	case EngineRedis, EngineMemcached:
		if err := validatePattern(r.Pattern); err != nil {
			add("%v", err)
		}
		if s.Engine == EngineRedis && !slices.Contains(redisTypes, r.Type) {
			add("redis read needs type: one of %v", redisTypes)
		}
		if s.Engine == EngineMemcached {
			field("type", r.Type)
			field("key_type", r.KeyType)
		}
		if r.Decode != "" && s.Engine == EngineRedis && r.Type != "string" {
			add("decode applies to string values only")
		}
		if r.Filter != nil {
			add("filter is not a read field of engine %s", s.Engine)
		}
		field("database", r.Database)
		field("collection", r.Collection)
		field("table", r.Table)
		field("key", r.Key)
		field("where", r.Where)
	}
	return errs
}

func validatePattern(pattern string) error {
	before, after, found := strings.Cut(pattern, "{id}")
	if !found || strings.Contains(after, "{id}") || !patternLiteral.MatchString(before) || !patternLiteral.MatchString(after) {
		return fmt.Errorf("pattern %q must hold {id} once, around letters, digits and _.:@/+=- only", pattern)
	}
	return nil
}

// mongoOperators are the query operators a filter may use: comparisons and
// logic only. $where, $expr, $function and every operator that runs code or
// reads other collections are refused.
var mongoOperators = []string{
	"$all", "$and", "$elemMatch", "$eq", "$exists", "$gt", "$gte", "$in", "$lt", "$lte", "$mod",
	"$ne", "$nin", "$nor", "$not", "$options", "$or", "$regex", "$size", "$type",
}

func validateFilter(v any, depth int) error {
	if depth > 8 {
		return fmt.Errorf("nested deeper than 8")
	}
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if strings.HasPrefix(k, "$") {
				if !slices.Contains(mongoOperators, k) {
					return fmt.Errorf("operator %s is not allowed; allowed: %v", k, mongoOperators)
				}
			} else if !pathPattern.MatchString(k) || strings.Contains(k, "[]") {
				return fmt.Errorf("field %q is not a field path", k)
			}
			if err := validateFilter(child, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range x {
			if err := validateFilter(child, depth+1); err != nil {
				return err
			}
		}
	case nil, string, bool, int, int64, uint64, float64:
	default:
		return fmt.Errorf("value of type %T is not allowed", v)
	}
	return nil
}

func (c Check) validateAssertions() []error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if len(c.Assert) == 0 || len(c.Assert) > MaxAssertions {
		add("assert lists 1 to %d assertions", MaxAssertions)
	}
	cross := 0
	path := func(where, p string) bool {
		if !pathPattern.MatchString(p) {
			add("%s: %q is not a field path (name, name.child, list[] for each element)", where, p)
			return false
		}
		return true
	}
	for i, a := range c.Assert {
		where := fmt.Sprintf("assert[%d]", i)
		switch a.Kind() {
		case KindShape:
			for p, t := range a.Shape {
				if path(where, p) && !slices.Contains(shapeTypes, t) {
					add("%s: type %q of %s is not one of %v", where, t, p, shapeTypes)
				}
			}
		case KindBounds:
			b := a.Bounds
			path(where, b.Field)
			if b.Min == nil && b.Max == nil {
				add("%s: bounds need min, max or both", where)
			}
			if b.Min != nil && b.Max != nil && *b.Min > *b.Max {
				add("%s: min is above max", where)
			}
		case KindReference:
			cross++
			if path(where, a.Reference.Field) {
				if err := c.Store.extractable(a.Reference.Field); err != nil {
					add("%s: %v", where, err)
				}
			}
		case KindEquivalence:
			cross++
			for _, pair := range a.Equivalence {
				path(where, pair.Field)
				path(where, pair.Source)
				if strings.Contains(pair.Field, "[]") != strings.Contains(pair.Source, "[]") {
					add("%s: %s and %s compare a set with a single value", where, pair.Field, pair.Source)
				}
			}
		default:
			add("%s sets exactly one of shape, bounds, reference, equivalence", where)
		}
	}
	switch {
	case cross > 1:
		add("a check holds at most one reference or equivalence assertion; write another check for the next one")
	case cross == 1 && c.Lookup == nil:
		add("reference and equivalence read a second store: declare lookup")
	case cross == 0 && c.Lookup != nil:
		add("lookup is read only by a reference or equivalence assertion")
	}
	for _, s := range c.stores() {
		if fields := s.fields(c.pathsOf(s)); len(fields) > MaxFields {
			add("%s reads %d fields, more than %d", s.Component, len(fields), MaxFields)
		} else if err := s.readable(c.pathsOf(s)); err != nil {
			add("%v", err)
		}
	}
	return errs
}

func (c Check) stores() []Store {
	if c.Lookup == nil {
		return []Store{c.Store}
	}
	return []Store{c.Store, *c.Lookup}
}

// pathsOf lists the field paths the check reads from s.
func (c Check) pathsOf(s Store) []string {
	var paths []string
	if c.Lookup != nil && s.Component == c.Lookup.Component {
		for _, a := range c.Assert {
			for _, pair := range a.Equivalence {
				paths = append(paths, pair.Source)
			}
		}
		return paths
	}
	for _, a := range c.Assert {
		for p := range a.Shape {
			paths = append(paths, p)
		}
		if a.Bounds != nil {
			paths = append(paths, a.Bounds.Field)
		}
		if a.Reference != nil {
			paths = append(paths, a.Reference.Field)
		}
		for _, pair := range a.Equivalence {
			paths = append(paths, pair.Field)
		}
	}
	return paths
}

// fields lists the distinct top-level fields of paths, sorted.
func (s Store) fields(paths []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, p := range paths {
		top := strings.TrimSuffix(strings.SplitN(p, ".", 2)[0], "[]")
		if !seen[top] {
			seen[top] = true
			out = append(out, top)
		}
	}
	sort.Strings(out)
	return out
}

// readable reports a path the store's record shape cannot hold.
func (s Store) readable(paths []string) error {
	collection := s.Engine == EngineRedis && slices.Contains([]string{"set", "zset", "list"}, s.Read.Type)
	stringValue := s.Engine == EngineRedis && s.Read.Type == "string" || s.Engine == EngineMemcached
	text := stringValue && s.Read.Decode != "json" && s.Read.Decode != "ndjson"
	lines := stringValue && s.Read.Decode == "ndjson"
	for _, p := range paths {
		top := strings.SplitN(p, ".", 2)[0]
		switch {
		case collection && !slices.Contains([]string{"members", "members[]", "size"}, p):
			return fmt.Errorf("%s: a redis %s record has members, members[] and size only, not %s", s.Component, s.Read.Type, p)
		case text && p != "value":
			return fmt.Errorf("%s: a text value is the field value; decode: json reads fields of a JSON value, not %s", s.Component, p)
		case lines && top != "lines" && top != "lines[]":
			return fmt.Errorf("%s: an ndjson value is the list lines (lines[].field for each line's field), not %s", s.Component, p)
		case s.Engine == EngineRedis && s.Read.Type == "hash" && p != top:
			return fmt.Errorf("%s: a redis hash record has one level of fields, not %s", s.Component, p)
		case s.Engine == EnginePostgres && strings.Contains(p, "."):
			return fmt.Errorf("%s: a postgres record has columns, not %s", s.Component, p)
		}
	}
	return nil
}

// extractable reports whether the store's read can print the keys of a
// reference field inside the store's container.
func (s Store) extractable(field string) error {
	switch {
	case s.Engine == EngineMemcached:
		return fmt.Errorf("a memcached value cannot be decoded in its container; reference from the store that holds the field")
	case s.Engine == EnginePostgres && strings.Count(field, "[]") > 0 && !strings.HasSuffix(field, "[]"):
		return fmt.Errorf("a postgres reference is a column or an array column[]")
	}
	return nil
}

// FilterJSON is the mongodb filter as JSON, for a read and for a reader.
func (r Read) FilterJSON() (string, error) {
	if r.Filter == nil {
		return "{}", nil
	}
	data, err := json.Marshal(r.Filter)
	return string(data), err
}

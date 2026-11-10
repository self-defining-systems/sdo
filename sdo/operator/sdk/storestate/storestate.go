// Package storestate reads value-free facts about a data store: whether it
// accepts writes and why not, its replication role, its connection and memory
// caps, its principals and their grants, its catalog (indexes, validators,
// locks) and inventory counts. It never reads records.
//
// An engine Adapter supplies one read command, run inside the store's own
// container through the podread path with the store's own CLI, and the parser
// of its output. Facts from a healthy moment form a baseline; Diff lists the
// items that differ now, so a finding names the exact setting, role or grant
// that changed. A read that gives no answer is an error, never a healthy
// result.
package storestate

import (
	"fmt"
	"sort"
	"time"

	"sdo.dev/operator/sdk"
)

// Engine names a store engine; the values match the application map's
// data.engine.
type Engine string

// Known engines. Only engines with an Adapter can be read.
const (
	EnginePostgres  Engine = "postgres"
	EngineMySQL     Engine = "mysql"
	EngineMongoDB   Engine = "mongodb"
	EngineRedis     Engine = "redis"
	EngineMemcached Engine = "memcached"
	EngineConsul    Engine = "consul"
)

// ItemKind classifies a fact. The set is closed; each engine uses a subset.
type ItemKind string

// Item kinds.
const (
	// KindSetting is a runtime setting and its value.
	KindSetting ItemKind = "setting"
	// KindWriteBlock is an active reason the store refuses writes; Value is "active".
	KindWriteBlock ItemKind = "write-block"
	// KindRole is the replication role.
	KindRole ItemKind = "role"
	// KindPrincipal is a user or credential requirement: its flags and grants,
	// never a secret.
	KindPrincipal ItemKind = "principal"
	// KindNamespace is an inventory count of a database, collection or keyspace.
	KindNamespace ItemKind = "namespace"
	// KindIndex is an index and a digest of its definition.
	KindIndex ItemKind = "index"
	// KindValidator is a schema validator and a digest of its text.
	KindValidator ItemKind = "validator"
	// KindLock is a lock that blocks writers and its count.
	KindLock ItemKind = "lock"
	// KindKeyPattern is a count of keys or documents of one pattern.
	KindKeyPattern ItemKind = "key-pattern"
	// KindRegistryEntry is a count of registered entries.
	KindRegistryEntry ItemKind = "registry-entry"
	// KindRefusal is the store refusing one of SDO's own reads; Value is the
	// class (auth-required, no-permission, max-clients, loading, unsupported,
	// refused).
	KindRefusal ItemKind = "refusal"
)

var knownKinds = map[ItemKind]bool{
	KindSetting: true, KindWriteBlock: true, KindRole: true, KindPrincipal: true, KindNamespace: true,
	KindIndex: true, KindValidator: true, KindLock: true, KindKeyPattern: true, KindRegistryEntry: true, KindRefusal: true,
}

// Bounds of one Facts value.
const (
	MaxItems     = 512
	MaxItemBytes = 64 * 1024
)

// Key identifies an item across two Facts.
type Key struct {
	Kind  ItemKind `json:"kind"`
	Scope string   `json:"scope,omitempty"`
	Name  string   `json:"name"`
}

// Less orders keys by kind, scope and name.
func (k Key) Less(o Key) bool {
	if k.Kind != o.Kind {
		return k.Kind < o.Kind
	}
	if k.Scope != o.Scope {
		return k.Scope < o.Scope
	}
	return k.Name < o.Name
}

func (k Key) String() string {
	if k.Scope == "" {
		return string(k.Kind) + " " + k.Name
	}
	return string(k.Kind) + " " + k.Scope + "/" + k.Name
}

// Item is one value-free fact. Scope is the database, collection or keyspace,
// or empty for the server. Value is a setting value, a count, a digest, a flag
// list or "active"; it is never a record or a secret.
type Item struct {
	Kind  ItemKind `json:"kind"`
	Scope string   `json:"scope,omitempty"`
	Name  string   `json:"name"`
	Value string   `json:"value"`
}

// Key returns the item's identity.
func (i Item) Key() Key { return Key{Kind: i.Kind, Scope: i.Scope, Name: i.Name} }

// Less orders items by key.
func (i Item) Less(o Item) bool { return i.Key().Less(o.Key()) }

// Gauge is current use against a cap; Max 0 means no cap.
type Gauge struct {
	Used int64 `json:"used"`
	Max  int64 `json:"max"`
}

// Near reports use at or above fraction of a set cap.
func (g Gauge) Near(fraction float64) bool {
	return g.Max > 0 && float64(g.Used) >= fraction*float64(g.Max)
}

// Facts is one read of one store instance. Writable, Connections and Memory
// are nil when the read could not establish them. Error marks an unavailable
// read; such Facts are never healthy. Unread lists the kinds the store refused
// to show (a KindRefusal item says why): their items may be missing, and a
// missing one is unknown, not removed.
type Facts struct {
	Engine      Engine        `json:"engine"`
	Component   string        `json:"component,omitempty"`
	Pod         sdk.ObjectRef `json:"pod,omitzero"`
	At          time.Time     `json:"at,omitzero"`
	Writable    *bool         `json:"writable,omitempty"`
	Connections *Gauge        `json:"connections,omitempty"`
	Memory      *Gauge        `json:"memory,omitempty"`
	Items       []Item        `json:"items"`
	// Holders groups the connections the store holds by peer and state; nil
	// when the read could not list them. It is never part of Diff.
	Holders []Holder   `json:"holders,omitempty"`
	Unread  []ItemKind `json:"unread,omitempty"`
	Error   string     `json:"error,omitempty"`
}

// unread reports whether items of kind may be missing from f.
func (f Facts) unread(kind ItemKind) bool {
	for _, k := range f.Unread {
		if k == kind {
			return true
		}
	}
	return false
}

// NewFacts returns facts whose items are sorted and unique by key (the last
// item of a key wins).
func NewFacts(engine Engine, items []Item) Facts {
	byKey := map[Key]Item{}
	for _, it := range items {
		byKey[it.Key()] = it
	}
	out := make([]Item, 0, len(byKey))
	for _, it := range byKey {
		out = append(out, it)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Less(out[j]) })
	return Facts{Engine: engine, Items: out}
}

// Validate checks the item bounds, kinds, order and uniqueness.
func (f Facts) Validate() error {
	if len(f.Items) > MaxItems {
		return fmt.Errorf("store facts carry %d items, more than %d", len(f.Items), MaxItems)
	}
	size := 0
	for i, it := range f.Items {
		if !knownKinds[it.Kind] {
			return fmt.Errorf("store fact %s: unknown kind", it.Key())
		}
		if it.Name == "" {
			return fmt.Errorf("store fact of kind %s has no name", it.Kind)
		}
		if i > 0 && !f.Items[i-1].Less(it) {
			return fmt.Errorf("store facts are not sorted and unique at %s", it.Key())
		}
		size += len(it.Scope) + len(it.Name) + len(it.Value)
	}
	for _, k := range f.Unread {
		if !knownKinds[k] {
			return fmt.Errorf("store facts mark unknown kind %q unread", k)
		}
	}
	if len(f.Holders) > MaxHolders {
		return fmt.Errorf("store facts carry %d holder groups, more than %d", len(f.Holders), MaxHolders)
	}
	if size > MaxItemBytes {
		return fmt.Errorf("store facts carry %d bytes, more than %d", size, MaxItemBytes)
	}
	return nil
}

// Lookup returns the item with key k.
func (f Facts) Lookup(k Key) (Item, bool) {
	i := sort.Search(len(f.Items), func(i int) bool { return !f.Items[i].Key().Less(k) })
	if i < len(f.Items) && f.Items[i].Key() == k {
		return f.Items[i], true
	}
	return Item{}, false
}

// Change is one item that differs between a baseline and the current facts.
// Before is nil for an added item and After is nil for a removed one. A
// restore puts Before back.
type Change struct {
	Before *Item `json:"before,omitempty"`
	After  *Item `json:"after,omitempty"`
}

// Key returns the changed item's identity.
func (c Change) Key() Key {
	if c.After != nil {
		return c.After.Key()
	}
	if c.Before != nil {
		return c.Before.Key()
	}
	return Key{}
}

func (c Change) String() string {
	before, after := "(absent)", "(absent)"
	if c.Before != nil {
		before = c.Before.Value
	}
	if c.After != nil {
		after = c.After.Value
	}
	return fmt.Sprintf("%s %s -> %s", c.Key(), before, after)
}

// Diff lists the items added, removed or changed from baseline to current,
// sorted by key. Diff of facts with themselves is empty. An item missing from
// current is not reported as removed when current could not read its kind.
func Diff(baseline, current Facts) []Change {
	var out []Change
	i, j := 0, 0
	for i < len(baseline.Items) || j < len(current.Items) {
		switch {
		case j == len(current.Items) || (i < len(baseline.Items) && baseline.Items[i].Less(current.Items[j])):
			before := baseline.Items[i]
			if !current.unread(before.Kind) {
				out = append(out, Change{Before: &before})
			}
			i++
		case i == len(baseline.Items) || current.Items[j].Less(baseline.Items[i]):
			after := current.Items[j]
			out = append(out, Change{After: &after})
			j++
		default:
			if baseline.Items[i].Value != current.Items[j].Value {
				before, after := baseline.Items[i], current.Items[j]
				out = append(out, Change{Before: &before, After: &after})
			}
			i++
			j++
		}
	}
	return out
}

// Apply returns facts with each change's After in place of its key: Apply(a,
// Diff(a, b)) has the items of b.
func Apply(f Facts, changes []Change) Facts {
	byKey := map[Key]Item{}
	for _, it := range f.Items {
		byKey[it.Key()] = it
	}
	for _, c := range changes {
		if c.After == nil {
			delete(byKey, c.Key())
			continue
		}
		byKey[c.Key()] = *c.After
	}
	items := make([]Item, 0, len(byKey))
	for _, it := range byKey {
		items = append(items, it)
	}
	out := NewFacts(f.Engine, items)
	out.Component, out.Pod, out.At = f.Component, f.Pod, f.At
	return out
}

// Endpoint is where the store listens inside its own container.
type Endpoint struct {
	Port int
}

// Adapter is one engine's read: an argv run in the store's container (its own
// CLI, credentials from the container environment, output never carrying a
// secret or a record) and the parser of its output. The read is one command so
// a store costs one pod read.
type Adapter interface {
	Engine() Engine
	DefaultPort() int
	ReadCommand(Endpoint) []string
	// Parse returns the facts of a complete read. A read that reached no
	// server, or lacks a section, is an error.
	Parse(stdout string) (Facts, error)
}

var adapters = map[Engine]Adapter{
	EngineRedis: redisAdapter{}, EngineMongoDB: mongoAdapter{}, EnginePostgres: postgresAdapter{},
	EngineMySQL: mysqlAdapter{}, EngineMemcached: memcachedAdapter{}, EngineConsul: consulAdapter{},
}

// AdapterFor returns the engine's adapter, or false when SDO has no reader for
// the engine yet.
func AdapterFor(engine Engine) (Adapter, bool) {
	a, ok := adapters[engine]
	return a, ok
}

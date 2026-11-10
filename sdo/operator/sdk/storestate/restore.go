package storestate

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// RestoredMarker is the line a restore command prints once the store accepted
// every statement of the step.
const RestoredMarker = "#sdo-restored"

// Restorer is an engine's closed inverse table: the commands that put one
// changed item back to its healthy baseline value. Each command runs in the
// store's own container through the same credential prelude as the read, and
// prints RestoredMarker when the store accepted it. A restore never carries a
// credential: a password is restored only from the one the store container's
// own environment already holds (the Secret the workload uses), or not at all.
//
// An adapter of another engine implements it to make its items restorable.
type Restorer interface {
	// Restore returns the commands, run in order, that put change.Before back
	// (remove the item when Before is nil). baseline is the whole healthy
	// baseline the change was taken against, for items that follow from
	// others (a write block from its setting). An item the table cannot
	// restore exactly is a *NotRestorableError.
	Restore(e Endpoint, change Change, baseline Facts, options RestoreOptions) ([][]string, error)
}

// RestoreOptions are the inputs of a restore beyond the change.
type RestoreOptions struct {
	// Payload looks up the controller-held restore text of a baseline item.
	Payload PayloadLookup
	// EndSessions also ends the store sessions a restored per-database or
	// per-role setting applies to, so their clients reconnect with it; a
	// session keeps the value it started with otherwise.
	EndSessions bool
}

// NotRestorableError says why an item has no typed restore.
type NotRestorableError struct {
	Key    Key
	Reason string
}

func (e *NotRestorableError) Error() string {
	return fmt.Sprintf("%s is not restorable: %s", e.Key, e.Reason)
}

// IsNotRestorable reports whether err says the item has no typed restore.
func IsNotRestorable(err error) bool {
	var target *NotRestorableError
	return errors.As(err, &target)
}

func notRestorable(key Key, format string, args ...any) error {
	return &NotRestorableError{Key: key, Reason: fmt.Sprintf(format, args...)}
}

// RestoreCommands returns the engine commands that restore change, or an
// error when the engine has no restore table or the item none.
func RestoreCommands(engine Engine, e Endpoint, change Change, baseline Facts, options RestoreOptions) ([][]string, error) {
	adapter, ok := AdapterFor(engine)
	if !ok {
		return nil, fmt.Errorf("engine %q has no store-state reader", engine)
	}
	restorer, ok := adapter.(Restorer)
	if !ok {
		return nil, notRestorable(change.Key(), "engine %s has no restore table", engine)
	}
	if e.Port == 0 {
		e.Port = adapter.DefaultPort()
	}
	if options.Payload == nil {
		options.Payload = func(Key) (PayloadEntry, bool) { return PayloadEntry{}, false }
	}
	return restorer.Restore(e, change, baseline, options)
}

// Restorable says whether a program may expect a RestoreStoreItem of change
// to run: nil when the engine's table has an inverse for it, assuming the
// controller holds the restore text it needs (captured with the baseline).
// The executor still refuses when that text is missing or no longer matches
// the baseline.
func Restorable(engine Engine, change Change, baseline Facts) error {
	_, err := RestoreCommands(engine, Endpoint{}, change, baseline, RestoreOptions{Payload: func(k Key) (PayloadEntry, bool) {
		value := ""
		if item, ok := baseline.Lookup(k); ok {
			value = item.Value
		} else if change.Before != nil && change.Before.Key() == k {
			value = change.Before.Value
		}
		return PayloadEntry{Key: k, Value: value, Format: PayloadPlaceholder, Text: "{}"}, true
	}})
	return err
}

// Payload formats. A payload is engine text the restore passes back to the
// same engine shell it was captured with.
const (
	// PayloadEJSON is MongoDB canonical Extended JSON (mongosh).
	PayloadEJSON = "ejson"
	// PayloadShell is a legacy mongo shell tojson() expression.
	PayloadShell = "shell"
	// PayloadGrants is an account's MySQL SHOW GRANTS text, its statements
	// joined by the unit separator.
	PayloadGrants = "grants"
	// PayloadPlaceholder stands in for real text in Restorable.
	PayloadPlaceholder = "placeholder"
)

// Bounds of one Payload.
const (
	MaxPayloadEntries    = 512
	MaxPayloadEntryBytes = 64 * 1024
	MaxPayloadBytes      = 1 << 20
)

// PayloadEntry is the full text needed to restore one baseline item whose
// value-free fact cannot be turned back into it: a validator's document, an
// index's specification. Value is the item's fact value computed in the same
// read, so a restore uses the text only while it still describes the baseline
// item. It never holds a credential or a record.
type PayloadEntry struct {
	Key    Key    `json:"key"`
	Value  string `json:"value"`
	Format string `json:"format"`
	Text   string `json:"text"`
}

// Payload is one store Pod's restore text, captured by the controller with
// its promoted healthy baseline and kept with it, outside .sdo/ and out of
// evidence.
type Payload struct {
	Entries []PayloadEntry `json:"entries"`
}

// PayloadLookup returns the restore text of a baseline item.
type PayloadLookup func(Key) (PayloadEntry, bool)

// Lookup returns the entry of key.
func (p Payload) Lookup(k Key) (PayloadEntry, bool) {
	i := sort.Search(len(p.Entries), func(i int) bool { return !p.Entries[i].Key.Less(k) })
	if i < len(p.Entries) && p.Entries[i].Key == k {
		return p.Entries[i], true
	}
	return PayloadEntry{}, false
}

// Validate checks the bounds, order and uniqueness of the entries.
func (p Payload) Validate() error {
	if len(p.Entries) > MaxPayloadEntries {
		return fmt.Errorf("restore payload carries %d entries, more than %d", len(p.Entries), MaxPayloadEntries)
	}
	size := 0
	for i, entry := range p.Entries {
		if entry.Key.Name == "" || !knownKinds[entry.Key.Kind] {
			return fmt.Errorf("restore payload entry %s has no valid key", entry.Key)
		}
		if entry.Format != PayloadEJSON && entry.Format != PayloadShell && entry.Format != PayloadGrants {
			return fmt.Errorf("restore payload entry %s has format %q", entry.Key, entry.Format)
		}
		if len(entry.Text) > MaxPayloadEntryBytes {
			return fmt.Errorf("restore payload entry %s is %d bytes, more than %d", entry.Key, len(entry.Text), MaxPayloadEntryBytes)
		}
		if i > 0 && !p.Entries[i-1].Key.Less(entry.Key) {
			return fmt.Errorf("restore payload entries are not sorted and unique at %s", entry.Key)
		}
		size += len(entry.Text)
	}
	if size > MaxPayloadBytes {
		return fmt.Errorf("restore payload is %d bytes, more than %d", size, MaxPayloadBytes)
	}
	return nil
}

// PayloadReader is an engine whose restore table needs text its value-free
// read does not keep. The controller runs PayloadCommand on each store Pod
// when it promotes the Pod's healthy baseline and keeps the parsed payload
// with that baseline.
type PayloadReader interface {
	PayloadCommand(Endpoint) []string
	ParsePayload(stdout string) (Payload, error)
}

// PayloadReaderFor returns the engine's payload reader, or false when its
// restore table needs no captured text.
func PayloadReaderFor(engine Engine) (PayloadReader, bool) {
	adapter, ok := AdapterFor(engine)
	if !ok {
		return nil, false
	}
	reader, ok := adapter.(PayloadReader)
	return reader, ok
}

// payloadFor returns the payload entry of a baseline item, refusing one that
// is missing or describes another value.
func payloadFor(payload PayloadLookup, item Item) (PayloadEntry, error) {
	entry, ok := payload(item.Key())
	if !ok {
		return PayloadEntry{}, notRestorable(item.Key(), "the controller holds no restore text for it (captured with the healthy baseline); it is captured again at the next baseline promotion")
	}
	if entry.Value != item.Value {
		return PayloadEntry{}, notRestorable(item.Key(), "the restore text held by the controller describes %q, not the baseline value %q", entry.Value, item.Value)
	}
	return entry, nil
}

// fields splits a value-free list value.
func fields(v string) map[string]bool {
	out := map[string]bool{}
	for _, f := range strings.Fields(v) {
		out[f] = true
	}
	return out
}

package program

import (
	"fmt"
	"regexp"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storeops"
	"sdo.dev/operator/sdk/storestate"
)

// StoreTarget selects one data store instance: its application-map component,
// the engine the map declares for it (redis, mongodb or postgres), and the Pod
// and container whose own CLI runs the operation. The controller refuses a
// target the accepted map does not ground: the component must declare data
// with this engine, and the Pod must belong to its workload. Port 0 is the
// engine's default. Credentials come from the container's environment.
type StoreTarget struct {
	Component string        `json:"component"`
	Engine    string        `json:"engine"`
	Pod       sdk.ObjectRef `json:"pod"`
	Container string        `json:"container"`
	Port      int           `json:"port,omitempty"`
}

// DataScope selects records with an engine-native, read-only selector: for
// redis a database number and key glob, for mongodb a database, collection and
// JSON filter, for postgres a database, table and WHERE body. Limit bounds the
// items (0 is 10000, at most 100000); a larger scope is refused.
type DataScope = storeops.Scope

// StoreBackup copies the records of one scope into the controller's backup
// storage (outside .sdo/, on the controller's repository volume, per incident)
// and records its ID, item count, size, SHA-256 and the store's facts at
// backup time. It changes nothing in the store (effect read) and fails when the
// scope holds more than its limit or the dump exceeds the size cap. ID names
// the backup within the incident; it is immutable. A redis backup holds
// DUMP-serialized keys, a mongodb one canonical Extended JSON documents, a
// postgres one COPY rows.
type StoreBackup struct {
	ID     string
	Target StoreTarget
	Scope  DataScope
	Reason string
}

// InvalidateDerived drops the entries of a derived store (a cache) in Scope so
// they are read again from their source (effect derived-invalidate). The
// controller runs it only when the accepted application map marks the
// component data.role derived and every derived_from source passes its live
// store-state checks (it answers, accepts writes and refuses none of SDO's
// reads). Repair the source first: flushing a cache whose source is damaged
// refills it with the damage. The automatic stage may run it under the same
// guard. Only redis supports it today.
type InvalidateDerived struct {
	Target StoreTarget
	Scope  DataScope
	Reason string
}

// RepairData changes the records of one scope with one engine-native
// statement (effect data-write): a redis single-key write per matched key
// ("SREM member"), a mongodb update document or pipeline, a postgres SET
// clause, or "delete". The controller requires Backup to name a StoreBackup
// of this incident whose scope covers Scope, counts the matching items first
// and refuses unless the count is within [ExpectMin, ExpectMax], writes
// (atomically with the count on redis and postgres), then counts again and
// reads the store's facts. It never runs automatically. A backed repair does
// not make the outcome unconfirmed by itself.
type RepairData struct {
	Target    StoreTarget
	Scope     DataScope
	Statement string
	ExpectMin int
	ExpectMax int
	Backup    string
	Reason    string
}

// RestoreBackup puts the records of one StoreBackup of this incident back
// (effect data-write): redis keys are restored with REPLACE, mongodb documents
// upserted by _id, postgres rows replaced by primary key in one transaction.
// Items created since the backup stay, and writes made since the backup to
// the restored items are lost: name them in the outcome. It is a responder
// action only, never automatic. Take a fresh StoreBackup of the same scope
// first; without it the restore is an unbacked write and the outcome is
// unconfirmed.
type RestoreBackup struct {
	Target StoreTarget
	Backup string
	Reason string
}

func (StoreBackup) action()       {}
func (InvalidateDerived) action() {}
func (RepairData) action()        {}
func (RestoreBackup) action()     {}

// StoreBackupRecord is the controller's record of one verified StoreBackup.
// Path is relative to the controller's backup root. Facts is the store's
// value-free state at backup time, absent when the engine has no store-state
// reader; FactsError says why it is absent.
type StoreBackupRecord struct {
	ID         string            `json:"id"`
	IncidentID string            `json:"incident_id"`
	Component  string            `json:"component"`
	Engine     string            `json:"engine"`
	Pod        sdk.ObjectRef     `json:"pod"`
	Scope      DataScope         `json:"scope"`
	Items      int               `json:"items"`
	Bytes      int64             `json:"bytes"`
	SHA256     string            `json:"sha256"`
	Path       string            `json:"path"`
	At         time.Time         `json:"at"`
	Facts      *storestate.Facts `json:"facts,omitempty"`
	FactsError string            `json:"facts_error,omitempty"`
}

// Covers reports whether the backup holds every item a write on target's
// scope can change.
func (r StoreBackupRecord) Covers(target StoreTarget, scope DataScope) bool {
	engine, ok := storeops.For(r.Engine)
	return ok && r.Component == target.Component && r.Engine == target.Engine && storeops.Covers(engine, r.Scope, scope)
}

// DataRepairEffect is the controller's record of one RepairData,
// RestoreBackup or InvalidateDerived: the items the scope matched before, the
// items the store reports changed, the match count after, the backup it
// relied on, and the store's facts afterwards.
type DataRepairEffect struct {
	Component  string            `json:"component"`
	Before     int               `json:"before"`
	Affected   int               `json:"affected"`
	After      int               `json:"after"`
	Backup     string            `json:"backup,omitempty"`
	Facts      *storestate.Facts `json:"facts,omitempty"`
	FactsError string            `json:"facts_error,omitempty"`
}

var backupID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// Validate checks the target's identity and engine.
func (t StoreTarget) Validate() error {
	if t.Component == "" {
		return fmt.Errorf("store target requires an application-map component")
	}
	if _, ok := storeops.For(t.Engine); !ok {
		return fmt.Errorf("store target engine %q has no record operations (%v)", t.Engine, storeops.Engines())
	}
	if err := (Command{Pod: t.Pod, Container: t.Container, Args: []string{"sh"}}).Validate(); err != nil {
		return fmt.Errorf("store target requires a named Pod and container")
	}
	if t.Port < 0 || t.Port > 65535 {
		return fmt.Errorf("store target port %d is out of range", t.Port)
	}
	return nil
}

// EngineOps returns the target engine's record operations.
func (t StoreTarget) EngineOps() storeops.Engine {
	engine, _ := storeops.For(t.Engine)
	return engine
}

func validateBackupID(id string) error {
	if !backupID.MatchString(id) {
		return fmt.Errorf("backup ID %q must be 1-63 lowercase letters, digits or '-'", id)
	}
	return nil
}

func (a StoreBackup) Validate() error {
	if err := validateBackupID(a.ID); err != nil {
		return err
	}
	if err := a.Target.Validate(); err != nil {
		return err
	}
	return a.Target.EngineOps().ValidateScope(a.Scope)
}

func (a InvalidateDerived) Validate() error {
	if err := a.Target.Validate(); err != nil {
		return err
	}
	return a.Target.EngineOps().ValidateScope(a.Scope)
}

func (a RepairData) Validate() error {
	if err := a.Target.Validate(); err != nil {
		return err
	}
	engine := a.Target.EngineOps()
	if err := engine.ValidateScope(a.Scope); err != nil {
		return err
	}
	if err := engine.ValidateStatement(a.Statement); err != nil {
		return err
	}
	if a.ExpectMin < 0 || a.ExpectMax < a.ExpectMin {
		return fmt.Errorf("RepairData requires 0 <= ExpectMin <= ExpectMax, got [%d, %d]", a.ExpectMin, a.ExpectMax)
	}
	if a.ExpectMax == 0 {
		return fmt.Errorf("RepairData with ExpectMax 0 changes nothing")
	}
	if err := validateBackupID(a.Backup); err != nil {
		return fmt.Errorf("RepairData requires the ID of a StoreBackup taken in this incident: %w", err)
	}
	return nil
}

func (a RestoreBackup) Validate() error {
	if err := a.Target.Validate(); err != nil {
		return err
	}
	if err := validateBackupID(a.Backup); err != nil {
		return fmt.Errorf("RestoreBackup requires an explicit backup ID: %w", err)
	}
	return nil
}

// DataAction returns a store action given by value or pointer as its value,
// or nil when action is not one.
func DataAction(action Action) Action {
	switch a := action.(type) {
	case StoreBackup, InvalidateDerived, RepairData, RestoreBackup:
		return a
	case *StoreBackup:
		if a != nil {
			return *a
		}
	case *InvalidateDerived:
		if a != nil {
			return *a
		}
	case *RepairData:
		if a != nil {
			return *a
		}
	case *RestoreBackup:
		if a != nil {
			return *a
		}
	}
	return nil
}

// DataActionEffect is the data effect a store action declares on its target
// component. The executor enforces it: a backup only reads, an invalidation
// only drops a derived store's entries, and a repair or restore changes
// records.
func DataActionEffect(action Action) (StoreTarget, DataEffect, bool) {
	switch a := DataAction(action).(type) {
	case StoreBackup:
		return a.Target, EffectRead, true
	case InvalidateDerived:
		return a.Target, EffectDerivedInvalidate, true
	case RepairData:
		return a.Target, EffectDataWrite, true
	case RestoreBackup:
		return a.Target, EffectDataWrite, true
	}
	return StoreTarget{}, "", false
}

func validateDataAction(action Action) error {
	switch a := DataAction(action).(type) {
	case StoreBackup:
		return a.Validate()
	case InvalidateDerived:
		return a.Validate()
	case RepairData:
		return a.Validate()
	case RestoreBackup:
		return a.Validate()
	}
	switch action.(type) {
	case *StoreBackup, *InvalidateDerived, *RepairData, *RestoreBackup:
		return fmt.Errorf("nil store action")
	}
	return nil
}

package program

import (
	"fmt"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storestate"
)

// RestoreStoreItem puts a store-state item back to its baseline. Its effect
// is store-setting. Change names the item, normally one entry of
// storestate.Changes(view) for the Target Pod; the controller restores the
// value of its own promoted healthy baseline, not the one in Change. It runs
// the engine's fixed inverse (storestate.RestoreCommands): CONFIG SET, ACL
// SETUSER, REPLICAOF NO ONE or CLIENT UNPAUSE on redis; collMod, fsyncUnlock,
// dropIndexes/createIndexes, setParameter or updateUser on mongodb; ALTER
// SYSTEM/DATABASE/ROLE or pg_promote on postgres. A validator or index text
// comes from the restore text the controller captured with the baseline. A
// password is restored only from the store container's own environment.
// The executor reads the item first and records its prior value, does nothing
// when it already has the baseline value, and reads the facts again to verify
// it. EndSessions also ends the sessions a restored per-database or per-role
// postgres setting applies to, so clients reconnect with it.
// storestate.Restorable says beforehand whether an item has an inverse.
type RestoreStoreItem struct {
	Target      StoreTarget
	Change      storestate.Change
	EndSessions bool
	Reason      string
}

// CheckStoreCredential tests a client's credential against a store. It reads
// no data; its effect is read. The controller
// runs one short-lived Pod in the client's namespace with the store
// container's image and the env and envFrom references of Client's Container
// (the kubelet resolves them; SDO never reads the values). It attempts a single
// authentication against Target's Pod IP and reports ok, denied or unreachable
// with a class. Credential names the client's variables; empty names are
// discovered like the store read's own. Denied for a client whose Secret did
// not change means the store's credential changed.
type CheckStoreCredential struct {
	Target     StoreTarget
	Client     sdk.ObjectRef
	Container  string
	Credential storestate.CredentialInputs
	Reason     string
}

func (RestoreStoreItem) action()     {}
func (CheckStoreCredential) action() {}

// StoreRestoreEffect is the controller's record of one RestoreStoreItem: the
// item's baseline value, its value before the restore (Prior) and after it,
// whether it already had the baseline value, and the store's facts after.
// Values are value-free facts; Before or After nil means absent.
type StoreRestoreEffect struct {
	Component     string            `json:"component"`
	Key           storestate.Key    `json:"key"`
	Baseline      *storestate.Item  `json:"baseline,omitempty"`
	Prior         *storestate.Item  `json:"prior,omitempty"`
	After         *storestate.Item  `json:"after,omitempty"`
	Already       bool              `json:"already,omitempty"`
	SessionsEnded bool              `json:"sessions_ended,omitempty"`
	Facts         *storestate.Facts `json:"facts,omitempty"`
}

// StoreCredentialEffect is the controller's record of one
// CheckStoreCredential.
type StoreCredentialEffect struct {
	Component string        `json:"component"`
	Client    sdk.ObjectRef `json:"client"`
	Container string        `json:"container"`
	storestate.CredentialCheck
}

// validateIdentity checks the target's component, Pod, container and port.
func (t StoreTarget) validateIdentity() error {
	if t.Component == "" {
		return fmt.Errorf("store target requires an application-map component")
	}
	if err := (Command{Pod: t.Pod, Container: t.Container, Args: []string{"sh"}}).Validate(); err != nil {
		return fmt.Errorf("store target requires a named Pod and container")
	}
	if t.Port < 0 || t.Port > 65535 {
		return fmt.Errorf("store target port %d is out of range", t.Port)
	}
	return nil
}

func (a RestoreStoreItem) Validate() error {
	if err := a.Target.validateIdentity(); err != nil {
		return err
	}
	if _, ok := storestate.AdapterFor(storestate.Engine(a.Target.Engine)); !ok {
		return fmt.Errorf("RestoreStoreItem: engine %q has no store-state reader", a.Target.Engine)
	}
	key := a.Change.Key()
	if key.Name == "" || key.Kind == "" {
		return fmt.Errorf("RestoreStoreItem requires the change of one store-state item")
	}
	for _, item := range []*storestate.Item{a.Change.Before, a.Change.After} {
		if item != nil && item.Key() != key {
			return fmt.Errorf("RestoreStoreItem change mixes items %s and %s", item.Key(), key)
		}
	}
	return nil
}

func (a CheckStoreCredential) Validate() error {
	if err := a.Target.validateIdentity(); err != nil {
		return err
	}
	supported := false
	for _, engine := range storestate.CredentialCheckEngines() {
		supported = supported || string(engine) == a.Target.Engine
	}
	if !supported {
		return fmt.Errorf("CheckStoreCredential: engine %q has no credential check (%v)", a.Target.Engine, storestate.CredentialCheckEngines())
	}
	if err := (Command{Pod: a.Client, Container: a.Container, Args: []string{"sh"}}).Validate(); err != nil {
		return fmt.Errorf("CheckStoreCredential requires the client's named Pod and container")
	}
	if a.Client.Namespace != a.Target.Pod.Namespace {
		return fmt.Errorf("CheckStoreCredential requires the client and the store in one namespace")
	}
	return a.Credential.Validate()
}

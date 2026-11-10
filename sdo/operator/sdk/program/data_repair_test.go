package program_test

import (
	"testing"
	"testing/quick"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var syntheticStore = program.StoreTarget{Component: "store", Engine: "mongodb", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-0"}, Container: "store"}

func TestStoreActionsValidate(t *testing.T) {
	scope := program.DataScope{Database: "app", Collection: "items", Selector: `{"a": 1}`}
	repair := program.RepairData{Target: syntheticStore, Scope: scope, Statement: `{"$unset": {"a": ""}}`, ExpectMin: 1, ExpectMax: 3, Backup: "before"}
	for name, test := range map[string]struct {
		action program.Action
		ok     bool
	}{
		"backup":                     {program.StoreBackup{ID: "before", Target: syntheticStore, Scope: scope}, true},
		"backup without ID":          {program.StoreBackup{Target: syntheticStore, Scope: scope}, false},
		"backup ID with a path":      {program.StoreBackup{ID: "../x", Target: syntheticStore, Scope: scope}, false},
		"engine without operations":  {program.StoreBackup{ID: "b", Target: program.StoreTarget{Component: "c", Engine: "memcached", Pod: syntheticStore.Pod, Container: "c"}}, false},
		"target without component":   {program.StoreBackup{ID: "b", Target: program.StoreTarget{Engine: "redis", Pod: syntheticStore.Pod, Container: "c"}}, false},
		"repair":                     {repair, true},
		"repair without backup":      {func() program.RepairData { r := repair; r.Backup = ""; return r }(), false},
		"repair with inverted bound": {func() program.RepairData { r := repair; r.ExpectMin = 4; return r }(), false},
		"repair expecting nothing":   {func() program.RepairData { r := repair; r.ExpectMin, r.ExpectMax = 0, 0; return r }(), false},
		"repair replacing documents": {func() program.RepairData { r := repair; r.Statement = `{"a": 2}`; return r }(), false},
		"restore":                    {program.RestoreBackup{Target: syntheticStore, Backup: "before"}, true},
		"restore without backup ID":  {program.RestoreBackup{Target: syntheticStore}, false},
		"nil pointer":                {(*program.RepairData)(nil), false},
	} {
		t.Run(name, func(t *testing.T) {
			err := program.Plan{Actions: []program.Action{test.action}}.Validate()
			if (err == nil) != test.ok {
				t.Fatalf("Validate = %v, want ok=%v", err, test.ok)
			}
		})
	}
}

func TestStoreActionsDeclareEffects(t *testing.T) {
	for action, want := range map[program.Action]program.DataEffect{
		program.StoreBackup{Target: syntheticStore}:        program.EffectRead,
		&program.InvalidateDerived{Target: syntheticStore}: program.EffectDerivedInvalidate,
		program.RepairData{Target: syntheticStore}:         program.EffectDataWrite,
		program.RestoreBackup{Target: syntheticStore}:      program.EffectDataWrite,
	} {
		target, effect, ok := program.DataActionEffect(action)
		if !ok || effect != want || target != syntheticStore {
			t.Fatalf("%T declares %s, want %s", action, effect, want)
		}
	}
	if _, _, ok := program.DataActionEffect(program.DeletePod{}); ok {
		t.Fatal("only store actions declare a store effect")
	}
}

// A backup always covers a write on its own scope, and never one on another
// component or engine.
func TestBackupCoverProperties(t *testing.T) {
	property := func(selector string, limit uint16, other bool) bool {
		scope := program.DataScope{Database: "app", Collection: "items", Selector: selector, Limit: int(limit)}
		record := program.StoreBackupRecord{Component: "store", Engine: "mongodb", Scope: scope}
		target := syntheticStore
		if other {
			target.Component = "elsewhere"
		}
		return record.Covers(target, scope) == !other
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 500}); err != nil {
		t.Fatal(err)
	}
}

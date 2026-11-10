package program_test

import (
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
)

func TestStoreRestoreAndCredentialActions(t *testing.T) {
	item := storestate.Item{Kind: storestate.KindSetting, Name: "maxmemory", Value: "0"}
	after := item
	after.Value = "1"
	other := storestate.Item{Kind: storestate.KindSetting, Name: "maxclients", Value: "1"}
	client := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "web-1"}
	for name, test := range map[string]struct {
		action program.Action
		ok     bool
		effect program.DataEffect
	}{
		"restore":                            {program.RestoreStoreItem{Target: syntheticStore, Change: storestate.Change{Before: &item, After: &after}}, true, program.EffectStoreSetting},
		"restore of an added item":           {&program.RestoreStoreItem{Target: syntheticStore, Change: storestate.Change{After: &after}}, true, program.EffectStoreSetting},
		"restore without an item":            {program.RestoreStoreItem{Target: syntheticStore}, false, ""},
		"restore mixing items":               {program.RestoreStoreItem{Target: syntheticStore, Change: storestate.Change{Before: &item, After: &other}}, false, ""},
		"restore without a reader":           {program.RestoreStoreItem{Target: program.StoreTarget{Component: "c", Engine: "etcd", Pod: syntheticStore.Pod, Container: "c"}, Change: storestate.Change{After: &after}}, false, ""},
		"credential check":                   {program.CheckStoreCredential{Target: syntheticStore, Client: client, Container: "web"}, true, program.EffectRead},
		"credential check by name":           {program.CheckStoreCredential{Target: syntheticStore, Client: client, Container: "web", Credential: storestate.CredentialInputs{Password: "DB_PASSWORD"}}, true, program.EffectRead},
		"credential check bad name":          {program.CheckStoreCredential{Target: syntheticStore, Client: client, Container: "web", Credential: storestate.CredentialInputs{Password: "$(id)"}}, false, ""},
		"credential check across namespaces": {program.CheckStoreCredential{Target: syntheticStore, Client: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "other", Name: "w"}, Container: "web"}, false, ""},
	} {
		t.Run(name, func(t *testing.T) {
			err := program.Plan{Actions: []program.Action{test.action}}.Validate()
			if (err == nil) != test.ok {
				t.Fatalf("Validate = %v, want ok=%v", err, test.ok)
			}
			if !test.ok {
				return
			}
			if _, effect, ok := program.DataActionEffect(test.action); !ok || effect != test.effect {
				t.Fatalf("effect = %q %v", effect, ok)
			}
		})
	}
}

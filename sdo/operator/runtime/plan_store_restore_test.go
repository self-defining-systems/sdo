package runtime

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
)

// withStoreBaseline makes the world's snapshot carry the promoted healthy
// store-state reading of db-0, as the controller's pod-read baseline does.
func withStoreBaseline(w *storeWorld) {
	view := w.stores.view
	id := storestate.QueryID(storestate.EngineRedis, "db")
	reading := podread.Reading{Namespace: "app", Pod: "db-0", UID: "uid-db-0", Container: "redis", Stdout: healthyRedisRead}
	view.PodReads = map[string]podread.Result{id: {Query: podread.Query{ID: id}, Readings: []podread.Reading{reading}, Baseline: []podread.Reading{reading}}}
	w.executor.Stores.Snapshot = riskSnapshots{view: view}
}

const writesBlockedRead = "min-replicas-to-write\n1\n"

func blockWrites(w *storeWorld) {
	w.stores.facts["db-0"] = strings.Replace(healthyRedisRead, "min-replicas-to-write\n0\n", writesBlockedRead, 1)
}

func minReplicasChange() storestate.Change {
	return storestate.Change{Before: &storestate.Item{Kind: storestate.KindSetting, Name: "min-replicas-to-write", Value: "0"},
		After: &storestate.Item{Kind: storestate.KindSetting, Name: "min-replicas-to-write", Value: "1"}}
}

// A typed restore puts the baseline value back with the engine's command,
// records the prior value and verifies the item with a fresh read.
func TestRestoreStoreItemRecordsPriorValueAndVerifies(t *testing.T) {
	w := newStoreWorld(t)
	withStoreBaseline(w)
	blockWrites(w)
	w.stores.restored = func(pod string, words []string) string {
		if strings.Join(words, " ") == "CONFIG SET min-replicas-to-write 0" {
			delete(w.stores.facts, pod)
		}
		return "OK\n" + storestate.RestoredMarker + "\n"
	}
	result, err := w.apply(t, program.RestoreStoreItem{Target: dbTarget, Change: minReplicasChange(), Reason: "synthetic"})
	if err != nil {
		t.Fatalf("apply: %v (ops %v)", err, w.stores.ops)
	}
	effect := result.ActionReports[0].StoreRestore
	if effect == nil || effect.Prior == nil || effect.Prior.Value != "1" || effect.Baseline.Value != "0" || effect.After == nil || effect.After.Value != "0" || effect.Already {
		t.Fatalf("effect = %+v", effect)
	}
	if want := []string{"facts:db-0", "restore-item:db-0:CONFIG SET min-replicas-to-write 0", "facts:db-0"}; strings.Join(w.stores.ops, ",") != strings.Join(want, ",") {
		t.Fatalf("ops = %v", w.stores.ops)
	}
	if len(result.Patches) != 1 || result.Patches[0].Operation != "restore_store_item" || result.Patches[0].Irreversible {
		t.Fatalf("patches = %+v", result.Patches)
	}
}

// The baseline value comes from the controller, not from the program's
// Change; an item already at its baseline is left alone.
func TestRestoreStoreItemUsesTheControllersBaselineAndIsIdempotent(t *testing.T) {
	w := newStoreWorld(t)
	withStoreBaseline(w)
	forged := minReplicasChange()
	forged.Before.Value = "5"
	result, err := w.apply(t, program.RestoreStoreItem{Target: dbTarget, Change: forged})
	if err != nil {
		t.Fatal(err)
	}
	effect := result.ActionReports[0].StoreRestore
	if effect == nil || !effect.Already || effect.Baseline.Value != "0" || len(result.Patches) != 0 {
		t.Fatalf("effect = %+v patches %+v", effect, result.Patches)
	}
	for _, op := range w.stores.ops {
		if strings.HasPrefix(op, "restore-item") {
			t.Fatalf("restored an item at its baseline: %v", w.stores.ops)
		}
	}
}

func TestRestoreStoreItemRefusals(t *testing.T) {
	for name, test := range map[string]struct {
		baseline bool
		change   storestate.Change
		answer   string
		want     string
		wrote    bool
	}{
		"no baseline":      {false, minReplicasChange(), "", "no promoted healthy baseline", false},
		"no inverse":       {true, storestate.Change{Before: &storestate.Item{Kind: storestate.KindNamespace, Name: "dbsize", Value: "2"}}, "", "not restorable", false},
		"store refuses":    {true, minReplicasChange(), "ERR CONFIG SET failed\n", "did not accept the restore", false},
		"value comes back": {true, minReplicasChange(), "OK\n" + storestate.RestoredMarker + "\n", "not the baseline", true},
	} {
		t.Run(name, func(t *testing.T) {
			w := newStoreWorld(t)
			if test.baseline {
				withStoreBaseline(w)
			}
			blockWrites(w)
			w.stores.facts["db-0"] = strings.Replace(w.stores.facts["db-0"], "== dbsize\n2", "== dbsize\n0", 1)
			w.stores.restored = func(string, []string) string { return test.answer }
			result, err := w.apply(t, program.RestoreStoreItem{Target: dbTarget, Change: test.change})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want %q", err, test.want)
			}
			if (len(result.Patches) > 0) != test.wrote {
				t.Fatalf("patches = %+v", result.Patches)
			}
		})
	}
}

type fakeCredentialStores struct {
	*fakeRedisStores
	probe  StoreCredentialProbe
	output string
}

func (f *fakeCredentialStores) StoreCredentialCheck(_ context.Context, probe StoreCredentialProbe) (string, error) {
	f.probe = probe
	return f.output, nil
}

// A credential check records a value-free result and changes nothing.
func TestCheckStoreCredentialRecordsTheResult(t *testing.T) {
	w := newStoreWorld(t)
	fake := &fakeCredentialStores{fakeRedisStores: w.stores, output: "noise\n" + storestate.CredentialMarker + " denied wrong-password direct\n"}
	w.executor.Client = fake
	client := storePodRef("web-1-a")
	result, err := w.apply(t, program.CheckStoreCredential{Target: dbTarget, Client: client, Container: "redis", Credential: storestate.CredentialInputs{Password: "CACHE_PASSWORD"}})
	if err != nil {
		t.Fatal(err)
	}
	check := result.ActionReports[0].StoreCredential
	if check == nil || check.Result != storestate.CredentialDenied || check.Class != "wrong-password" || len(result.Patches) != 0 {
		t.Fatalf("check = %+v patches %+v", check, result.Patches)
	}
	if fake.probe.ClientPod != "web-1-a" || fake.probe.StorePod != "db-0" || fake.probe.Port != 6379 || fake.probe.Inputs.Password != "CACHE_PASSWORD" {
		t.Fatalf("probe = %+v", fake.probe)
	}
}

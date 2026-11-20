package bench

import (
	"strings"
	"testing"
	"time"
)

func pinInv() Inventory {
	return Inventory{
		Hosts: map[string]Host{"h1": {}, "h2": {}},
		Checkouts: Checkouts{
			Fuzzops: map[string]map[string]string{"86e1c7c": {"h1": "/fz", "h2": "/fz"}},
			Hooks:   map[string]map[string]map[string]string{"sdo": {"ef31eb34": {"h1": "/sdo", "h2": "/sdo"}}},
		},
		HookCommand: map[string]string{"sdo": "bin/hooks"},
	}
}

func pinSpec() Spec {
	return Spec{Fuzzops: "86e1c7c", Hosts: []string{"h1", "h2"}, Operator: Operator{Hooks: "sdo", Revision: "ef31eb34"}}
}

var stampT0 = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)

func TestStampsFollowTheHostsChecks(t *testing.T) {
	specs := []specFile{{Spec: pinSpec()}}
	results := []PreflightResult{
		{Check: "capacity", Host: "h1", OK: true},
		{Check: "pack[examples/x@86e1c7c]", Host: "h1", OK: false, Detail: "binary is not stamped with 86e1c7c"},
		{Check: "capacity", Host: "h2", OK: true},
		{Check: "images", Host: "h2", OK: true},
	}
	stamps := stampsFromResults(pinInv(), specs, results, stampT0)
	got := map[string]PreflightStamp{}
	for _, s := range stamps {
		got[s.Host] = s
	}
	if len(stamps) != 2 || got["h1"].OK || !got["h2"].OK {
		t.Fatalf("stamps %+v", stamps)
	}
	if len(got["h1"].Failed) != 1 || !strings.Contains(got["h1"].Failed[0], "not stamped") {
		t.Fatalf("a failing host names what failed: %+v", got["h1"])
	}
}

func TestPlanPinsNeedAPassingFreshStampOnAnAdmittedHost(t *testing.T) {
	inv, s := pinInv(), pinSpec()
	none := Stamps{}
	err := checkPins(inv, none, s, stampT0)
	if err == nil || !strings.Contains(err.Error(), "preflight") || !strings.Contains(err.Error(), "fuzzops 86e1c7c") ||
		!strings.Contains(err.Error(), "lanes preflight --plan") {
		t.Fatalf("an unstamped pin: %v", err)
	}
	failed := Stamps{Stamps: []PreflightStamp{{Host: "h1", Fuzzops: "86e1c7c", Hooks: "sdo", Revision: "ef31eb34", At: stampT0, Failed: []string{"pack[x]: binary is not stamped"}}}}
	if err := checkPins(inv, failed, s, stampT0); err == nil || !strings.Contains(err.Error(), "not stamped") {
		t.Fatalf("a failed stamp must be refused with its failure: %v", err)
	}
	ok := Stamps{Stamps: []PreflightStamp{
		{Host: "h1", Fuzzops: "86e1c7c", Hooks: "sdo", Revision: "ef31eb34", At: stampT0},
		{Host: "h2", Fuzzops: "86e1c7c", Hooks: "sdo", Revision: "ef31eb34", At: stampT0, OK: true},
	}}
	if err := checkPins(inv, ok, s, stampT0.Add(time.Hour)); err != nil {
		t.Fatalf("one passing host is enough: %v", err)
	}
	if err := checkPins(inv, ok, s, stampT0.Add(StampMaxAge+time.Minute)); err == nil || !strings.Contains(err.Error(), "older than") {
		t.Fatalf("a stale stamp: %v", err)
	}
	other := pinSpec()
	other.Operator.Revision = "ef31eb35"
	inv.Checkouts.Hooks["sdo"]["ef31eb35"] = map[string]string{"h1": "/sdo2"}
	if err := checkPins(inv, ok, other, stampT0); err == nil {
		t.Fatal("a stamp of another subject revision must not count")
	}
}

func TestStampsPersistAndANewRunReplacesTheSameKey(t *testing.T) {
	root := Root(t.TempDir())
	s := pinSpec()
	st1 := PreflightStamp{Host: "h1", Fuzzops: s.Fuzzops, Hooks: "sdo", Revision: "ef31eb34", At: stampT0, OK: true}
	if err := SaveStamps(root, []PreflightStamp{st1}); err != nil {
		t.Fatal(err)
	}
	st2 := PreflightStamp{Host: "h2", Fuzzops: s.Fuzzops, Hooks: "sdo", Revision: "ef31eb34", At: stampT0, OK: true}
	bad := st1
	bad.OK, bad.Failed = false, []string{"x"}
	if err := SaveStamps(root, []PreflightStamp{st2, bad}); err != nil {
		t.Fatal(err)
	}
	got, err := LoadStamps(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Stamps) != 2 {
		t.Fatalf("stamps %+v", got.Stamps)
	}
	for _, x := range got.Stamps {
		if x.Host == "h1" && x.OK {
			t.Fatal("the newer failing run of the same key must replace the pass")
		}
	}
}

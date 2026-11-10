package storestate_test

import (
	"math/rand"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

func change(changes []storestate.Change, kind storestate.ItemKind, name string) (storestate.Change, bool) {
	for _, c := range changes {
		if c.Key().Kind == kind && c.Key().Name == name {
			return c, true
		}
	}
	return storestate.Change{}, false
}

func TestDiffAgainstTheHealthyBaseline(t *testing.T) {
	baseline := parse(t, healthyRedis)
	for name, test := range map[string]struct {
		from, to string
		kind     storestate.ItemKind
		item     string
		before   string
		after    string
	}{
		"min replicas gives a setting change": {"min-replicas-to-write\n0", "min-replicas-to-write\n1", storestate.KindSetting, "min-replicas-to-write", "0", "1"},
		"min replicas gives a write block":    {"min-replicas-to-write\n0", "min-replicas-to-write\n1", storestate.KindWriteBlock, "min-replicas", "", "active"},
		"master to replica is a role change":  {"role:master", "role:slave", storestate.KindRole, "role", "master", "slave"},
		"revoked categories change the principal": {
			"user default on nopass sanitize-payload ~* &* +@all", "user default on nopass sanitize-payload ~* &* +@all -@read -@write",
			storestate.KindPrincipal, "default", "on nopass sanitize-payload ~* &* +@all", "on nopass sanitize-payload ~* &* +@all -@read -@write",
		},
		"a set password changes the principal": {"requirepass\nempty", "requirepass\nset", storestate.KindPrincipal, "requirepass", "empty", "set"},
	} {
		t.Run(name, func(t *testing.T) {
			current := parse(t, strings.Replace(healthyRedis, test.from, test.to, 1))
			got, ok := change(storestate.Diff(baseline, current), test.kind, test.item)
			if !ok {
				t.Fatalf("no %s change for %s in %+v", test.kind, test.item, storestate.Diff(baseline, current))
			}
			if value(got.Before) != test.before || value(got.After) != test.after {
				t.Fatalf("change = %q -> %q", value(got.Before), value(got.After))
			}
		})
	}
}

func value(it *storestate.Item) string {
	if it == nil {
		return ""
	}
	return it.Value
}

func TestDiffProperties(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	kinds := []storestate.ItemKind{storestate.KindSetting, storestate.KindRole, storestate.KindPrincipal, storestate.KindWriteBlock}
	randomFacts := func() storestate.Facts {
		var items []storestate.Item
		for i := 0; i < random.Intn(12); i++ {
			items = append(items, storestate.Item{Kind: kinds[random.Intn(len(kinds))], Name: string(rune('a' + random.Intn(6))), Value: string(rune('0' + random.Intn(3)))})
		}
		return storestate.NewFacts(storestate.EngineRedis, items)
	}
	for i := 0; i < 500; i++ {
		a, b := randomFacts(), randomFacts()
		if d := storestate.Diff(a, a); len(d) != 0 {
			t.Fatalf("Diff(x, x) = %+v", d)
		}
		forward, backward := storestate.Diff(a, b), storestate.Diff(b, a)
		if len(forward) != len(backward) {
			t.Fatalf("asymmetric diff: %+v vs %+v", forward, backward)
		}
		// Applying the diff to the baseline gives the current facts.
		if got := storestate.Apply(a, forward); len(storestate.Diff(got, b)) != 0 {
			t.Fatalf("Apply(a, Diff(a, b)) != b: %+v", storestate.Diff(got, b))
		}
		for j := 1; j < len(forward); j++ {
			if !forward[j-1].Key().Less(forward[j].Key()) {
				t.Fatalf("diff not sorted: %+v", forward)
			}
		}
	}
}

func TestNewFactsBoundsItems(t *testing.T) {
	items := make([]storestate.Item, storestate.MaxItems+5)
	for i := range items {
		items[i] = storestate.Item{Kind: storestate.KindNamespace, Name: strings.Repeat("k", i%40+1), Scope: string(rune('a' + i%26)), Value: "1"}
	}
	if err := storestate.NewFacts(storestate.EngineRedis, items).Validate(); err == nil {
		t.Fatal("unbounded facts validate")
	}
}

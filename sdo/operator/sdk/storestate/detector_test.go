package storestate_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

func config(t *testing.T, baseline ...storestate.Facts) storestate.Config {
	t.Helper()
	return storestate.Config{
		Spec:      sdk.DetectorSpec{ID: "store-state-cache", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Minute},
		Component: "cache",
		Engine:    storestate.EngineRedis,
		Target:    podread.Target{Labels: map[string]string{"app": "cache"}},
		Baselines: baseline,
	}
}

func detector(t *testing.T, c storestate.Config) sdk.Detector {
	t.Helper()
	d, err := storestate.NewDetector(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := podread.ValidateDetector(d); err != nil {
		t.Fatalf("pod reads: %v", err)
	}
	return d
}

func snapshot(t *testing.T, d sdk.Detector, readings ...podread.Reading) sdktest.Snapshot {
	t.Helper()
	q := d.(podread.Consumer).PodReads()
	if len(q) != 1 {
		t.Fatalf("queries = %+v", q)
	}
	return sdktest.Snapshot{PodReads: map[string]podread.Result{q[0].ID: {Query: q[0], Readings: readings}}}
}

func reading(stdout string) podread.Reading {
	return podread.Reading{Namespace: "shop", Pod: "cache-0", Container: "store", ObservedAt: time.Unix(100, 0).UTC(), Stdout: stdout}
}

func healthyBaseline(t *testing.T) storestate.Facts {
	t.Helper()
	f := parse(t, healthyRedis)
	f.Component = "cache"
	f.Pod = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "shop", Name: "cache-0"}
	return f
}

func TestDetectorClearsOnlyOnACompleteHealthyRead(t *testing.T) {
	d := detector(t, config(t, healthyBaseline(t)))
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(healthyRedis)))
	if err != nil || len(findings) != 0 {
		t.Fatalf("healthy: %+v, %v", findings, err)
	}
}

func TestDetectorFindingNamesTheComponentAndTheChangedItem(t *testing.T) {
	d := detector(t, config(t, healthyBaseline(t)))
	current := strings.Replace(healthyRedis, "min-replicas-to-write\n0", "min-replicas-to-write\n1", 1)
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(current)))
	if err != nil {
		t.Fatal(err)
	}
	var block *sdk.Finding
	for i := range findings {
		if findings[i].RuleID == storestate.RuleWriteBlocked {
			block = &findings[i]
		}
	}
	if block == nil {
		t.Fatalf("no write-block finding: %+v", findings)
	}
	if block.PrimaryResource != (sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "shop", Name: "cache-0"}) {
		t.Fatalf("primary = %+v", block.PrimaryResource)
	}
	for _, want := range []string{"cache", "min-replicas"} {
		if !strings.Contains(block.Summary, want) {
			t.Errorf("summary %q does not name %q", block.Summary, want)
		}
	}
	if !strings.Contains(block.Evidence, "min-replicas-to-write 0 -> 1") {
		t.Errorf("evidence %q does not cite the changed setting", block.Evidence)
	}
	if block.Metadata["component"] != "cache" || block.Metadata["change"] == nil {
		t.Errorf("metadata = %+v", block.Metadata)
	}
	seen := map[string]bool{}
	for _, f := range findings {
		key := sdk.FindingFingerprint(f)
		if seen[key] {
			t.Fatalf("duplicate fingerprint %s", key)
		}
		seen[key] = true
	}
}

func TestDetectorRules(t *testing.T) {
	for name, test := range map[string]struct {
		from, to, rule string
	}{
		"role":        {"role:master", "role:slave", storestate.RuleRoleChanged},
		"principal":   {"+@all\n", "+@all -@read -@write\n", storestate.RulePrincipalChanged},
		"password":    {"requirepass\nempty", "requirepass\nset", storestate.RulePrincipalChanged},
		"refusal":     {"== dbsize\n12", "== dbsize\nNOPERM no permissions", storestate.RuleRefused},
		"max clients": {"connected_clients:3", "connected_clients:9990", storestate.RuleConnectionsNearLimit},
		"memory full": {"maxmemory:0", "maxmemory:1048000", storestate.RuleMemoryNearLimit},
	} {
		t.Run(name, func(t *testing.T) {
			d := detector(t, config(t, healthyBaseline(t)))
			findings, err := d.Detect(context.Background(), snapshot(t, d, reading(strings.Replace(healthyRedis, test.from, test.to, 1))))
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range findings {
				if f.RuleID == test.rule {
					return
				}
			}
			t.Fatalf("no %s finding: %+v", test.rule, findings)
		})
	}
}

func TestDetectorWithoutBaselineJudgesAbsoluteFacts(t *testing.T) {
	d := detector(t, config(t))
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(healthyRedis)))
	if err != nil || len(findings) != 0 {
		t.Fatalf("healthy without baseline: %+v, %v", findings, err)
	}
	findings, err = d.Detect(context.Background(), snapshot(t, d, reading(strings.Replace(healthyRedis, "role:master", "role:slave", 1))))
	if err != nil || len(findings) == 0 || findings[0].RuleID != storestate.RuleWriteBlocked {
		t.Fatalf("replica without baseline: %+v, %v", findings, err)
	}
}

func TestDetectorUnavailableReadIsAnErrorNeverClear(t *testing.T) {
	d := detector(t, config(t, healthyBaseline(t)))
	id := d.(podread.Consumer).PodReads()[0].ID
	for name, s := range map[string]sdktest.Snapshot{
		"no read":       {},
		"source error":  {PodReads: map[string]podread.Result{id: {Error: "no Pod holds the role"}}},
		"no readings":   {PodReads: map[string]podread.Result{id: {}}},
		"reading error": snapshot(t, d, podread.Reading{Pod: "cache-0", Error: "exec timed out"}),
		"truncated":     snapshot(t, d, podread.Reading{Pod: "cache-0", Stdout: healthyRedis, Truncated: true}),
		"connection":    snapshot(t, d, reading("== info\nCould not connect to Redis at 127.0.0.1:6379: Connection refused\n")),
		"no cli":        snapshot(t, d, reading("sh: redis-cli: not found\n")),
	} {
		t.Run(name, func(t *testing.T) {
			findings, err := d.Detect(context.Background(), s)
			if err == nil {
				t.Fatalf("unavailable read cleared: %+v", findings)
			}
		})
	}
}

func TestNewDetectorValidatesConfig(t *testing.T) {
	for name, change := range map[string]func(*storestate.Config){
		"no component": func(c *storestate.Config) { c.Component = "" },
		"no id":        func(c *storestate.Config) { c.Spec.ID = "" },
		// Baselines are cleared so only the missing adapter can refuse it.
		"engine not read":    func(c *storestate.Config) { c.Engine, c.Baselines = storestate.EngineMySQL, nil },
		"no target":          func(c *storestate.Config) { c.Target = podread.Target{} },
		"bad port":           func(c *storestate.Config) { c.Port = 70000 },
		"short interval":     func(c *storestate.Config) { c.ReadInterval = time.Second },
		"foreign baseline":   func(c *storestate.Config) { c.Baselines[0].Component = "other" },
		"unavailable":        func(c *storestate.Config) { c.Baselines[0].Error = "unavailable" },
		"wrong engine":       func(c *storestate.Config) { c.Baselines[0].Engine = storestate.EnginePostgres },
		"ambiguous baseline": func(c *storestate.Config) { c.Baselines = append(c.Baselines, c.Baselines[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			c := config(t, healthyBaseline(t))
			change(&c)
			if _, err := storestate.NewDetector(c); err == nil {
				t.Fatal("NewDetector accepted the config")
			}
		})
	}
}

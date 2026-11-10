package storestate_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

const (
	appGrants   = "GRANT USAGE ON *.* TO `app`@`%`\x1fGRANT ALL PRIVILEGES ON `shop`.* TO `app`@`%`"
	appValue    = "unlocked global=1 schema=18 table=0"
	mysqlPayout = "== payload grants\ngrants\tapp\t%\t" + appValue + "\t" + appGrants + "\n== end\n"
)

func mysqlChanges(t *testing.T, replacements ...string) (storestate.Facts, map[storestate.Key]storestate.Change) {
	t.Helper()
	baseline := parseAs(t, storestate.EngineMySQL, healthyMySQL)
	current := healthyMySQL
	for i := 0; i+1 < len(replacements); i += 2 {
		if !strings.Contains(current, replacements[i]) {
			t.Fatalf("fixture lacks %q", replacements[i])
		}
		current = strings.Replace(current, replacements[i], replacements[i+1], 1)
	}
	out := map[storestate.Key]storestate.Change{}
	for _, c := range storestate.Diff(baseline, parseAs(t, storestate.EngineMySQL, current)) {
		out[c.Key()] = c
	}
	return baseline, out
}

func mysqlPayload(t *testing.T) storestate.Payload {
	t.Helper()
	reader, ok := storestate.PayloadReaderFor(storestate.EngineMySQL)
	if !ok {
		t.Fatal("mysql has no payload reader")
	}
	p, err := reader.ParsePayload(mysqlPayout)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Every mysql fault the read shows either has an inverse or a stated reason
// why none is exact.
func TestMysqlRestoreTableCoversWhatTheReadShows(t *testing.T) {
	payload := mysqlPayload(t)
	for name, test := range map[string]struct {
		replace []string
		key     storestate.Key
		want    string // a fragment of the command, or of the refusal reason
		refused bool
	}{
		"max_connections": {[]string{"max_connections\t151", "max_connections\t5"}, storestate.Key{Kind: storestate.KindSetting, Name: "max_connections"}, "max_connections", false},
		"super_read_only": {[]string{"super_read_only\tOFF", "super_read_only\tON"}, storestate.Key{Kind: storestate.KindWriteBlock, Name: "super-read-only"}, "super_read_only", false},
		"account locked":  {[]string{"00:00:00\tunlocked global=1", "00:00:00\tlocked global=1"}, storestate.Key{Kind: storestate.KindPrincipal, Name: "app@%"}, "ACCOUNT UNLOCK", false},
		"grants revoked":  {[]string{"schema=18", "schema=0"}, storestate.Key{Kind: storestate.KindPrincipal, Name: "app@%"}, "GRANT ALL PRIVILEGES ON `shop`.*", false},
		"password":        {[]string{"app\t%\t2026-01-01T00:00:00", "app\t%\t2026-02-02T00:00:00"}, storestate.Key{Kind: storestate.KindSetting, Scope: "app@%", Name: "password-changed"}, "password", true},
		"global lock":     {[]string{"== mdl\n", "== mdl\nGLOBAL\t\t\tSHARED\n"}, storestate.Key{Kind: storestate.KindWriteBlock, Name: "global-read-lock"}, "session that took it", true},
		"startup option":  {[]string{"innodb_read_only\tOFF", "innodb_read_only\tON"}, storestate.Key{Kind: storestate.KindSetting, Name: "innodb_read_only"}, "runtime", true},
		"table renamed":   {[]string{"shop\torders\t80", "shop\torders_old\t80"}, storestate.Key{Kind: storestate.KindNamespace, Scope: "shop", Name: "orders_old"}, "no inverse", true},
	} {
		t.Run(name, func(t *testing.T) {
			baseline, changes := mysqlChanges(t, test.replace...)
			change, ok := changes[test.key]
			if !ok {
				t.Fatalf("no change of %s in %v", test.key, changes)
			}
			cmds, err := storestate.RestoreCommands(storestate.EngineMySQL, storestate.Endpoint{}, change, baseline, storestate.RestoreOptions{Payload: payload.Lookup})
			if test.refused {
				if !storestate.IsNotRestorable(err) || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("err = %v, want a refusal naming %q", err, test.want)
				}
				return
			}
			if err != nil || len(cmds) != 1 || !strings.Contains(strings.Join(cmds[0], "\n"), test.want) {
				t.Fatalf("cmds = %q, err = %v, want %q", cmds, err, test.want)
			}
		})
	}
}

func TestMysqlGrantsAreReplayedOnlyFromAMatchingBaselineText(t *testing.T) {
	baseline, changes := mysqlChanges(t, "schema=18", "schema=0")
	change := changes[storestate.Key{Kind: storestate.KindPrincipal, Name: "app@%"}]
	for name, test := range map[string]struct {
		payload storestate.PayloadLookup
		want    string
	}{
		"no text": {func(storestate.Key) (storestate.PayloadEntry, bool) { return storestate.PayloadEntry{}, false }, "no restore text"},
		"stale text": {func(k storestate.Key) (storestate.PayloadEntry, bool) {
			return storestate.PayloadEntry{Key: k, Value: "unlocked global=1 schema=2 table=0", Format: storestate.PayloadGrants, Text: appGrants}, true
		}, "describes"},
		"foreign line": {func(k storestate.Key) (storestate.PayloadEntry, bool) {
			return storestate.PayloadEntry{Key: k, Value: appValue, Format: storestate.PayloadGrants, Text: "GRANT ALL ON *.* TO `other`@`%`"}, true
		}, "does not replay"},
		"injection": {func(k storestate.Key) (storestate.PayloadEntry, bool) {
			return storestate.PayloadEntry{Key: k, Value: appValue, Format: storestate.PayloadGrants, Text: "GRANT SELECT ON `a`.* TO `app`@`%`; DROP DATABASE shop; -- TO `app`@`%`"}, true
		}, "does not replay"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := storestate.RestoreCommands(storestate.EngineMySQL, storestate.Endpoint{}, change, baseline, storestate.RestoreOptions{Payload: test.payload})
			if !storestate.IsNotRestorable(err) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want a refusal naming %q", err, test.want)
			}
		})
	}
	if err := storestate.Restorable(storestate.EngineMySQL, change, baseline); err != nil {
		t.Fatalf("Restorable = %v", err)
	}
}

func TestMysqlPayloadReadIsIncompleteWithoutItsEndMarker(t *testing.T) {
	reader, _ := storestate.PayloadReaderFor(storestate.EngineMySQL)
	if _, err := reader.ParsePayload(strings.TrimSuffix(mysqlPayout, "== end\n")); err == nil {
		t.Fatal("a payload without its end marker parsed")
	}
	if p := mysqlPayload(t); len(p.Entries) != 1 || p.Entries[0].Key.Name != "app@%" {
		t.Fatalf("entries = %+v", p.Entries)
	}
}

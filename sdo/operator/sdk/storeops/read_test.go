package storeops_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storeops"
)

func TestValidateReadRefusesSideEffectingSelectors(t *testing.T) {
	for name, test := range map[string]struct {
		engine string
		scope  storeops.Scope
		ok     bool
	}{
		"postgres comparison":      {"postgres", storeops.Scope{Collection: "items", Selector: "kind = 'a' AND n > 3"}, true},
		"postgres null and in":     {"postgres", storeops.Scope{Collection: "items", Selector: "price IS NULL OR kind IN ('a', 'b')"}, true},
		"postgres sleep":           {"postgres", storeops.Scope{Collection: "items", Selector: "pg_sleep(5) IS NULL"}, false},
		"postgres terminate":       {"postgres", storeops.Scope{Collection: "items", Selector: "pg_terminate_backend(1)"}, false},
		"postgres read file":       {"postgres", storeops.Scope{Collection: "items", Selector: "pg_read_file('/etc/passwd') = ''"}, false},
		"postgres subquery":        {"postgres", storeops.Scope{Collection: "items", Selector: "n = (SELECT 1)"}, false},
		"postgres cast":            {"postgres", storeops.Scope{Collection: "items", Selector: "n::text = '1'"}, false},
		"postgres dollar quote":    {"postgres", storeops.Scope{Collection: "items", Selector: "kind = $$a$$"}, false},
		"postgres credential col":  {"postgres", storeops.Scope{Collection: "items", Selector: "password_hash = 'x'"}, false},
		"postgres function spaced": {"postgres", storeops.Scope{Collection: "items", Selector: "now ()  > 1"}, false},
		"mysql sleep":              {"mysql", storeops.Scope{Collection: "items", Selector: "SLEEP(5) = 0"}, false},
		"mysql get lock":           {"mysql", storeops.Scope{Collection: "items", Selector: "GET_LOCK('a', 5)"}, false},
		"mysql comparison":         {"mysql", storeops.Scope{Collection: "items", Selector: "price IS NOT NULL AND id >= 2"}, true},
		"mysql user variable":      {"mysql", storeops.Scope{Collection: "items", Selector: "@x = 1"}, false},
		"mongodb comparison":       {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"price": {"$gt": 3}, "kind": "a"}`}, true},
		"mongodb logical":          {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"$or": [{"a": 1}, {"b": {"$in": [1, 2]}}]}`}, true},
		"mongodb where":            {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"$where": "sleep(5000)"}`}, false},
		"mongodb expr":             {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"$expr": {"$function": {}}}`}, false},
		"mongodb nested where":     {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"$and": [{"$where": "1"}]}`}, false},
		"mongodb credential field": {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"api_token": "x"}`}, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := storeops.ValidateRead(engine(t, test.engine), test.scope, []string{"id"})
			if (err == nil) != test.ok {
				t.Fatalf("ValidateRead(%+v) = %v, want ok=%v", test.scope, err, test.ok)
			}
		})
	}
}

func TestValidateReadRefusesCredentialCatalogsAndFields(t *testing.T) {
	for name, test := range map[string]struct {
		engine string
		scope  storeops.Scope
		fields []string
		ok     bool
	}{
		"postgres pg_shadow":        {"postgres", storeops.Scope{Collection: "pg_shadow"}, []string{"usename"}, false},
		"postgres pg_authid":        {"postgres", storeops.Scope{Collection: "pg_catalog.pg_authid"}, []string{"rolname"}, false},
		"postgres pg_user_mapping":  {"postgres", storeops.Scope{Collection: "pg_user_mapping"}, []string{"umuser"}, false},
		"postgres app table":        {"postgres", storeops.Scope{Collection: "public.items"}, []string{"id", "price"}, true},
		"mysql mysql.user":          {"mysql", storeops.Scope{Database: "mysql", Collection: "user"}, []string{"User"}, false},
		"mysql mysql grant table":   {"mysql", storeops.Scope{Database: "mysql", Collection: "db"}, []string{"Db"}, false},
		"mysql app table":           {"mysql", storeops.Scope{Database: "shop", Collection: "orders"}, []string{"id"}, true},
		"mongodb system users":      {"mongodb", storeops.Scope{Database: "admin", Collection: "system.users"}, []string{"user"}, false},
		"mongodb system keys":       {"mongodb", storeops.Scope{Database: "admin", Collection: "system.keys"}, []string{"_id"}, false},
		"mongodb dotted path":       {"mongodb", storeops.Scope{Database: "app", Collection: "items"}, []string{"a.b.c"}, true},
		"no fields":                 {"postgres", storeops.Scope{Collection: "items"}, nil, false},
		"too many fields":           {"postgres", storeops.Scope{Collection: "items"}, strings.Split("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q", ","), false},
		"duplicate field":           {"postgres", storeops.Scope{Collection: "items"}, []string{"a", "a"}, false},
		"password field":            {"postgres", storeops.Scope{Collection: "items"}, []string{"password"}, false},
		"hash field":                {"mysql", storeops.Scope{Collection: "items"}, []string{"password_hash"}, false},
		"authentication_string":     {"mysql", storeops.Scope{Collection: "items"}, []string{"authentication_string"}, false},
		"passwd field":              {"postgres", storeops.Scope{Collection: "items"}, []string{"passwd"}, false},
		"pwd field":                 {"mysql", storeops.Scope{Collection: "items"}, []string{"pwd"}, false},
		"token field":               {"postgres", storeops.Scope{Collection: "items"}, []string{"session_token"}, false},
		"api key suffix":            {"postgres", storeops.Scope{Collection: "items"}, []string{"api_key"}, false},
		"key prefix":                {"postgres", storeops.Scope{Collection: "items"}, []string{"key_id"}, false},
		"salt field":                {"postgres", storeops.Scope{Collection: "items"}, []string{"salt"}, false},
		"otp field":                 {"postgres", storeops.Scope{Collection: "items"}, []string{"otp"}, false},
		"mongodb dollar field":      {"mongodb", storeops.Scope{Database: "app", Collection: "items"}, []string{"$where"}, false},
		"sql expression as field":   {"postgres", storeops.Scope{Collection: "items"}, []string{"id, usename"}, false},
		"sql function as field":     {"postgres", storeops.Scope{Collection: "items"}, []string{"pg_sleep(1)"}, false},
		"mongodb empty path piece":  {"mongodb", storeops.Scope{Database: "app", Collection: "items"}, []string{"a..b"}, false},
		"ordinary price and status": {"postgres", storeops.Scope{Collection: "items"}, []string{"price", "status", "created_at"}, true},
	} {
		t.Run(name, func(t *testing.T) {
			err := storeops.ValidateRead(engine(t, test.engine), test.scope, test.fields)
			if (err == nil) != test.ok {
				t.Fatalf("ValidateRead(%+v, %v) = %v, want ok=%v", test.scope, test.fields, err, test.ok)
			}
		})
	}
}

func TestReadIsUnavailableWhereRowsHaveNoFields(t *testing.T) {
	for _, name := range []string{"redis", "memcached"} {
		if err := storeops.ValidateRead(engine(t, name), storeops.Scope{}, []string{"key"}); err == nil {
			t.Fatalf("%s must refuse Read: only Count reads its records", name)
		}
	}
}

func TestReadCommandsAreBoundedAndReadOnly(t *testing.T) {
	for name, test := range map[string]struct {
		engine string
		scope  storeops.Scope
		want   []string
	}{
		"postgres": {"postgres", storeops.Scope{Collection: "items", Selector: "price IS NULL"}, []string{"default_transaction_read_only=on", "statement_timeout", "LIMIT 32", `"price"`, "sdo-count"}},
		"mysql":    {"mysql", storeops.Scope{Database: "shop", Collection: "items", Selector: "price IS NULL"}, []string{"READ ONLY", "LIMIT 32", "JSON_OBJECT", "sdo-count"}},
		"mongodb":  {"mongodb", storeops.Scope{Database: "app", Collection: "items", Selector: `{"a": 1}`}, []string{"limit(32)", "maxTimeMS", "sdo-count", "#sdo-end"}},
	} {
		t.Run(name, func(t *testing.T) {
			e := engine(t, test.engine)
			fields := []string{"price"}
			if err := storeops.ValidateRead(e, test.scope, fields); err != nil {
				t.Fatal(err)
			}
			command, err := e.Read(0, test.scope, fields)
			if err != nil {
				t.Fatal(err)
			}
			text := strings.Join(command.Args, "\n") + command.Stdin
			for _, want := range test.want {
				if !strings.Contains(text, want) {
					t.Fatalf("%s read command lacks %q:\n%s", name, want, text)
				}
			}
			for _, banned := range []string{"UPDATE", "DELETE", "INSERT", "updateMany", "deleteMany"} {
				if strings.Contains(text, banned) {
					t.Fatalf("%s read command contains %q", name, banned)
				}
			}
		})
	}
}

func TestParseReadRequiresTheEndMarker(t *testing.T) {
	fields := []string{"id", "price"}
	count, rows, err := storeops.ParseRead("sdo-count 3\n{\"id\":1,\"price\":null}\n{\"id\":2,\"price\":4.5}\n#sdo-end 2\n", fields)
	if err != nil || count != 3 || len(rows) != 2 {
		t.Fatalf("ParseRead = %d, %v, %v", count, rows, err)
	}
	if rows[0]["id"] != int64(1) || rows[0]["price"] != nil || rows[1]["price"] != 4.5 {
		t.Fatalf("rows are not typed scalars: %#v", rows)
	}
	for name, out := range map[string]string{
		"no end marker":    "sdo-count 1\n{\"id\":1}\n",
		"no count":         "{\"id\":1}\n#sdo-end 1\n",
		"marker mismatch":  "sdo-count 1\n{\"id\":1}\n#sdo-end 2\n",
		"array value":      "sdo-count 1\n{\"id\":[1]}\n#sdo-end 1\n",
		"object value":     "sdo-count 1\n{\"id\":{\"a\":1}}\n#sdo-end 1\n",
		"unrequested key":  "sdo-count 1\n{\"id\":1,\"password\":\"x\"}\n#sdo-end 1\n",
		"long string":      "sdo-count 1\n{\"id\":\"" + strings.Repeat("a", 257) + "\"}\n#sdo-end 1\n",
		"too many rows":    "sdo-count 33\n" + strings.Repeat("{\"id\":1}\n", 33) + "#sdo-end 33\n",
		"not json":         "sdo-count 1\nnot json\n#sdo-end 1\n",
		"data after end":   "sdo-count 1\n{\"id\":1}\n#sdo-end 1\n{\"id\":2}\n",
		"empty everything": "",
	} {
		if _, _, err := storeops.ParseRead(out, fields); err == nil {
			t.Fatalf("%s: ParseRead accepted %q", name, out)
		}
	}
}

func TestCredentialName(t *testing.T) {
	for name, want := range map[string]bool{
		"password": true, "Password_Hash": true, "pwd": true, "passwd": true, "pass": true, "api_key": true, "key_id": true,
		"apiKey": true, "session_token": true, "authentication_string": true, "salt": true, "otp": true, "digest": true,
		"encrypted_value": true, "cookie": true, "secret": true, "credential": true, "authorization": true,
		"price": false, "status": false, "created_at": false, "kind": false, "id": false, "name": false, "key": false,
	} {
		if got := storeops.CredentialName(name); got != want {
			t.Errorf("CredentialName(%q) = %v, want %v", name, got, want)
		}
	}
}

//go:build integration

package storestate_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// A credential check authenticates once with the client's credential, taken
// from the client's environment inside the check's container, and prints
// only a value-free result.
func TestCredentialCheckClassifiesOneAuthentication(t *testing.T) {
	for name, test := range map[string]struct {
		engine storestate.Engine
		cli    string
		env    map[string]string
		inputs storestate.CredentialInputs
		reply  string
		want   storestate.CredentialCheck
		args   []string
	}{
		"redis ok":             {storestate.EngineRedis, "redis-cli", map[string]string{"REDIS_PASSWORD": "synthetic-a"}, storestate.CredentialInputs{}, "PONG\n", storestate.CredentialCheck{Result: "ok", Class: "authenticated"}, []string{"-h\n10.0.0.2\n-p\n6379\nPING"}},
		"redis wrong password": {storestate.EngineRedis, "redis-cli", map[string]string{"APP_SECRET": "synthetic-a"}, storestate.CredentialInputs{Password: "APP_SECRET"}, "AUTH failed: WRONGPASS invalid username-password pair\n", storestate.CredentialCheck{Result: "denied", Class: "wrong-password"}, nil},
		"redis password now required": {storestate.EngineRedis, "redis-cli", nil, storestate.CredentialInputs{}, "NOAUTH Authentication required.\n", storestate.CredentialCheck{Result: "denied", Class: "credential-required"}, nil},
		"redis at the cap":     {storestate.EngineRedis, "redis-cli", nil, storestate.CredentialInputs{}, "ERR max number of clients reached\n", storestate.CredentialCheck{Result: "unreachable", Class: "max-clients"}, nil},
		"postgres ok":          {storestate.EnginePostgres, "psql", map[string]string{"DB_USER": "app", "DB_PASSWORD": "synthetic-b", "DB_NAME": "orders"}, storestate.CredentialInputs{Database: "DB_NAME"}, "1\n", storestate.CredentialCheck{Result: "ok", Class: "authenticated"}, []string{"-U\napp\n-d\norders\n"}},
		"postgres changed":     {storestate.EnginePostgres, "psql", map[string]string{"DB_USER": "app", "DB_PASSWORD": "synthetic-b"}, storestate.CredentialInputs{}, "psql: error: FATAL:  password authentication failed for user \"app\"\n", storestate.CredentialCheck{Result: "denied", Class: "wrong-password"}, nil},
		"postgres uri":         {storestate.EnginePostgres, "psql", map[string]string{"DATABASE_URL": "postgres://app:synthetic-c@db:5432/orders"}, storestate.CredentialInputs{}, "1\n", storestate.CredentialCheck{Result: "ok", Class: "authenticated", Via: "uri"}, []string{"postgres://app:synthetic-c@db:5432/orders"}},
		"postgres down":        {storestate.EnginePostgres, "psql", nil, storestate.CredentialInputs{}, "psql: error: could not connect to server: Connection refused\n", storestate.CredentialCheck{Result: "unreachable", Class: "no-connection"}, nil},
		"mongodb ok":           {storestate.EngineMongoDB, "mongo", map[string]string{"MONGO_USER": "app", "MONGO_PASSWORD": "synthetic-d"}, storestate.CredentialInputs{}, "SDO-USERS 1\n", storestate.CredentialCheck{Result: "ok", Class: "authenticated"}, []string{"-u\napp\n"}},
		"mongodb denied":       {storestate.EngineMongoDB, "mongo", map[string]string{"MONGO_USER": "app", "MONGO_PASSWORD": "synthetic-d"}, storestate.CredentialInputs{}, "Error: Authentication failed.\n", storestate.CredentialCheck{Result: "denied", Class: "wrong-password"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			argv, err := storestate.CredentialCheckCommand(test.engine, "10.0.0.2", map[storestate.Engine]int{storestate.EngineRedis: 6379, storestate.EnginePostgres: 5432, storestate.EngineMongoDB: 27017}[test.engine], test.inputs)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(strings.Join(argv, " "), "synthetic") {
				t.Fatal("the command carries a credential")
			}
			out, record := runStub(t, argv, test.env, test.reply, test.cli)
			if strings.Contains(out, "synthetic") {
				t.Fatalf("the output carries a credential: %q", out)
			}
			got, err := storestate.ParseCredentialCheck(out)
			if err != nil || got != test.want {
				t.Fatalf("check = %+v, %v (output %q)", got, err, out)
			}
			for _, a := range test.args {
				if !strings.Contains(record, a) {
					t.Errorf("CLI args lack %q: %q", a, record)
				}
			}
		})
	}
	if _, err := storestate.CredentialCheckCommand(storestate.EngineRedis, "h", 6379, storestate.CredentialInputs{Password: "BAD NAME"}); err == nil {
		t.Fatal("an input that is not a variable name was accepted")
	}
}

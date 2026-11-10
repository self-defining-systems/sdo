//go:build integration

package storestate_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// runRead runs an engine's read command with sh in a synthetic container
// environment whose store CLI is a stub recording the credentials it was
// given, and returns that record. Values are synthetic.
func runRead(t *testing.T, shell []string, engine storestate.Engine, cli string, env map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	record := filepath.Join(dir, "record")
	stub := "#!/bin/sh\n" +
		`printf 'args=%s\n' "$(printf '%s ' "$@"|tr '\n' ' ')" >>"` + record + `"` + "\n" +
		`printf 'auth=%s\n' "${REDISCLI_AUTH:-}" >>"` + record + `"` + "\n" +
		`printf 'pgpassword=%s\n' "${PGPASSWORD:-}" >>"` + record + `"` + "\n"
	if err := os.WriteFile(filepath.Join(dir, cli), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	a, ok := storestate.AdapterFor(engine)
	if !ok {
		t.Fatalf("no %s adapter", engine)
	}
	argv := a.ReadCommand(storestate.Endpoint{Port: a.DefaultPort()})
	cmd := exec.Command(shell[0], append(shell[1:], argv[1:]...)...)
	cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin"}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Logf("read exited %v: %s", err, out)
	}
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("the store CLI never ran: %v", err)
	}
	// The first invocation is representative: every call gets the same credentials.
	first := strings.SplitN(string(data), "\n", 4)
	return strings.Join(first[:3], "\n")
}

func secretFile(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// shells are the POSIX shells store images carry that this host has: dash or
// bash as sh, and busybox ash.
func shells(t *testing.T) map[string][]string {
	out := map[string][]string{}
	if sh, err := exec.LookPath("sh"); err == nil {
		out["sh"] = []string{sh}
	}
	if busybox, err := exec.LookPath("busybox"); err == nil {
		out["busybox"] = []string{busybox, "sh"}
	}
	if len(out) == 0 {
		t.Skip("no POSIX shell")
	}
	return out
}

// Every engine finds its credentials the same way, in the store container's
// own environment: a variable naming the engine and a password (or a _FILE
// twin naming a mounted Secret), paired with the user variable of the same
// prefix. SDO never reads a Secret value itself.
func TestReadsDiscoverCredentialsGenerically(t *testing.T) {
	for name, test := range map[string]struct {
		engine storestate.Engine
		cli    string
		env    map[string]string
		want   []string
		absent []string
	}{
		"redis password":           {storestate.EngineRedis, "redis-cli", map[string]string{"REDIS_PASSWORD": "synthetic-a"}, []string{"auth=synthetic-a"}, nil},
		"redis cli auth":           {storestate.EngineRedis, "redis-cli", map[string]string{"REDISCLI_AUTH": "synthetic-b"}, []string{"auth=synthetic-b"}, nil},
		"redis password file":      {storestate.EngineRedis, "redis-cli", map[string]string{"REDIS_PASSWORD_FILE": ""}, []string{"auth=synthetic-file"}, nil},
		"redis prefixed password":  {storestate.EngineRedis, "redis-cli", map[string]string{"CACHE_REDIS_PASSWORD": "synthetic-c"}, []string{"auth=synthetic-c"}, nil},
		"redis ignores others":     {storestate.EngineRedis, "redis-cli", map[string]string{"MAIL_PASSWORD": "synthetic-d"}, []string{"auth=\n"}, []string{"synthetic-d"}},
		"postgres pair":            {storestate.EnginePostgres, "psql", map[string]string{"POSTGRES_USER": "owner", "POSTGRES_PASSWORD": "synthetic-e"}, []string{"-U owner", "pgpassword=synthetic-e"}, nil},
		"postgres alternate names": {storestate.EnginePostgres, "psql", map[string]string{"POSTGRESQL_USERNAME": "app", "POSTGRESQL_PASSWORD": "synthetic-f"}, []string{"-U app", "pgpassword=synthetic-f"}, nil},
		"postgres password file":   {storestate.EnginePostgres, "psql", map[string]string{"POSTGRES_USER": "owner", "POSTGRES_PASSWORD_FILE": ""}, []string{"-U owner", "pgpassword=synthetic-file"}, nil},
		"postgres libpq names":     {storestate.EnginePostgres, "psql", map[string]string{"PGUSER": "reader", "PGPASSWORD": "synthetic-g"}, []string{"-U reader", "pgpassword=synthetic-g"}, nil},
		"postgres user only":       {storestate.EnginePostgres, "psql", map[string]string{"POSTGRES_USER": "owner"}, []string{"-U owner"}, nil},
		"postgres default":         {storestate.EnginePostgres, "psql", nil, []string{"-U postgres"}, nil},
		"mongo root pair":          {storestate.EngineMongoDB, "mongosh", map[string]string{"MONGO_INITDB_ROOT_USERNAME": "admin", "MONGO_INITDB_ROOT_PASSWORD": "synthetic-h"}, []string{"-u admin -p synthetic-h --authenticationDatabase admin"}, nil},
		"mongo root password":      {storestate.EngineMongoDB, "mongosh", map[string]string{"MONGODB_ROOT_PASSWORD": "synthetic-i", "MONGODB_USERNAME": "app", "MONGODB_PASSWORD": "synthetic-j"}, []string{"-u root -p synthetic-i"}, []string{"synthetic-j"}},
		"mongo password file":      {storestate.EngineMongoDB, "mongosh", map[string]string{"MONGO_INITDB_ROOT_USERNAME": "admin", "MONGO_INITDB_ROOT_PASSWORD_FILE": ""}, []string{"-u admin -p synthetic-file"}, nil},
		"mongo without auth":       {storestate.EngineMongoDB, "mongosh", nil, []string{"--quiet --port 27017"}, []string{" -u "}},
	} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range test.env {
				if strings.HasSuffix(k, "_FILE") {
					v = secretFile(t, "synthetic-file")
				}
				env[k] = v
			}
			for shellName, shell := range shells(t) {
				got := runRead(t, shell, test.engine, test.cli, env)
				for _, want := range test.want {
					if !strings.Contains(got+"\n", want) {
						t.Errorf("%s: record lacks %q:\n%s", shellName, want, got)
					}
				}
				for _, absent := range test.absent {
					if strings.Contains(got, absent) {
						t.Errorf("%s: record has %q:\n%s", shellName, absent, got)
					}
				}
			}
		})
	}
}

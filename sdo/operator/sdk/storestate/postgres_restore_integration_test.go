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

// The server restore compares the server-scope value with the baseline and
// sets it only when they differ, however the session's own value is scoped.
func TestPostgresServerRestoreJudgesTheServerScopeValue(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	on := storestate.Item{Kind: storestate.KindSetting, Name: "default_transaction_read_only", Value: "on"}
	off := storestate.Item{Kind: storestate.KindSetting, Name: "default_transaction_read_only", Value: "off"}
	a := adapterFor(t, storestate.EnginePostgres).(storestate.Restorer)
	cmds, err := a.Restore(storestate.Endpoint{Port: 5432}, storestate.Change{Before: &off, After: &on}, storestate.Facts{}, storestate.RestoreOptions{})
	if err != nil || len(cmds) != 1 {
		t.Fatalf("restore = %v, %v", cmds, err)
	}
	for name, test := range map[string]struct {
		reply   string // the settings query's answer
		wantSet bool
	}{
		"session value is scoped, server is at baseline": {"scoped:off", false},
		"session value is the server's and is on":        {"server:on", true},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			record := filepath.Join(dir, "record")
			stub := "#!/bin/sh\nfor a in \"$@\";do case \"$a\" in 'ALTER SYSTEM SET'*) echo set >>" + record + ";;'SELECT CASE WHEN source'*) echo '" + test.reply + "';;'SELECT CASE WHEN s.vartype'*) exit 1;; esac;done\n"
			if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stub), 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(sh, cmds[0][1:]...)
			cmd.Env = []string{"PATH=" + dir + ":/usr/bin:/bin"}
			out, _ := cmd.CombinedOutput()
			data, _ := os.ReadFile(record)
			if got := strings.Contains(string(data), "set"); got != test.wantSet {
				t.Fatalf("ALTER SYSTEM SET ran = %v, want %v (output %q)", got, test.wantSet, out)
			}
		})
	}
}

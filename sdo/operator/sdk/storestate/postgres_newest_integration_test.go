//go:build integration

package storestate_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// newestRead runs the postgres read against a stub psql with one database whose
// newest-objects query prints newest and exits.
func newestRead(t *testing.T, newest string) (string, storestate.Facts) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "newest"), []byte(newest), 0o644); err != nil {
		t.Fatal(err)
	}
	stub := "#!/bin/sh\nfor a in \"$@\";do case \"$a\" in *statement_timeout=1000*) cat " + dir + "/newest;;" +
		"'SELECT datname FROM pg_database WHERE datallowconn'*) echo appdb;;*\"'lock' k\"*) printf 'grant\\tapp\\t\\tconnect=true\\n';;" +
		"\"SELECT 'role'\"*) printf 'role\\tprimary\\n';;'SELECT datname,datallowconn'*) printf 'appdb\\tt\\t-1\\n';;esac;done\n"
	if err := os.WriteFile(filepath.Join(dir, "psql"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	a := adapterFor(t, storestate.EnginePostgres)
	argv := a.ReadCommand(storestate.Endpoint{Port: 5432})
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("read: %v\n%s", err, out)
	}
	facts, err := a.Parse(string(out))
	if err != nil {
		t.Fatalf("parse: %v\n%s", err, out)
	}
	return string(out), facts
}

// The newest objects print rarest kind first and only in whole lines that fit
// in bytes; a cut or a failed query is marked and never costs the catalog.
func TestPostgresNewestObjectsFitOrAreMarkedCut(t *testing.T) {
	row := func(kind, name string) string { return "newest\t" + kind + " 5\tpublic.t\t" + name + "\n" }
	end := "newest\tend\t\t\n"
	catalog := func(f storestate.Facts) bool {
		_, ok := f.Lookup(storestate.Key{Kind: storestate.KindPrincipal, Scope: "appdb", Name: "app"})
		return ok && len(f.Unread) == 0
	}
	out, f := newestRead(t, row("index", "i")+row("trigger", "t")+end)
	if !f.NewestComplete || len(f.Newest) != 2 || f.Newest[0].Kind != "trigger" || !catalog(f) {
		t.Fatalf("complete: %+v\n%s", f.Newest, out)
	}
	for name, failed := range map[string]string{"error": "ERROR:  canceling statement due to statement timeout\n", "half": row("trigger", "t") + "ERROR:  x\n", "silent": ""} {
		if out, f := newestRead(t, failed); f.NewestComplete || !catalog(f) || strings.Contains(out, "ERROR") {
			t.Errorf("%s: complete %v\n%s", name, f.NewestComplete, out)
		}
	}
	var many strings.Builder
	for i := range 60 {
		many.WriteString(row("index", fmt.Sprintf("%02d%s", i, strings.Repeat("é", 50))))
	}
	out, f = newestRead(t, many.String()+row("trigger", "t")+end)
	if len(out) > 3900 || len(out) < 3000 || f.NewestComplete || f.Newest[0].Kind != "trigger" || !strings.HasSuffix(out, "\ncut\n") || !catalog(f) {
		t.Fatalf("many: %d bytes, complete %v\n%s", len(out), f.NewestComplete, out)
	}
}

package storestate_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/sdk/storestate"
)

// fakeConsul stands in for the image's consul CLI on a fresh agent: a leader
// with no services of its own beyond "consul" and an empty KV, for which
// `kv get -keys` prints one blank line.
const fakeConsul = `#!/bin/sh
case "$1 $2" in
info*) printf '\tleader = true\n\tserver = true\n' ;;
"catalog services") echo consul ;;
"catalog nodes") printf 'Node ID Address DC\nnode-1 abc 127.0.0.1 dc1\n' ;;
members*) printf 'Node Address Status Type\nnode-1 127.0.0.1:8301 alive server\n' ;;
"kv get") echo ;;
"acl token") echo "ACL support disabled"; exit 1 ;;
esac
`

// An empty consul KV is a valid healthy state: the read yields no keys, never
// a prefix with an empty name.
func TestConsulEmptyKVReadsAsNoKeys(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "consul"), []byte(fakeConsul), 0o755); err != nil {
		t.Fatal(err)
	}
	argv := adapterFor(t, storestate.EngineConsul).ReadCommand(storestate.Endpoint{Port: 8500})
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("read: %v\n%s", err, out)
	}
	f := parseAs(t, storestate.EngineConsul, string(out))
	if got := lookup(t, f, storestate.KindNamespace, "", "kv-keys"); got != "0" {
		t.Fatalf("kv-keys = %q, want 0; items %+v", got, f.Items)
	}
	for _, it := range f.Items {
		if it.Scope == "kv" {
			t.Fatalf("empty KV produced prefix %+v", it)
		}
	}
}

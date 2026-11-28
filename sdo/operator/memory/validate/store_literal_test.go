package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// storeLiteralCapture is a corpus snapshot whose store read names the
// synthetic collection shopdb/user-orders.
func storeLiteralCapture() string {
	stdout := "== server\nrole\tstandalone\nconn\t4\t100\n== params\n== locks\nfsync\t0\nwaiting\t0\n== users\n== catalog\ndb\tshopdb\t0\ncoll\tshopdb\tuser-orders\t12\t-\n"
	id := storestate.QueryID(storestate.EngineMongoDB, "orders-store")
	raw, err := json.Marshal(map[string]any{"namespace": "shop", "pod_reads": map[string]podread.Result{id: {
		Baseline: []podread.Reading{{Namespace: "shop", Pod: "orders-store-0", Stdout: stdout}},
	}}})
	if err != nil {
		panic(err)
	}
	return string(raw)
}

// storeDiagnose is the generic cache diagnosis made to read one collection by
// its name instead of the bound store's own state.
var storeDiagnose = strings.Replace(genericDiagnose, `strings.Contains(strings.ToUpper(sample.Error), "NOPERM")`, `strings.Contains(sample.Error, "shopdb/user-orders")`, 1)

func gateStoreLiteral(name string, edit func(t *testing.T, r string), rules ...string) gateRow {
	row := gateComponentLiteral(name, false, []string{"cache-acl-restore"}, edit, rules...)
	request := row.request
	row.request = func(r *Request) {
		request(r)
		// The controller's capture of the incident carries the store read.
		r.CaptureDirectory = filepath.Join(r.Root, "..", "capture")
		if err := os.MkdirAll(filepath.Join(r.CaptureDirectory, "healthy"), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(filepath.Join(r.CaptureDirectory, "healthy", "healthy.json"), []byte(storeLiteralCapture()), 0o600); err != nil {
			panic(err)
		}
	}
	return row
}

var storeLiteralRows = []gateRow{
	gateStoreLiteral("new playbook names a store collection captured state shows",
		writePlaybook("cache-acl-restore", storeDiagnose, genericMitigate), RulePlaybookComponentLiteral),
	gateStoreLiteral("new playbook names no store collection",
		writePlaybook("cache-acl-restore", genericDiagnose, genericMitigate)),
}

func init() { gateRows = append(gateRows, storeLiteralRows...) }

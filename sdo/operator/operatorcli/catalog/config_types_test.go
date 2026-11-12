package catalog_test

import (
	"strings"
	"testing"

	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/internal/archcheck"
	"sdo.dev/operator/operatorcli/catalog/images"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
)

// TestEveryPluginConfigTypeIsInTheArchcheckRegistry fails when a built-in
// plugin decodes a config message that archcheck's R4 registry
// (archcheck.Rules().ConfigTypes) does not list. R4 is an explicit registry,
// so a config type missing from it is invisible to the rule: any package could
// then construct or decode the type without the owner.
func TestEveryPluginConfigTypeIsInTheArchcheckRegistry(t *testing.T) {
	registered := map[string]bool{}
	for _, ct := range archcheck.Rules().ConfigTypes {
		registered[ct.Name] = true
	}
	used := map[string]string{} // message name -> who decodes it
	protoName := func(full string) string { return full[strings.LastIndex(full, ".")+1:] }

	for _, d := range renderers.Catalog().Describe() {
		used[protoName(d.ConfigType)] = "renderer " + d.ID
	}
	imgs, err := images.Catalog(images.Config{Builder: buildkit.Builder{AllowedRegistryHost: "registry.invalid"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range imgs.Describe() {
		used[protoName(d.ConfigType)] = "image source " + d.ID
	}
	ups := upstreams.Catalog()
	for _, id := range ups.IDs() {
		packed, err := ups.ParseConfig(id, []byte("{}"))
		if err != nil {
			t.Fatalf("upstream %s: %v", id, err)
		}
		used[protoName(packed.GetTypeUrl())] = "upstream watcher " + id
	}
	if len(used) < 6 {
		t.Fatalf("only %d plugin config types discovered (%v): the discovery is not seeing the catalogs", len(used), used)
	}
	for name, who := range used {
		if !registered[name] {
			t.Errorf("%s is the config of %s but is not in archcheck.Rules().ConfigTypes (internal/archcheck/rules.go)", name, who)
		}
	}
}

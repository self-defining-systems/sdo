package layout

import (
	"strings"
	"testing"
)

// The parse hint lists every manifest key, read from the struct's YAML tags.
func TestManifestKeysNameEveryField(t *testing.T) {
	keys := ManifestKeys()
	for _, want := range []string{"apiVersion", "sdkVersion", "watches{apiVersion, kind, namespace}", "persistence{firing, clearing}", "batching{", "debounce", "possiblePlaybooks", "originatingIncident"} {
		if !strings.Contains(keys, want) {
			t.Fatalf("manifest keys %q lack %q", keys, want)
		}
	}
}

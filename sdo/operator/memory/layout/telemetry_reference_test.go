package layout

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// telemetryReference is the designer's schema reference for the two binding
// files; agents copy its examples, so they must parse with the strict decoders.
var telemetryReference = filepath.Join("..", "..", "..", "skills", "sdo-operationalize", "references", "telemetry.md")

var yamlBlock = regexp.MustCompile("(?s)```yaml\n(.*?)```")

// The reference's sources.yaml and backends.yaml examples are exactly what the
// broker decodes: a renamed or added field in the Go types fails here instead
// of leaving the designer to guess the schema from validator messages.
func TestTelemetryReferenceExamplesParseStrictly(t *testing.T) {
	data, err := os.ReadFile(telemetryReference)
	if err != nil {
		t.Fatal(err)
	}
	var sources, backends int
	for _, match := range yamlBlock.FindAllSubmatch(data, -1) {
		block := match[1]
		switch {
		case strings.Contains(string(block), "\nsources:"):
			sources++
			if _, err := ParseTelemetrySources(block); err != nil {
				t.Errorf("sources example does not parse: %v\n%s", err, block)
			}
		case strings.Contains(string(block), "\nbackends:"):
			backends++
			if _, err := ParseTelemetryBackends(block); err != nil {
				t.Errorf("backends example does not parse: %v\n%s", err, block)
			}
		}
	}
	if sources != 1 || backends != 1 {
		t.Fatalf("reference has %d sources and %d backends examples, want one of each", sources, backends)
	}
}

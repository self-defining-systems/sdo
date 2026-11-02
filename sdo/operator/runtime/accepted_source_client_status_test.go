package runtime

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Result.Raw drops the server Status; no production request in this package
// may end in Raw() outside bodyOf, which reads Result.Error first.
func TestProductionRequestsNeverReadRawBeforeError(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(body), "\n") {
			if (strings.Contains(line, ".Raw()") || strings.Contains(line, ".DoRaw(")) && !strings.Contains(line, "return result.Raw()") {
				t.Errorf("%s:%d reads a request body without Result.Error: %s", file, number+1, strings.TrimSpace(line))
			}
		}
	}
}

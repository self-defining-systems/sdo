package main

import (
	"os"
	"path/filepath"
	"testing"
)

// docs/config.md is generated; a change to settings.Config or its defaults that does not regenerate it fails here as
// well as in scripts/check_go_errors.sh.
func TestConfigReferenceIsCurrent(t *testing.T) {
	committed, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "docs", "config.md"))
	if err != nil {
		t.Fatal(err)
	}
	if string(committed) != render() {
		t.Fatal("docs/config.md is out of date: run scripts/gen_config_docs.sh")
	}
}

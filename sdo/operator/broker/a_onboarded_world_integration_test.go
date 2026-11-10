//go:build integration

package broker

import (
	"os"
	"slices"
	"strings"
	"testing"
)

// Go runs tests in file-name order, then source order. The first test of the
// package to need an onboarded world pays for building its template, so the
// tests that build each variant live in the file that sorts first: no
// behavior test, whatever its file is called, carries the setup in its
// duration.
func TestOnboardedWorldWithADeployerEditBuilds(t *testing.T) { onboardedTemplate(t, true) }

func TestOnboardedWorldOfAProvidedDeploymentBuilds(t *testing.T) { onboardedTemplate(t, false) }

// The template builders must precede every other test file of the package.
func TestOnboardedTemplateBuildersSortFirst(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), "_test.go") {
			files = append(files, e.Name())
		}
	}
	slices.Sort(files)
	if files[0] != "a_onboarded_world_integration_test.go" {
		t.Fatalf("%s sorts before the onboarded-world template builders; tests there pay for building the shared world. Rename it or move the builders ahead of it", files[0])
	}
}

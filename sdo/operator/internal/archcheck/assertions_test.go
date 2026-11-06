package archcheck

import (
	"strings"
	"testing"
)

func TestUncheckedAssertionFixture(t *testing.T) {
	findings, err := UncheckedAssertions("testdata/unchecked_assertion")
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || !strings.Contains(findings[0], "a.go:7:") {
		t.Fatalf("findings = %v, want only the unchecked assertion to a named interface", findings)
	}
}

// TestNoUncheckedCapabilityAssertions keeps injected collaborators typed by the
// capability they need. A Coordinator.Readiness asserted to LiveChecker at the
// point of use panicked the controller end-to-end run; the field is now the
// interface and the compiler enforces it.
//
//ci:whole-tree
func TestNoUncheckedCapabilityAssertions(t *testing.T) {
	findings, err := UncheckedAssertions("../..")
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) > 0 {
		t.Fatalf("unchecked type assertions (type the dependency, or use the comma-ok form; reflection and generics take //assertcheck:ok <reason>):\n%s", strings.Join(findings, "\n"))
	}
}

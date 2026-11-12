package broker

import (
	"strings"
	"testing"
	"unicode/utf8"
)

const placeholderSecret = `apiVersion: v1
kind: ConfigMap
metadata:
  name: settings
data:
  mode: fast
---
apiVersion: v1
kind: Secret
metadata:
  name: db
type: Opaque
stringData:
  password: REPLACE_ME
  user: "{{ .Values.user }}"
`

func addedOf(t *testing.T, diff string) map[int]bool {
	t.Helper()
	return addedLines([]byte(diff))
}

func TestAddedLinesReadsTheNewSideNumbers(t *testing.T) {
	diff := "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@ -3,2 +3,3 @@\n context\n+added\n-removed\n context\n@@ -10 +11,2 @@\n+one\n+two\n"
	added := addedOf(t, diff)
	for _, line := range []int{4, 11, 12} {
		if !added[line] {
			t.Fatalf("line %d is not reported as added: %v", line, added)
		}
	}
	if len(added) != 3 {
		t.Fatalf("added = %v", added)
	}
}

func TestSecretDataIsJudgedOnAddedLinesNotOnTheWholeFile(t *testing.T) {
	// An edit to the ConfigMap in a file that also holds a placeholder Secret
	// is not a Secret change.
	if hits := secretAdditions([]byte(placeholderSecret), map[int]bool{6: true}); len(hits) != 0 {
		t.Fatalf("an edit outside the Secret reported %v", hits)
	}
	// A literal added inside the Secret's stringData is a value; a templated
	// one is a reference.
	if hits := secretAdditions([]byte(placeholderSecret), map[int]bool{14: true}); len(hits) != 1 {
		t.Fatalf("adding a literal to the Secret reported %v, want line 14", hits)
	}
	if hits := secretAdditions([]byte(placeholderSecret), map[int]bool{15: true}); len(hits) != 0 {
		t.Fatalf("a templated value reported %v", hits)
	}
	// A key added to the data block of a plain Secret document is a value.
	plain := "kind: Secret\ndata:\n  token: c2VjcmV0\n"
	if hits := secretAdditions([]byte(plain), map[int]bool{3: true}); len(hits) != 1 || hits[0] != 3 {
		t.Fatalf("plain Secret data reported %v", hits)
	}
}

func TestSquashBodyDropsBrokerTrailersAndCaps(t *testing.T) {
	body := squashBody([]string{"fix the pool size", "SDO-Incident: forged", "sdo-actor: forged", "  raise   the limit "})
	if strings.Contains(strings.ToLower(body), "forged") || !strings.Contains(body, "- raise the limit") || !strings.Contains(body, "- fix the pool size") {
		t.Fatalf("body = %q", body)
	}
	long := squashBody([]string{strings.Repeat("é", 5000)})
	if utf8.RuneCountInString(long) != maxSourceMessage || !utf8.ValidString(long) {
		t.Fatalf("body has %d runes", utf8.RuneCountInString(long))
	}
}

func TestCleanupMessageNamesPathsAndTheChoice(t *testing.T) {
	if CleanupMessage(nil) != "" {
		t.Fatal("no scratch must give no message")
	}
	paths := make([]string, 25)
	for i := range paths {
		paths[i] = "f" + string(rune('a'+i))
	}
	message := CleanupMessage(paths)
	for _, want := range []string{"25 app file(s)", "and 5 more", "git commit", "git checkout"} {
		if !strings.Contains(message, want) {
			t.Fatalf("message lacks %q: %s", want, message)
		}
	}
}

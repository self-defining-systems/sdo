package patchset

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

const reasonText = "The chart's db-migrate Job has no imagePullPolicy and fails against the local registry."

const modifyDiff = `diff --git a/charts/x/job.yaml b/charts/x/job.yaml
index 1111111111111111111111111111111111111111..2222222222222222222222222222222222222222 100644
--- a/charts/x/job.yaml
+++ b/charts/x/job.yaml
@@ -1,3 +1,4 @@
 a
+b
 c
 d
`

func preamble(extra ...string) string {
	return "SDO-Patch: 1\nOwner: deployer\nReason: |\n  " + reasonText + "\n" + strings.Join(extra, "") + "\n"
}

func valid() string { return preamble() + modifyDiff }

func mustKind(t *testing.T, err error, kind Kind) *PatchError {
	t.Helper()
	var pe *PatchError
	if !errors.As(err, &pe) {
		t.Fatalf("want *PatchError %s, got %v", kind, err)
	}
	if pe.Kind != kind {
		t.Fatalf("kind = %s, want %s (%v)", pe.Kind, kind, pe)
	}
	return pe
}

func TestParseValidPatch(t *testing.T) {
	p, err := Parse("0001-x.patch", []byte(valid()))
	if err != nil {
		t.Fatal(err)
	}
	if p.Header.Owner != OwnerDeployer || p.Header.Reason != reasonText {
		t.Fatalf("header = %+v", p.Header)
	}
	if len(p.Files) != 1 || p.Files[0].Op != OpModify || p.Files[0].Path != "charts/x/job.yaml" {
		t.Fatalf("files = %+v", p.Files)
	}
	if !strings.HasPrefix(string(p.Diff), "diff --git ") || strings.Contains(string(p.Diff), "SDO-Patch") {
		t.Fatalf("diff must start at the first diff line: %q", p.Diff)
	}
	if string(p.Raw()) != valid() {
		t.Fatal("raw content changed")
	}
}

func TestParseResponderHeaderAndOptionalKeys(t *testing.T) {
	data := "SDO-Patch: 1\nOwner: responder\nIncident: inc-7\nReason: " + reasonText + "\nUpstream-Base: " + strings.Repeat("a", 40) + "\nUpstream-Fix: issue 12\n\n" + modifyDiff
	p, err := Parse("0002-r.patch", []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if p.Header.Incident != "inc-7" || p.Header.UpstreamBase != strings.Repeat("a", 40) || p.Header.UpstreamFix != "issue 12" {
		t.Fatalf("header = %+v", p.Header)
	}
}

func TestParseAddDeleteRenameAndModeChange(t *testing.T) {
	diff := `diff --git a/new.txt b/new.txt
new file mode 100644
index 0000000000000000000000000000000000000000..1111111111111111111111111111111111111111
--- /dev/null
+++ b/new.txt
@@ -0,0 +1 @@
+x
diff --git a/old.txt b/old.txt
deleted file mode 100755
index 1111111111111111111111111111111111111111..0000000000000000000000000000000000000000
--- a/old.txt
+++ /dev/null
@@ -1 +0,0 @@
-x
diff --git a/a.txt b/b.txt
similarity index 100%
rename from a.txt
rename to b.txt
diff --git a/run.sh b/run.sh
old mode 100644
new mode 100755
`
	p, err := Parse("0001-x.patch", []byte(preamble()+diff))
	if err != nil {
		t.Fatal(err)
	}
	want := []FileChange{
		{Op: OpAdd, Path: "new.txt", Mode: "100644"},
		{Op: OpDelete, Path: "old.txt", Mode: "100755"},
		{Op: OpRename, OldPath: "a.txt", Path: "b.txt"},
		{Op: OpModify, Path: "run.sh", Mode: "100755"},
	}
	if fmt.Sprint(p.Files) != fmt.Sprint(want) {
		t.Fatalf("files = %+v, want %+v", p.Files, want)
	}
	if got := strings.Join(p.Paths(), ","); got != "a.txt,b.txt,new.txt,old.txt,run.sh" {
		t.Fatalf("paths = %s", got)
	}
}

// Hunk bodies that look like diff headers are content, not new sections.
func TestParseHunkContentIsNotReinterpreted(t *testing.T) {
	diff := "diff --git a/f b/f\nindex 1111111..2222222 100644\n--- a/f\n+++ b/f\n@@ -1,2 +1,3 @@\n a\n+diff --git a/.sdo/x b/.sdo/x\n b\n"
	p, err := Parse("0001-x.patch", []byte(preamble()+diff))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Files) != 1 {
		t.Fatalf("files = %+v", p.Files)
	}
}

func TestParseRejectsBadPatches(t *testing.T) {
	sdoDiff := strings.ReplaceAll(modifyDiff, "charts/x/job.yaml", ".sdo/goal.md")
	cases := []struct {
		name string
		data string
		kind Kind
		frag string
	}{
		{"no newline", strings.TrimSuffix(valid(), "\n"), KindMalformed, "newline"},
		{"empty", "", KindMalformed, "newline"},
		{"no diff", "SDO-Patch: 1\nOwner: deployer\nReason: x\n", KindMalformed, "no diff"},
		{"missing version", "Owner: deployer\n\n" + modifyDiff, KindMalformed, "SDO-Patch: 1"},
		{"wrong version", strings.Replace(valid(), "SDO-Patch: 1", "SDO-Patch: 2", 1), KindMalformed, "SDO-Patch: 1"},
		{"unknown key", preamble("Foo: bar\n") + modifyDiff, KindMalformed, "unknown header key Foo"},
		{"duplicate key", preamble("Owner: deployer\n") + modifyDiff, KindMalformed, "duplicate header key Owner"},
		{"bad owner", strings.Replace(valid(), "Owner: deployer", "Owner: human", 1), KindMalformed, "Owner"},
		{"missing reason", "SDO-Patch: 1\nOwner: deployer\n\n" + modifyDiff, KindMalformed, "missing Reason"},
		{"empty reason", "SDO-Patch: 1\nOwner: deployer\nReason: |\n\n" + modifyDiff, KindMalformed, "Reason must be"},
		{"short reason", "SDO-Patch: 1\nOwner: deployer\nReason: too short\n\n" + modifyDiff, KindMalformed, "Reason must be"},
		{"long reason", "SDO-Patch: 1\nOwner: deployer\nReason: " + strings.TrimSpace(strings.Repeat("a b ", 600)) + "\n\n" + modifyDiff, KindMalformed, "Reason must be"},
		{"path list reason", "SDO-Patch: 1\nOwner: deployer\nReason: charts/x/job.yaml, charts/x/other.yaml, charts/x/third.yaml\n\n" + modifyDiff, KindMalformed, "list of paths"},
		{"responder without incident", "SDO-Patch: 1\nOwner: responder\nReason: " + reasonText + "\n\n" + modifyDiff, KindMalformed, "Incident"},
		{"deployer with incident", preamble("Incident: inc-1\n") + modifyDiff, KindMalformed, "Incident"},
		{"bad upstream base", preamble("Upstream-Base: main\n") + modifyDiff, KindMalformed, "Upstream-Base"},
		{"non key preamble", preamble("just prose\n") + modifyDiff, KindMalformed, "Key: value"},
		{"sdo target", preamble() + sdoDiff, KindForbidden, ".sdo/"},
		{"sdo target mixed case", preamble() + strings.ReplaceAll(modifyDiff, "charts/x/job.yaml", ".SDO/goal.md"), KindForbidden, ".sdo/"},
		{"dot dot", preamble() + strings.ReplaceAll(modifyDiff, "charts/x/job.yaml", "a/../b"), KindForbidden, "clean"},
		{"git dir", preamble() + strings.ReplaceAll(modifyDiff, "charts/x/job.yaml", "x/.git/config"), KindForbidden, ".git"},
		{"binary", preamble() + "diff --git a/i.png b/i.png\nindex 1111111..2222222 100644\nBinary files a/i.png and b/i.png differ\n", KindForbidden, "binary"},
		{"git binary patch", preamble() + "diff --git a/i.png b/i.png\nindex 1111111..2222222 100644\nGIT binary patch\nliteral 1\n", KindForbidden, "binary"},
		{"nul byte", valid() + "\x00\n", KindForbidden, "binary"},
		{"symlink add", preamble() + "diff --git a/l b/l\nnew file mode 120000\nindex 0000000..1111111\n--- /dev/null\n+++ b/l\n@@ -0,0 +1 @@\n+target\n\\ No newline at end of file\n", KindForbidden, "120000"},
		{"gitlink", preamble() + "diff --git a/m b/m\nnew file mode 160000\nindex 0000000..1111111\n", KindForbidden, "160000"},
		{"mode change to symlink", preamble() + "diff --git a/f b/f\nold mode 100644\nnew mode 120000\n", KindForbidden, "120000"},
		{"index mode symlink", strings.Replace(valid(), "100644", "120000", 1), KindForbidden, "120000"},
		{"rename out of tree", preamble() + "diff --git a/a.txt b/.sdo/x.txt\nsimilarity index 100%\nrename from a.txt\nrename to .sdo/x.txt\n", KindForbidden, ".sdo/"},
		{"rename in from sdo", preamble() + "diff --git a/.sdo/x b/y\nrename from .sdo/x\nrename to y\n", KindForbidden, ".sdo/"},
		{"copy out", preamble() + "diff --git a/a.txt b/../x.txt\ncopy from a.txt\ncopy to ../x.txt\n", KindForbidden, "clean"},
		{"quoted path", preamble() + "diff --git \"a/x\\ty\" \"b/x\\ty\"\n", KindForbidden, "unsupported"},
		{"spaces in path", preamble() + "diff --git a/x y b/x y\n", KindForbidden, "unsupported"},
		{"header mismatch", preamble() + strings.Replace(modifyDiff, "+++ b/charts/x/job.yaml", "+++ b/charts/x/other.yaml", 1), KindMalformed, "inconsistent"},
		{"rename mismatch", preamble() + "diff --git a/a b/b\nrename from a\nrename to c\n", KindMalformed, "rename"},
		{"empty section", preamble() + "diff --git a/a b/a\n", KindMalformed, "nothing"},
		{"short hunk", preamble() + strings.Replace(modifyDiff, "@@ -1,3 +1,4 @@", "@@ -1,9 +1,10 @@", 1), KindMalformed, "shorter"},
		{"long hunk", preamble() + strings.Replace(modifyDiff, "@@ -1,3 +1,4 @@", "@@ -1,1 +1,1 @@", 1), KindMalformed, "after hunk"},
		{"trailing garbage", valid() + "trailing\n", KindMalformed, "unexpected line"},
		{"unknown ext header", preamble() + "diff --git a/a b/a\nweird thing\n", KindMalformed, "unrecognized"},
		{"too big", valid() + strings.Repeat("x", MaxPatchBytes), KindForbidden, "limit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Parse("0001-x.patch", []byte(c.data))
			pe := mustKind(t, err, c.kind)
			if !strings.Contains(pe.Detail, c.frag) {
				t.Fatalf("detail %q lacks %q", pe.Detail, c.frag)
			}
			if pe.Patch != "0001-x.patch" {
				t.Fatalf("patch name = %q", pe.Patch)
			}
		})
	}
}

// Property: the parser never panics, and an accepted patch satisfies the path,
// mode and header invariants no matter how the input was mutated. Seeded and
// capped, so it is deterministic.
func TestParseMutationsKeepInvariants(t *testing.T) {
	seeds := []string{valid(), preamble() + "diff --git a/a b/b\nrename from a\nrename to b\n", preamble() + "diff --git a/n b/n\nnew file mode 100755\nindex 0000000..1111111\n--- /dev/null\n+++ b/n\n@@ -0,0 +1 @@\n+x\n"}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		b := []byte(seeds[rng.IntN(len(seeds))])
		for n := rng.IntN(4) + 1; n > 0; n-- {
			switch rng.IntN(4) {
			case 0:
				b[rng.IntN(len(b))] = byte(rng.IntN(256))
			case 1:
				at := rng.IntN(len(b))
				b = append(b[:at], b[at+rng.IntN(len(b)-at)+0:]...)
			case 2:
				at := rng.IntN(len(b))
				b = append(b[:at], append([]byte("\n"), b[at:]...)...)
			default:
				b = append(b, byte(rng.IntN(256)))
			}
			if len(b) == 0 {
				b = []byte("x")
			}
		}
		checkInvariants(t, b)
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte(valid()))
	f.Fuzz(func(t *testing.T, b []byte) { checkInvariants(t, b) })
}

func checkInvariants(t *testing.T, b []byte) {
	t.Helper()
	p, err := Parse("0001-x.patch", b)
	if err != nil {
		var pe *PatchError
		if !errors.As(err, &pe) || (pe.Kind != KindMalformed && pe.Kind != KindForbidden) {
			t.Fatalf("untyped parse error %v for %q", err, b)
		}
		return
	}
	if len(p.Files) == 0 || p.Header.Owner == "" {
		t.Fatalf("accepted patch without files or owner: %q", b)
	}
	for _, path := range p.Paths() {
		if why := badPath(path); why != "" {
			t.Fatalf("accepted bad path %q (%s) from %q", path, why, b)
		}
	}
	for _, f := range p.Files {
		if f.Mode != "" && f.Mode != "100644" && f.Mode != "100755" {
			t.Fatalf("accepted mode %s from %q", f.Mode, b)
		}
	}
	if strings.Contains(string(p.Diff), "SDO-Patch:") && !strings.Contains(string(b[len(b)-len(p.Diff):]), "SDO-Patch:") {
		t.Fatal("diff is not a suffix of the file")
	}
}

package patchset

import (
	"fmt"
	"strings"
	"testing"
)

func file(name, data string) File { return File{Name: name, Data: []byte(data)} }

func TestParseSeriesOrdersByNameAndDigestBindsContent(t *testing.T) {
	a := file("0002-b.patch", valid())
	b := file("0001-a.patch", strings.Replace(valid(), "+b\n", "+B\n", 1))
	s, err := ParseSeries([]File{a, b})
	if err != nil {
		t.Fatal(err)
	}
	ps := s.Patches()
	if len(ps) != 2 || ps[0].Name != "0001-a.patch" || ps[1].Name != "0002-b.patch" {
		t.Fatalf("order = %v, %v", ps[0].Name, ps[1].Name)
	}
	s2, _ := ParseSeries([]File{b, a})
	if s.Digest() != s2.Digest() {
		t.Fatal("digest depends on input order")
	}
	changed := file("0002-b.patch", strings.Replace(valid(), "Job has", "Job really has", 1))
	s3, err := ParseSeries([]File{changed, b})
	if err != nil {
		t.Fatal(err)
	}
	if s3.Digest() == s.Digest() {
		t.Fatal("digest ignores content")
	}
	renamed, _ := ParseSeries([]File{file("0003-b.patch", valid()), b})
	if renamed.Digest() == s.Digest() {
		t.Fatal("digest ignores names")
	}
}

func TestDigestGolden(t *testing.T) {
	var empty Series
	if got := empty.Digest(); got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatalf("empty digest = %s", got)
	}
	// Git blob ID of "hello\n" is ce013625030ba8dba906f756967f9e9ca394464a.
	if got := BlobID([]byte("hello\n")); got != "ce013625030ba8dba906f756967f9e9ca394464a" {
		t.Fatalf("blob id = %s", got)
	}
}

func TestParseSeriesRejects(t *testing.T) {
	big := make([]File, MaxPatches+1)
	for i := range big {
		big[i] = file("0001-a.patch", valid())
	}
	var filled []File
	// Each patch is under the per-patch cap but the total passes 2 MiB.
	pad := preamble("Upstream-Fix: " + strings.Repeat("y", 200) + "\n")
	body := "diff --git a/f b/f\nindex 1111111..2222222 100644\n--- a/f\n+++ b/f\n@@ -0,0 +1,2000 @@\n" + strings.Repeat("+"+strings.Repeat("z", 100)+"\n", 2000)
	for i := 0; i < 11; i++ {
		filled = append(filled, file(fmt.Sprintf("%04d-a.patch", i+1), pad+body))
	}
	cases := []struct {
		name  string
		files []File
		kind  Kind
		frag  string
	}{
		{"bad name", []File{file("fix.patch", valid())}, KindMalformed, "NNNN-slug"},
		{"uppercase name", []File{file("0001-Fix.patch", valid())}, KindMalformed, "NNNN-slug"},
		{"duplicate number", []File{file("0001-a.patch", valid()), file("0001-b.patch", valid())}, KindMalformed, "already used"},
		{"too many", big, KindForbidden, "limit"},
		{"total size", filled, KindForbidden, "series exceeds"},
		{"bad member", []File{file("0001-a.patch", "nope\n")}, KindMalformed, "no diff"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseSeries(c.files)
			pe := mustKind(t, err, c.kind)
			if !strings.Contains(pe.Detail, c.frag) {
				t.Fatalf("detail %q lacks %q", pe.Detail, c.frag)
			}
		})
	}
}

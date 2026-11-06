//go:build integration

package patchset

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const longReason = "The chart renders the job without the retry limit the application needs to start."

func readFile(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func exists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

// patchedFixture is an accepted repository whose commit carries one deployer
// patch, plus a clone of it as a broker would hand to an agent.
func patchedFixture(t *testing.T) (accepted, work string) {
	t.Helper()
	accepted = fixture(t, baseFiles)
	p1 := patchFor(t, accepted, "deployer", func() { put(t, accepted, "charts/x/job.yaml", "a\nb\nB2\nc\nd\n") })
	put(t, accepted, Directory+"/0001-retry.patch", p1)
	git(t, accepted, "add", "-A")
	git(t, accepted, "commit", "-q", "-m", "patch")
	work = filepath.Join(t.TempDir(), "checkout")
	git(t, accepted, "clone", "-q", accepted, work)
	return accepted, work
}

func TestMaterializeAppliesTheSeriesToFilesOnly(t *testing.T) {
	_, work := patchedFixture(t)
	head := git(t, work, "rev-parse", "HEAD")
	view, err := Materialize(context.Background(), work)
	if err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, work, "charts/x/job.yaml"); got != "a\nb\nB2\nc\nd\n" {
		t.Fatalf("working file = %q, want the patched content", got)
	}
	if git(t, work, "rev-parse", "HEAD") != head || git(t, work, "diff", "--cached", "--name-only") != "" {
		t.Fatal("materialization touched the commit or the index")
	}
	if view.EffectiveTree == view.BaseTree {
		t.Fatal("effective tree equals the base tree despite a patch")
	}
	scratch, err := Scratch(context.Background(), work)
	if err != nil || len(scratch) != 0 {
		t.Fatalf("scratch after materialize = %v, %v", scratch, err)
	}
	// The effective tree is readable from the checkout's own object database.
	if got := git(t, work, "cat-file", "-p", view.EffectiveTree+":charts/x/job.yaml"); got != "a\nb\nB2\nc\nd" {
		t.Fatalf("effective blob = %q", got)
	}
	report, err := Check(context.Background(), work)
	if err != nil || report.Patches != 1 || len(report.Uncaptured) != 0 || report.Effective != view.EffectiveTree {
		t.Fatalf("check = %+v, %v", report, err)
	}
}

func TestMaterializeWithoutPatchesOnlyRecordsTheView(t *testing.T) {
	dir := fixture(t, baseFiles)
	work := filepath.Join(t.TempDir(), "checkout")
	git(t, dir, "clone", "-q", dir, work)
	view, err := Materialize(context.Background(), work)
	if err != nil || view.BaseTree != view.EffectiveTree || git(t, work, "status", "--porcelain") != "" {
		t.Fatalf("view = %+v, %v", view, err)
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func TestCreateCapturesScratchStepwiseAndReproducesExactly(t *testing.T) {
	ctx := context.Background()
	accepted, work := patchedFixture(t)
	if _, err := Materialize(ctx, work); err != nil {
		t.Fatal(err)
	}
	put(t, work, "charts/x/job.yaml", "a\nb\nB2\nc\nd\ne\n")
	put(t, work, "added/new.txt", "new\n")
	if err := os.Remove(filepath.Join(work, "README")); err != nil {
		t.Fatal(err)
	}
	// Edits under .sdo are never scratch.
	put(t, work, ".sdo/playbooks/p.md", "note\n")
	want := []string{"README", "added/new.txt", "charts/x/job.yaml"}
	if got, err := Scratch(ctx, work); err != nil || !reflect.DeepEqual(sorted(got), want) {
		t.Fatalf("scratch = %v, %v; want %v", got, err, want)
	}
	if report, err := Check(ctx, work); err != nil || !reflect.DeepEqual(sorted(report.Uncaptured), want) {
		t.Fatalf("uncaptured = %+v, %v", report, err)
	}

	first, err := Create(ctx, work, CreateRequest{Owner: OwnerDeployer, Reason: longReason, Paths: []string{"charts"}})
	if err != nil {
		t.Fatal(err)
	}
	if first.Name != "0002-the-chart-renders-the-job-without.patch" || !reflect.DeepEqual(first.Paths, []string{"charts/x/job.yaml"}) {
		t.Fatalf("first = %+v", first)
	}
	if left, _ := Scratch(ctx, work); !reflect.DeepEqual(sorted(left), []string{"README", "added/new.txt"}) {
		t.Fatalf("scratch after first create = %v", left)
	}
	second, err := Create(ctx, work, CreateRequest{Owner: OwnerResponder, Incident: "inc-1", Reason: longReason, Name: "cleanup"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != "0003-cleanup.patch" {
		t.Fatalf("second = %+v", second)
	}
	raw := readFile(t, work, Directory+"/0003-cleanup.patch")
	if !strings.HasPrefix(raw, "SDO-Patch: 1\nOwner: responder\n") || !strings.Contains(raw, "Incident: inc-1\n") {
		t.Fatalf("header = %q", raw[:120])
	}
	report, err := Check(ctx, work)
	if err != nil || report.Patches != 3 || len(report.Uncaptured) != 0 {
		t.Fatalf("check after capture = %+v, %v", report, err)
	}

	// Publishing the new patches and re-materializing elsewhere gives the same files.
	git(t, work, "add", "-A", "--", Directory)
	git(t, work, "-c", "user.name=t", "-c", "user.email=t@e.test", "commit", "-q", "-m", "capture", "--", Directory)
	git(t, work, "push", "-q", "origin", "HEAD:refs/heads/captured")
	git(t, accepted, "checkout", "-q", "captured")
	again := filepath.Join(t.TempDir(), "again")
	git(t, accepted, "clone", "-q", "-b", "captured", accepted, again)
	if _, err := Materialize(ctx, again); err != nil {
		t.Fatal(err)
	}
	if left, err := Scratch(ctx, again); err != nil || len(left) != 0 {
		t.Fatalf("scratch after re-materialize = %v, %v", left, err)
	}
	if readFile(t, again, "charts/x/job.yaml") != "a\nb\nB2\nc\nd\ne\n" || readFile(t, again, "added/new.txt") != "new\n" || exists(again, "README") {
		t.Fatal("re-materialized files differ from the agent's scratch")
	}
}

func TestCreateRejectsWithoutWritingAnything(t *testing.T) {
	ctx := context.Background()
	_, work := patchedFixture(t)
	if _, err := Materialize(ctx, work); err != nil {
		t.Fatal(err)
	}
	before := readDirNames(t, work)
	try := func(label string, req CreateRequest) error {
		t.Helper()
		_, err := Create(ctx, work, req)
		if err == nil {
			t.Fatalf("%s: create succeeded", label)
		}
		if got := readDirNames(t, work); got != before {
			t.Fatalf("%s: patches directory changed: %s", label, got)
		}
		return err
	}
	try("no scratch", CreateRequest{Owner: OwnerDeployer, Reason: longReason})

	put(t, work, "charts/x/job.yaml", "a\nb\nB2\nc\nd\ne\n")
	var perr *PatchError
	if err := try("short reason", CreateRequest{Owner: OwnerDeployer, Reason: "fix"}); !errors.As(err, &perr) || perr.Kind != KindMalformed {
		t.Fatalf("short reason error = %v", err)
	}
	if err := try("responder without incident", CreateRequest{Owner: OwnerResponder, Reason: longReason}); !errors.As(err, &perr) {
		t.Fatalf("missing incident error = %v", err)
	}
	try("path without scratch", CreateRequest{Owner: OwnerDeployer, Reason: longReason, Paths: []string{"README"}})
	try("sdo path", CreateRequest{Owner: OwnerDeployer, Reason: longReason, Paths: []string{".sdo/deployment"}})
	try("escaping path", CreateRequest{Owner: OwnerDeployer, Reason: longReason, Paths: []string{"../x"}})
	try("bad name", CreateRequest{Owner: OwnerDeployer, Reason: longReason, Name: "Bad Name"})

	put(t, work, "blob.bin", "a\x00b")
	err := try("binary", CreateRequest{Owner: OwnerDeployer, Reason: longReason, Paths: []string{"blob.bin"}})
	if !strings.Contains(err.Error(), "binary") && !strings.Contains(err.Error(), "Binary") {
		t.Fatalf("binary error = %v", err)
	}
}

func readDirNames(t *testing.T, work string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(work, filepath.FromSlash(Directory)))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return strings.Join(names, ",")
}

func TestCheckReportsAHandWrittenPatchThatDoesNotApply(t *testing.T) {
	ctx := context.Background()
	_, work := patchedFixture(t)
	if _, err := Materialize(ctx, work); err != nil {
		t.Fatal(err)
	}
	bad := "SDO-Patch: 1\nOwner: deployer\nReason: |\n  " + longReason + "\n\n" +
		"diff --git a/README b/README\nindex " + strings.Repeat("1", 40) + ".." + strings.Repeat("2", 40) + " 100644\n--- a/README\n+++ b/README\n@@ -1 +1 @@\n-not the content\n+x\n"
	put(t, work, Directory+"/0002-bad.patch", bad)
	_, err := Check(ctx, work)
	var perr *PatchError
	if !errors.As(err, &perr) || perr.Kind != KindConflict || perr.Patch != "0002-bad.patch" {
		t.Fatalf("check = %v", err)
	}
	if _, err := Create(ctx, work, CreateRequest{Owner: OwnerDeployer, Reason: longReason}); !errors.As(err, &perr) {
		t.Fatalf("create over a broken series = %v", err)
	}
}

func TestOperationsRequireTheRecordedView(t *testing.T) {
	dir := fixture(t, baseFiles)
	if _, err := Check(context.Background(), dir); !errors.Is(err, ErrNoView) {
		t.Fatalf("check = %v", err)
	}
	if _, ok, err := LoadView(context.Background(), dir); ok || err != nil {
		t.Fatalf("LoadView = %v, %v", ok, err)
	}
}

func TestPublishedTreeCarriesTheWorkingDeploymentDirectory(t *testing.T) {
	ctx := context.Background()
	_, work := patchedFixture(t)
	view, err := Materialize(ctx, work)
	if err != nil {
		t.Fatal(err)
	}
	put(t, work, ".sdo/deployment/values/v.yaml", "k: v\n")
	put(t, work, "charts/x/job.yaml", "scratch\n")
	tree, err := PublishedTree(ctx, work)
	if err != nil {
		t.Fatal(err)
	}
	if got := git(t, work, "cat-file", "-p", tree+":.sdo/deployment/values/v.yaml"); got != "k: v" {
		t.Fatalf("values = %q", got)
	}
	// Scratch app edits are not part of the published tree: the base content stays.
	if got := git(t, work, "cat-file", "-p", tree+":charts/x/job.yaml"); got != "a\nb\nc\nd" {
		t.Fatalf("job.yaml in published tree = %q", got)
	}
	if tree == view.BaseTree {
		t.Fatal("published tree ignored the working deployment directory")
	}
}

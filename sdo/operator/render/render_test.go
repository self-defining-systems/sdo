package render_test

import (
	"context"
	"errors"
	"io/fs"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
)

func blob(s string) string { return render.GitBlobID([]byte(s)) }

func TestRecorderRecordsReadsListingsAbsentAndStats(t *testing.T) {
	base := rendertest.MapFS{"a/x.json": "1", "a/y.json": "22", "b.txt": "333", "c.txt": "4"}
	rec := render.Record(base, render.Limits{})
	if _, err := rec.ReadDir("a"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a/y.json", "a/x.json", "a/x.json"} {
		if _, err := rec.ReadFile(name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rec.Stat("b.txt"); err != nil {
		t.Fatal(err)
	}
	// Declared first, read later: one Read input.
	if _, err := rec.Stat("c.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := rec.ReadFile("c.txt"); err != nil {
		t.Fatal(err)
	}
	// Probes for absent paths are inputs too.
	if _, err := rec.Stat("kustomization.yaml"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := rec.ReadFile("values.json"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := rec.ReadDir("nodir"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	want := []render.Input{
		{Path: "a", Kind: render.InputListed},
		{Path: "a/x.json", Blob: blob("1"), Kind: render.InputRead},
		{Path: "a/y.json", Blob: blob("22"), Kind: render.InputRead},
		{Path: "b.txt", Blob: blob("333"), Kind: render.InputStat},
		{Path: "c.txt", Blob: blob("4"), Kind: render.InputRead},
		{Path: "kustomization.yaml", Kind: render.InputAbsent},
		{Path: "nodir", Kind: render.InputAbsent},
		{Path: "values.json", Kind: render.InputAbsent},
	}
	if got := rec.Inputs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Inputs = %v\nwant %v", got, want)
	}
}

func TestStatDoesNotChargeLimits(t *testing.T) {
	rec := render.Record(rendertest.MapFS{"big": "123456789"}, render.Limits{FileBytes: 4})
	if _, err := rec.Stat("big"); err != nil {
		t.Fatalf("declaring an input must not read it: %v", err)
	}
}

func TestGitBlobIDMatchesGit(t *testing.T) {
	// `printf 'hello\n' | git hash-object --stdin`
	if got := render.GitBlobID([]byte("hello\n")); got != "ce013625030ba8dba906f756967f9e9ca394464a" {
		t.Fatalf("GitBlobID = %s", got)
	}
}

func TestRecorderOpenIsRecordedAndLimited(t *testing.T) {
	rec := render.Record(rendertest.MapFS{"f": "12345"}, render.Limits{FileBytes: 4})
	if _, err := fs.ReadFile(rec, "f"); !errors.Is(err, render.ErrLimit) {
		t.Fatalf("Open path escaped the file limit: %v", err)
	}
	rec = render.Record(rendertest.MapFS{"f": "12345"}, render.Limits{})
	if _, err := fs.ReadFile(rec, "f"); err != nil {
		t.Fatal(err)
	}
	if got := rec.Inputs(); len(got) != 1 || got[0].Path != "f" || got[0].Kind != render.InputRead {
		t.Fatalf("Inputs = %v", got)
	}
}

func TestRecorderTotalLimitCountsDistinctFilesOnce(t *testing.T) {
	rec := render.Record(rendertest.MapFS{"a": "1234", "b": "1234"}, render.Limits{FileBytes: 4, TotalBytes: 6})
	for i := 0; i < 3; i++ {
		if _, err := rec.ReadFile("a"); err != nil {
			t.Fatalf("re-reading one file must not accumulate: %v", err)
		}
	}
	if _, err := rec.ReadFile("b"); !errors.Is(err, render.ErrLimit) {
		t.Fatalf("second file crossed the total limit without ErrLimit: %v", err)
	}
}

func TestRecorderRejectsEscapingPaths(t *testing.T) {
	rec := render.Record(rendertest.MapFS{"a": "1"}, render.Limits{})
	for _, name := range []string{"../a", "/a", "a/../a"} {
		if _, err := rec.ReadFile(name); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("ReadFile(%q) = %v, want ErrInvalid", name, err)
		}
	}
}

func TestOverlayShadowsAndIsNotRecorded(t *testing.T) {
	rec := render.Record(rendertest.MapFS{"real": "r", "both": "base"}, render.Limits{})
	ov := render.Overlay(rec, map[string][]byte{"virtual/v": []byte("v"), "both": []byte("over")})
	for name, want := range map[string]string{"real": "r", "virtual/v": "v", "both": "over"} {
		got, err := ov.ReadFile(name)
		if err != nil || string(got) != want {
			t.Fatalf("ReadFile(%q) = %q, %v; want %q", name, got, err, want)
		}
	}
	if got := rec.Inputs(); len(got) != 1 || got[0].Path != "real" {
		t.Fatalf("virtual files leaked into Inputs: %v", got)
	}
	entries, err := ov.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if want := []string{"both", "real", "virtual"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("ReadDir(.) = %v, want %v", names, want)
	}
	if info, err := ov.Stat("virtual"); err != nil || !info.IsDir() {
		t.Fatalf("Stat(virtual) = %v, %v", info, err)
	}
	if got, err := ov.Blob("virtual/v"); err != nil || got != blob("v") {
		t.Fatalf("Blob(virtual/v) = %q, %v", got, err)
	}
}

func TestOverlayDoesNotAliasCallerBytes(t *testing.T) {
	data := []byte("abc")
	ov := render.Overlay(rendertest.MapFS{}, map[string][]byte{"v": data})
	data[0] = 'X'
	got, _ := ov.ReadFile("v")
	got[1] = 'Y'
	again, _ := ov.ReadFile("v")
	if string(again) != "abc" {
		t.Fatalf("overlay content mutated through aliasing: %q", again)
	}
}

func TestRegisterValidates(t *testing.T) {
	toy := render.RendererFunc[*wrapperspb.StringValue](func(context.Context, render.FS, render.Env, *wrapperspb.StringValue) (render.Result, error) {
		return render.Result{}, nil
	})
	c := render.NewCatalog()
	for _, id := range []string{"", "Raw", "1raw", "raw_x", "raw v1"} {
		if err := render.Register[*wrapperspb.StringValue](c, id, toy); err == nil {
			t.Errorf("id %q accepted", id)
		}
	}
	if err := render.Register[*wrapperspb.StringValue](c, "a", toy); err != nil {
		t.Fatal(err)
	}
	if err := render.Register[*wrapperspb.StringValue](c, "a", toy); err == nil {
		t.Error("duplicate id accepted")
	}
	err := render.Register[*wrapperspb.StringValue](c, "b", toy)
	if err == nil || !strings.Contains(err.Error(), `"a"`) {
		t.Errorf("duplicate config type accepted or unhelpful: %v", err)
	}
	if err := render.Register[*wrapperspb.StringValue](c, "c", nil); err == nil {
		t.Error("nil renderer accepted")
	}
	if got := c.IDs(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Errorf("IDs = %v", got)
	}
}

func TestDecodeRejectsUnknownIDAndWrongConfigType(t *testing.T) {
	c := render.NewCatalog()
	if err := rendertest.RegisterToy(c); err != nil {
		t.Fatal(err)
	}
	good, _ := anypb.New(wrapperspb.String("app"))
	if _, err := c.Decode("nope", good); !errors.Is(err, render.ErrUnknownRenderer) {
		t.Errorf("unknown id: %v", err)
	}
	if _, err := c.Decode(rendertest.ToyID, nil); err == nil {
		t.Error("nil config accepted")
	}
	wrong, _ := anypb.New(structpb.NewStringValue("x"))
	if _, err := c.Decode(rendertest.ToyID, wrong); err == nil || !strings.Contains(err.Error(), "StringValue") {
		t.Errorf("wrong config type accepted or unhelpful: %v", err)
	}
	msg, err := c.Decode(rendertest.ToyID, good)
	if err != nil || msg.(*wrapperspb.StringValue).GetValue() != "app" {
		t.Errorf("Decode = %v, %v", msg, err)
	}
}

func TestCatalogRenderSetsInputsAndRejectsInvalidResults(t *testing.T) {
	obj := func(name string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name}}}
	}
	cases := map[string]render.Result{
		"missing name":   render.SinglePhase(obj("")),
		"nil object":     {Phases: []render.Phase{{Name: "p", Objects: []*unstructured.Unstructured{nil}}}},
		"unnamed phase":  {Phases: []render.Phase{{Objects: []*unstructured.Unstructured{obj("a")}}}},
		"repeated phase": {Phases: []render.Phase{{Name: "p"}, {Name: "p"}}},
		"set inputs":     {Phases: render.SinglePhase(obj("a")).Phases, Inputs: []render.Input{{Path: "lie", Kind: render.InputRead}}},
	}
	cfg, _ := anypb.New(wrapperspb.String(""))
	for name, res := range cases {
		c := render.NewCatalog()
		_ = render.Register[*wrapperspb.StringValue](c, "r", render.RendererFunc[*wrapperspb.StringValue](func(context.Context, render.FS, render.Env, *wrapperspb.StringValue) (render.Result, error) {
			return res, nil
		}))
		if _, err := c.Render(context.Background(), rendertest.MapFS{}, render.Env{}, "r", cfg); err == nil {
			t.Errorf("%s: invalid result accepted", name)
		}
	}

	c := render.NewCatalog()
	_ = render.Register[*wrapperspb.StringValue](c, "r", render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, f render.FS, _ render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		if _, err := f.ReadFile("in"); err != nil {
			return render.Result{}, err
		}
		_, _ = f.Stat("optional")
		return render.SinglePhase(obj("a")), nil
	}))
	res, err := c.Render(context.Background(), rendertest.MapFS{"in": "x"}, render.Env{}, "r", cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []render.Input{{Path: "in", Blob: blob("x"), Kind: render.InputRead}, {Path: "optional", Kind: render.InputAbsent}}
	if !reflect.DeepEqual(res.Inputs, want) {
		t.Fatalf("Inputs = %v, want %v: the catalog, not the renderer, must set them", res.Inputs, want)
	}
}

func TestResultDesiredExcludesEphemeralPhases(t *testing.T) {
	mk := func(n string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": n}}}
	}
	res := render.Result{Phases: []render.Phase{
		{Name: "p1", Objects: []*unstructured.Unstructured{mk("a")}},
		{Name: "h", Objects: []*unstructured.Unstructured{mk("hook")}, Ephemeral: true},
		{Name: "p2", Objects: []*unstructured.Unstructured{mk("b")}},
	}}
	var names []string
	for _, o := range res.Desired() {
		names = append(names, o.GetName())
	}
	if !reflect.DeepEqual(names, []string{"a", "b"}) {
		t.Fatalf("Desired = %v", names)
	}
	if got := len(res.Objects()); got != 3 {
		t.Fatalf("Objects = %d, want 3", got)
	}
	if got := render.SinglePhase(mk("x")).Desired(); len(got) != 1 {
		t.Fatalf("SinglePhase must be desired, got %d", len(got))
	}
}

func TestSecretOmitRejectedByCatalog(t *testing.T) {
	c := render.NewCatalog()
	_ = render.Register[*wrapperspb.StringValue](c, "r", render.RendererFunc[*wrapperspb.StringValue](func(context.Context, render.FS, render.Env, *wrapperspb.StringValue) (render.Result, error) {
		s := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "s"}, "data": map[string]any{"k": "dg=="}}}
		return render.SinglePhase(s), nil
	}))
	cfg, _ := anypb.New(wrapperspb.String(""))
	if _, err := c.Render(context.Background(), rendertest.MapFS{}, render.Env{Secrets: render.SecretsOmit}, "r", cfg); err == nil {
		t.Fatal("Secret data under SecretsOmit accepted")
	}
	if _, err := c.Render(context.Background(), rendertest.MapFS{}, render.Env{Secrets: render.SecretsInclude}, "r", cfg); err != nil {
		t.Fatalf("Secret data under SecretsInclude rejected: %v", err)
	}
}

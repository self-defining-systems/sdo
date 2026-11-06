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

func TestRecorderRecordsReadsAndListings(t *testing.T) {
	base := rendertest.MapFS{"a/x.json": "1", "a/y.json": "22", "b.txt": "333"}
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
	want := []string{"a/", "a/x.json", "a/y.json"}
	if got := rec.Inputs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Inputs = %v, want %v (Stat must not record)", got, want)
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
	if got := rec.Inputs(); !reflect.DeepEqual(got, []string{"f"}) {
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
	if got := rec.Inputs(); !reflect.DeepEqual(got, []string{"real"}) {
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
	if ov.Tree() != rec.Tree() {
		t.Fatal("overlay changed Tree")
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
		"missing name": {Objects: []*unstructured.Unstructured{obj("")}},
		"nil object":   {Objects: []*unstructured.Unstructured{nil}},
		"short phases": {Objects: []*unstructured.Unstructured{obj("a"), obj("b")}, Phases: []render.Phase{{End: 1}}},
		"long phases":  {Objects: []*unstructured.Unstructured{obj("a")}, Phases: []render.Phase{{End: 2}}},
		"decreasing":   {Objects: []*unstructured.Unstructured{obj("a"), obj("b")}, Phases: []render.Phase{{End: 2}, {End: 1}}},
		"set inputs":   {Objects: []*unstructured.Unstructured{obj("a")}, Inputs: []string{"lie"}},
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
		return render.Result{Objects: []*unstructured.Unstructured{obj("a")}}, nil
	}))
	res, err := c.Render(context.Background(), rendertest.MapFS{"in": "x"}, render.Env{}, "r", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Inputs, []string{"in"}) {
		t.Fatalf("Inputs = %v: the catalog, not the renderer, must set them", res.Inputs)
	}
}

func TestResultDesiredExcludesEphemeralPhases(t *testing.T) {
	mk := func(n string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": n}}}
	}
	res := render.Result{
		Objects: []*unstructured.Unstructured{mk("a"), mk("hook"), mk("b")},
		Phases:  []render.Phase{{Name: "p1", End: 1}, {Name: "h", End: 2, Ephemeral: true}, {Name: "p2", End: 3}},
	}
	var names []string
	for _, o := range res.Desired() {
		names = append(names, o.GetName())
	}
	if !reflect.DeepEqual(names, []string{"a", "b"}) {
		t.Fatalf("Desired = %v", names)
	}
	if got := (render.Result{Objects: res.Objects}).Desired(); len(got) != 3 {
		t.Fatalf("nil Phases must be one desired phase, got %d", len(got))
	}
}

func TestSecretOmitRejectedByCatalog(t *testing.T) {
	c := render.NewCatalog()
	_ = render.Register[*wrapperspb.StringValue](c, "r", render.RendererFunc[*wrapperspb.StringValue](func(context.Context, render.FS, render.Env, *wrapperspb.StringValue) (render.Result, error) {
		s := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "s"}, "data": map[string]any{"k": "dg=="}}}
		return render.Result{Objects: []*unstructured.Unstructured{s}}, nil
	}))
	cfg, _ := anypb.New(wrapperspb.String(""))
	if _, err := c.Render(context.Background(), rendertest.MapFS{}, render.Env{Secrets: render.SecretsOmit}, "r", cfg); err == nil {
		t.Fatal("Secret data under SecretsOmit accepted")
	}
	if _, err := c.Render(context.Background(), rendertest.MapFS{}, render.Env{Secrets: render.SecretsInclude}, "r", cfg); err != nil {
		t.Fatalf("Secret data under SecretsInclude rejected: %v", err)
	}
}

package imagesrc_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
)

type typedCause struct{ msg string }

func (e *typedCause) Error() string { return e.msg }

// stringSource takes a StringValue config so catalogs can hold two config types.
type stringSource struct{ got []string }

func (s *stringSource) Resolve(_ context.Context, req imagesrc.Request, cfg *wrapperspb.StringValue) (imagesrc.Resolved, error) {
	s.got = append(s.got, cfg.GetValue())
	return imagesrc.Resolved{Ref: imagesrctest.Repository(req.Image) + "@" + imagesrctest.Digest(req.Image), Inputs: imagesrc.Inputs{SourceImage: req.Image}}, nil
}

type funcSource func(ctx context.Context, req imagesrc.Request) (imagesrc.Resolved, error)

func (f funcSource) Resolve(ctx context.Context, req imagesrc.Request, _ *emptypb.Empty) (imagesrc.Resolved, error) {
	return f(ctx, req)
}

func catalogWith(t *testing.T, id string, s imagesrc.Source[*emptypb.Empty], o imagesrc.Options) *imagesrc.Catalog {
	t.Helper()
	c := imagesrc.NewCatalog()
	if err := imagesrc.Register(c, id, s, o); err != nil {
		t.Fatal(err)
	}
	if err := c.SetDefault(id); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestFakeSourceConforms(t *testing.T) {
	c := catalogWith(t, "fake", &imagesrctest.Fake{}, imagesrc.Options{})
	want := "registry.example/app@" + imagesrctest.Digest("registry.example/app:v1")
	imagesrctest.TestSource(t, c, "fake", []imagesrctest.Case{
		{Name: "tag", Image: "registry.example/app:v1", WantRef: want, WantInputs: &imagesrc.Inputs{SourceImage: "registry.example/app:v1"}},
		{Name: "with-files", Image: "registry.example/app:v1", Files: map[string]string{"Dockerfile": "FROM scratch\n"}, WantRef: want},
		{Name: "explicit-config", Image: "registry.example/app:v1", Config: &emptypb.Empty{}},
	})
}

func TestConformanceCatchesNonconformingSources(t *testing.T) {
	bad := funcSource(func(_ context.Context, req imagesrc.Request) (imagesrc.Resolved, error) {
		return imagesrc.Resolved{Ref: req.Image}, nil // tag, not a digest
	})
	c := catalogWith(t, "bad", bad, imagesrc.Options{})
	imagesrctest.TestSource(t, c, "bad", []imagesrctest.Case{{Name: "tag-rejected", Image: "r.example/app:v1", WantErr: "mutable"}})
}

func TestResolveAllDedupesAndKeepsOrder(t *testing.T) {
	fake := &imagesrctest.Fake{}
	c := catalogWith(t, "fake", fake, imagesrc.Options{})
	got, err := c.ResolveAll(context.Background(), []string{"r/a:1", "r/b:1", "r/a:1", "r/a:2"}, nil, imagesrc.Request{Tree: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"r/a:1", "r/b:1", "r/a:2"}; !reflect.DeepEqual(fake.Calls(), want) {
		t.Fatalf("calls = %v, want %v", fake.Calls(), want)
	}
	if len(got) != 3 || got["r/a:1"].Ref != "r/a@"+imagesrctest.Digest("r/a:1") {
		t.Fatalf("result = %v", got)
	}
}

func TestResolveAllPassesRequestAndDeclaredConfig(t *testing.T) {
	var seen []imagesrc.Request
	rec := funcSource(func(_ context.Context, req imagesrc.Request) (imagesrc.Resolved, error) {
		seen = append(seen, req)
		return imagesrc.Resolved{Ref: "r/a@" + imagesrctest.Digest("x"), Inputs: imagesrc.Inputs{SourceImage: req.Image}}, nil
	})
	strs := &stringSource{}
	c := imagesrc.NewCatalog()
	must(t, imagesrc.Register(c, "rec", imagesrc.Source[*emptypb.Empty](rec), imagesrc.Options{}))
	must(t, imagesrc.Register(c, "str", imagesrc.Source[*wrapperspb.StringValue](strs), imagesrc.Options{}))
	must(t, c.SetDefault("rec"))
	cfg, err := anypb.New(wrapperspb.String("hello"))
	must(t, err)
	base := imagesrc.Request{Image: "ignored", Tree: "t1", Checkout: "/co"}
	_, err = c.ResolveAll(context.Background(), []string{"r/a:1", "r/b:1"}, []imagesrc.Declared{{Image: "r/b:1", SourceID: "str", Config: cfg}}, base)
	must(t, err)
	if len(seen) != 1 || seen[0].Image != "r/a:1" || seen[0].Tree != "t1" || seen[0].Checkout != "/co" {
		t.Fatalf("default source saw %+v", seen)
	}
	if !reflect.DeepEqual(strs.got, []string{"hello"}) {
		t.Fatalf("declared config = %v", strs.got)
	}
	// A declared source with no config gets its empty config.
	strs.got = nil
	_, err = c.ResolveAll(context.Background(), []string{"r/b:1"}, []imagesrc.Declared{{Image: "r/b:1", SourceID: "str"}}, base)
	must(t, err)
	if !reflect.DeepEqual(strs.got, []string{""}) {
		t.Fatalf("empty config = %v", strs.got)
	}
}

func TestResolveAllRejectsBadDeclarations(t *testing.T) {
	strs := &stringSource{}
	c := imagesrc.NewCatalog()
	must(t, imagesrc.Register(c, "fake", imagesrc.Source[*emptypb.Empty](&imagesrctest.Fake{}), imagesrc.Options{}))
	must(t, imagesrc.Register(c, "str", imagesrc.Source[*wrapperspb.StringValue](strs), imagesrc.Options{}))
	wrongType, err := anypb.New(&emptypb.Empty{})
	must(t, err)
	for _, tc := range []struct {
		name     string
		images   []string
		declared []imagesrc.Declared
		want     string
	}{
		{"no default", []string{"r/a:1"}, nil, "no default"},
		{"unknown source", []string{"r/a:1"}, []imagesrc.Declared{{Image: "r/a:1", SourceID: "nope"}}, "unknown source"},
		{"unused declaration", []string{"r/a:1"}, []imagesrc.Declared{{Image: "r/b:1", SourceID: "fake"}}, "no object uses"},
		{"duplicate declaration", []string{"r/a:1"}, []imagesrc.Declared{{Image: "r/a:1", SourceID: "fake"}, {Image: "r/a:1", SourceID: "str"}}, "twice"},
		{"wrong config type", []string{"r/a:1"}, []imagesrc.Declared{{Image: "r/a:1", SourceID: "str", Config: wrongType}}, "takes config"},
		{"empty image", []string{""}, nil, "empty image"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.ResolveAll(context.Background(), tc.images, tc.declared, imagesrc.Request{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
	if len(strs.got) != 0 {
		t.Fatalf("a rejected plan still resolved: %v", strs.got)
	}
}

func TestResolveAllVerifiesRefs(t *testing.T) {
	good := "r.example/app@" + imagesrctest.Digest("x")
	for _, tc := range []struct {
		name  string
		ref   string
		same  bool
		image string
		want  string
	}{
		{"digest in repo", good, true, "r.example/app:v1", ""},
		{"other repo allowed", "other.example/app@" + imagesrctest.Digest("x"), false, "r.example/app:v1", ""},
		{"other repo rejected", "other.example/app@" + imagesrctest.Digest("x"), true, "r.example/app:v1", "outside the requested repository"},
		{"registry port is not a tag", "h:5000/app@" + imagesrctest.Digest("x"), true, "h:5000/app", ""},
		{"tag", "r.example/app:v1", false, "r.example/app:v1", "mutable"},
		{"short digest", "r.example/app@sha256:abc", false, "r.example/app:v1", "mutable"},
		{"empty", "", false, "r.example/app:v1", "mutable"},
		{"whitespace", "r.example/app @sha256:" + strings.Repeat("a", 64), false, "r.example/app:v1", "mutable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := funcSource(func(context.Context, imagesrc.Request) (imagesrc.Resolved, error) {
				return imagesrc.Resolved{Ref: tc.ref, Inputs: imagesrc.Inputs{SourceImage: tc.image}}, nil
			})
			c := catalogWith(t, "s", src, imagesrc.Options{SameRepository: tc.same})
			got, err := c.ResolveAll(context.Background(), []string{tc.image}, nil, imagesrc.Request{})
			if tc.want == "" {
				if err != nil || got[tc.image].Ref != tc.ref {
					t.Fatalf("got %v, %v", got, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) || got != nil {
				t.Fatalf("got %v, %v; want error %q", got, err, tc.want)
			}
		})
	}
}

func TestErrorKeepsTypedCause(t *testing.T) {
	cause := &typedCause{"registry refused"}
	src := funcSource(func(context.Context, imagesrc.Request) (imagesrc.Resolved, error) { return imagesrc.Resolved{}, cause })
	c := catalogWith(t, "s", src, imagesrc.Options{})
	_, err := c.ResolveAll(context.Background(), []string{"r/a:1"}, nil, imagesrc.Request{})
	var typed *typedCause
	var attributed *imagesrc.Error
	if !errors.As(err, &typed) || typed != cause || !errors.As(err, &attributed) || attributed.Image != "r/a:1" || attributed.SourceID != "s" {
		t.Fatalf("err = %#v", err)
	}
}

func TestBudgetIsSharedPerSourceAndClassified(t *testing.T) {
	slow := funcSource(func(ctx context.Context, req imagesrc.Request) (imagesrc.Resolved, error) {
		<-ctx.Done()
		return imagesrc.Resolved{}, ctx.Err()
	})
	c := catalogWith(t, "slow", slow, imagesrc.Options{Budget: 20 * time.Millisecond})
	_, err := c.ResolveAll(context.Background(), []string{"r/a:1", "r/b:1"}, nil, imagesrc.Request{})
	var budget *imagesrc.BudgetError
	if !errors.As(err, &budget) || budget.Image != "r/a:1" || budget.SourceID != "slow" || budget.Limit != 20*time.Millisecond {
		t.Fatalf("err = %#v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("budget error must unwrap to DeadlineExceeded")
	}
}

func TestBudgetCarriesAcrossImagesOfOneSource(t *testing.T) {
	var deadlines []time.Time
	src := funcSource(func(ctx context.Context, req imagesrc.Request) (imagesrc.Resolved, error) {
		d, ok := ctx.Deadline()
		if !ok {
			t.Error("no deadline")
		}
		deadlines = append(deadlines, d)
		time.Sleep(5 * time.Millisecond)
		return imagesrc.Resolved{Ref: "r/a@" + imagesrctest.Digest("x"), Inputs: imagesrc.Inputs{SourceImage: req.Image}}, nil
	})
	c := catalogWith(t, "s", src, imagesrc.Options{Budget: time.Minute})
	_, err := c.ResolveAll(context.Background(), []string{"r/a:1", "r/b:1"}, nil, imagesrc.Request{})
	must(t, err)
	if len(deadlines) != 2 || !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("deadlines differ across one source's images: %v", deadlines)
	}
}

func TestCallerCancellationIsNotABudgetError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	src := funcSource(func(ctx context.Context, _ imagesrc.Request) (imagesrc.Resolved, error) {
		cancel()
		<-ctx.Done()
		return imagesrc.Resolved{}, ctx.Err()
	})
	c := catalogWith(t, "s", src, imagesrc.Options{Budget: time.Minute})
	_, err := c.ResolveAll(ctx, []string{"r/a:1"}, nil, imagesrc.Request{})
	var budget *imagesrc.BudgetError
	if errors.As(err, &budget) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %#v", err)
	}
}

func TestRegisterValidates(t *testing.T) {
	fake := imagesrc.Source[*emptypb.Empty](&imagesrctest.Fake{})
	other := imagesrc.Source[*wrapperspb.StringValue](&stringSource{})
	c := imagesrc.NewCatalog()
	must(t, imagesrc.Register(c, "ok", fake, imagesrc.Options{}))
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"bad id", imagesrc.Register(c, "Bad_ID", fake, imagesrc.Options{}), "must match"},
		{"empty id", imagesrc.Register(c, "", fake, imagesrc.Options{}), "must match"},
		{"duplicate id", imagesrc.Register(c, "ok", other, imagesrc.Options{}), "already registered"},
		{"duplicate config type", imagesrc.Register(c, "twin", fake, imagesrc.Options{}), "already owned"},
		{"nil source", imagesrc.Register[*emptypb.Empty](c, "nil", nil, imagesrc.Options{}), "is nil"},
		{"negative budget", imagesrc.Register(c, "neg", other, imagesrc.Options{Budget: -1}), "negative"},
		{"interface config", imagesrc.Register[proto.Message](c, "iface", ifaceSource{}, imagesrc.Options{}), "concrete"},
		{"nil catalog", imagesrc.Register[*emptypb.Empty](nil, "x", fake, imagesrc.Options{}), "NewCatalog"},
		{"zero catalog", imagesrc.Register(&imagesrc.Catalog{}, "x", fake, imagesrc.Options{}), "NewCatalog"},
		{"unknown default", c.SetDefault("missing"), "not registered"},
	} {
		if tc.err == nil || !strings.Contains(tc.err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, tc.err, tc.want)
		}
	}
	if !reflect.DeepEqual(c.IDs(), []string{"ok"}) {
		t.Fatalf("ids = %v", c.IDs())
	}
}

type ifaceSource struct{}

func (ifaceSource) Resolve(context.Context, imagesrc.Request, proto.Message) (imagesrc.Resolved, error) {
	return imagesrc.Resolved{}, nil
}

func TestDecode(t *testing.T) {
	c := imagesrc.NewCatalog()
	must(t, imagesrc.Register(c, "str", imagesrc.Source[*wrapperspb.StringValue](&stringSource{}), imagesrc.Options{}))
	packed, err := anypb.New(wrapperspb.String("v"))
	must(t, err)
	m, err := c.Decode("str", packed)
	must(t, err)
	if m.(*wrapperspb.StringValue).GetValue() != "v" {
		t.Fatalf("decoded %v", m)
	}
	if m, err = c.Decode("str", nil); err != nil || m.(*wrapperspb.StringValue).GetValue() != "" {
		t.Fatalf("nil config: %v, %v", m, err)
	}
	wrong, _ := anypb.New(&emptypb.Empty{})
	if _, err := c.Decode("str", wrong); err == nil {
		t.Fatal("wrong type accepted")
	}
	corrupt := &anypb.Any{TypeUrl: packed.TypeUrl, Value: []byte{0xff, 0xff}}
	if _, err := c.Decode("str", corrupt); err == nil {
		t.Fatal("corrupt payload accepted")
	}
	if _, err := c.Decode("nope", nil); err == nil {
		t.Fatal("unknown id accepted")
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

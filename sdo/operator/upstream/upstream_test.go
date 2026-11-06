package upstream_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/upstream"
)

type fake struct{ rev, commit string }

func (f fake) Latest(context.Context, *emptypb.Empty) (upstream.Revision, error) {
	return upstream.Revision{ID: f.rev}, nil
}

func (f fake) Materialize(context.Context, *emptypb.Empty, upstream.Revision, string) (string, error) {
	return f.commit, nil
}

func mustAny(t *testing.T, m *emptypb.Empty) *anypb.Any {
	t.Helper()
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestRegisterValidation(t *testing.T) {
	c := upstream.NewCatalog()
	if err := upstream.Register[*emptypb.Empty](c, "Bad_ID", fake{}); err == nil {
		t.Error("invalid id accepted")
	}
	if err := upstream.Register[*emptypb.Empty](c, "a", nil); err == nil {
		t.Error("nil watcher accepted")
	}
	if err := upstream.Register[*emptypb.Empty](c, "a", fake{}); err != nil {
		t.Fatal(err)
	}
	if err := upstream.Register[*emptypb.Empty](c, "a", fake{}); err == nil {
		t.Error("duplicate id accepted")
	}
	if err := upstream.Register[*emptypb.Empty](c, "b", fake{}); err == nil || !strings.Contains(err.Error(), "already belongs") {
		t.Errorf("duplicate config type = %v", err)
	}
}

func TestCatalogRoutesByTypeAndRejectsEmptyAnswers(t *testing.T) {
	c := upstream.NewCatalog()
	if err := upstream.Register[*emptypb.Empty](c, "a", fake{}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	cfg := mustAny(t, &emptypb.Empty{})
	if _, err := c.Latest(ctx, "a", cfg); err == nil {
		t.Error("empty revision ID accepted")
	}
	if _, err := c.Materialize(ctx, "a", cfg, upstream.Revision{ID: "x"}, "d"); err == nil {
		t.Error("empty commit ID accepted")
	}
	wrong, _ := anypb.New(wrapperspb.String("x"))
	if _, err := c.Latest(ctx, "a", wrong); err == nil {
		t.Error("foreign config type accepted")
	}
	if _, err := c.Latest(ctx, "z", cfg); !errors.Is(err, upstream.ErrUnknownWatcher) {
		t.Errorf("unknown id = %v", err)
	}
	ok := upstream.NewCatalog()
	_ = upstream.Register[*emptypb.Empty](ok, "a", fake{rev: "r1", commit: "c1"})
	if rev, err := ok.Latest(ctx, "a", cfg); err != nil || rev.ID != "r1" {
		t.Errorf("Latest = %+v, %v", rev, err)
	}
	if commit, err := ok.Materialize(ctx, "a", cfg, upstream.Revision{ID: "r1"}, "d"); err != nil || commit != "c1" {
		t.Errorf("Materialize = %q, %v", commit, err)
	}
}

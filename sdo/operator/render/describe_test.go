package render_test

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/render"
)

func TestDescribeListsRegisteredRenderersSortedWithDoc(t *testing.T) {
	c := render.NewCatalog()
	if got := c.Describe(); len(got) != 0 {
		t.Fatalf("empty catalog describes %v", got)
	}
	if got := (*render.Catalog)(nil).Describe(); got != nil {
		t.Fatalf("nil catalog describes %v", got)
	}
	noop := render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, _ render.FS, _ render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		return render.Result{}, nil
	})
	if err := render.Register[*wrapperspb.StringValue](c, "zeta", noop, render.WithDoc("  Last one.  ")); err != nil {
		t.Fatal(err)
	}
	if err := render.Register[*wrapperspb.BoolValue](c, "alpha", render.RendererFunc[*wrapperspb.BoolValue](func(_ context.Context, _ render.FS, _ render.Env, _ *wrapperspb.BoolValue) (render.Result, error) {
		return render.Result{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	got := c.Describe()
	if ids := []string{got[0].ID, got[1].ID}; !reflect.DeepEqual(ids, []string{"alpha", "zeta"}) {
		t.Fatalf("order = %v", ids)
	}
	if got[1].Doc != "Last one." || got[0].Doc != "" {
		t.Fatalf("docs = %q, %q", got[0].Doc, got[1].Doc)
	}
	if got[1].ConfigType != "google.protobuf.StringValue" || got[1].ConfigSchemaJSON == "" {
		t.Fatalf("descriptor = %+v", got[1])
	}
}

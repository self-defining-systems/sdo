package rendered

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"testing"

	"sdo.dev/operator/render"
)

func TestSourceErrorKinds(t *testing.T) {
	commit := "0123456789abcdef0123456789abcdef01234567"
	tree := "89abcdef0123456789abcdef0123456789abcdef"
	ctx := context.Background()
	failing := func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("git down") }
	_, err := Source{Repository: "r", Deployment: rawSource(t, "k8s"), Catalog: testCatalog(t), Git: failing}.AcceptedManifests(ctx, commit, "app")
	if !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("git failure = %v, want unavailable", err)
	}
	listing := func(_ context.Context, _ string, args ...string) ([]byte, error) {
		switch args[0] {
		case "rev-parse":
			return []byte(tree + "\n"), nil
		case "cat-file":
			return []byte("tree\n"), nil
		}
		return []byte("100644 blob " + commit + " 4\tother.yaml\x00"), nil
	}
	_, err = Source{Repository: "r", Deployment: rawSource(t, "k8s"), Catalog: testCatalog(t), Git: listing}.AcceptedManifests(ctx, commit, "app")
	if !errors.Is(err, ErrSourceInvalid) {
		t.Fatalf("no manifests = %v, want invalid", err)
	}
	_, err = Source{Repository: "r", Catalog: testCatalog(t), Git: listing}.AcceptedManifests(ctx, commit, "app")
	if !errors.Is(err, ErrSourceUnconfigured) {
		t.Fatalf("nil deployment source = %v, want unconfigured", err)
	}
	_, err = Source{Repository: "r", Deployment: rawSource(t, "k8s"), Git: listing}.AcceptedManifests(ctx, commit, "app")
	if !errors.Is(err, ErrSourceInvalid) {
		t.Fatalf("no catalog = %v, want invalid", err)
	}
}

func TestClassifySourceByCause(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want error
	}{
		"read failure":  {&fs.PathError{Op: "read", Path: "a", Err: errors.New("i/o error")}, ErrSourceUnavailable},
		"wrapped read":  {fmt.Errorf("render: %w", &fs.PathError{Op: "read", Path: "a", Err: errors.New("i/o error")}), ErrSourceUnavailable},
		"not exist":     {&fs.PathError{Op: "read", Path: "a", Err: fs.ErrNotExist}, ErrSourceInvalid},
		"cancelled":     {context.Canceled, ErrSourceUnavailable},
		"output limit":  {fmt.Errorf("render helm: %w", &render.OutputLimitError{Bytes: 2, Limit: 1, Objects: 1}), ErrSourceInvalid},
		"uncategorized": {errors.New("bad yaml"), ErrSourceInvalid},
	} {
		if got := classifySource(tc.err); !errors.Is(got, tc.want) {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}

package gitpoll

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/upstream"
	"sdo.dev/operator/upstream/upstreamtest"
)

func catalog(t *testing.T) *upstream.Catalog {
	t.Helper()
	c := upstream.NewCatalog()
	if err := Register(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func config(url, ref string) *anypb.Any {
	a, err := anypb.New(&v1alpha1.GitPollConfig{Repository: url, Ref: ref, PollInterval: durationpb.New(0)})
	if err != nil {
		panic(err)
	}
	return a
}

func TestConformance(t *testing.T) {
	upstreamtest.TestWatcher(t, catalog(t), ID, config)
}

func TestMissingBranchIsNotUnavailable(t *testing.T) {
	r := upstreamtest.NewRemote(t)
	_, err := catalog(t).Latest(context.Background(), ID, config(r.URL, "no-such-branch"))
	if err == nil || strings.Contains(err.Error(), upstream.ErrUnavailable.Error()) {
		t.Fatalf("missing branch = %v, want a permanent error", err)
	}
}

func TestMaterializeMissingCommitIsNotUnavailable(t *testing.T) {
	r := upstreamtest.NewRemote(t)
	rev := upstream.Revision{ID: strings.Repeat("b", 40), Ref: "main"}
	_, err := catalog(t).Materialize(context.Background(), ID, config(r.URL, "main"), rev, upstreamtest.NewObjectStore(t))
	if err == nil || strings.Contains(err.Error(), upstream.ErrUnavailable.Error()) {
		t.Fatalf("absent commit = %v, want a permanent error", err)
	}
}

func TestValidate(t *testing.T) {
	for name, cfg := range map[string]*v1alpha1.GitPollConfig{
		"empty repository":  {Ref: "main"},
		"option repository": {Repository: "--upload-pack=x", Ref: "main"},
		"empty ref":         {Repository: "r"},
		"option ref":        {Repository: "r", Ref: "-x"},
		"refspec ref":       {Repository: "r", Ref: "a:b"},
		"negative interval": {Repository: "r", Ref: "main", PollInterval: durationpb.New(-1)},
	} {
		if Validate(cfg) == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := Validate(&v1alpha1.GitPollConfig{Repository: "r", Ref: "release/1.x"}); err != nil {
		t.Error(err)
	}
}

func TestMaterializeRejectsShortRevision(t *testing.T) {
	r := upstreamtest.NewRemote(t)
	_, err := catalog(t).Materialize(context.Background(), ID, config(r.URL, "main"), upstream.Revision{ID: "main"}, upstreamtest.NewObjectStore(t))
	if err == nil {
		t.Fatal("a branch name was accepted as a revision")
	}
}

var _ upstream.Watcher[*v1alpha1.GitPollConfig] = Watcher{}

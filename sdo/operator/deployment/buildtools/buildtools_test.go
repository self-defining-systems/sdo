package buildtools_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strconv"
	"testing"
	"time"

	"sdo.dev/operator/deployment/buildtools"
)

// yield polls again at once, so the tests use no real timer.
func yield(ctx context.Context, _ time.Duration) error {
	runtime.Gosched()
	return ctx.Err()
}

// A request names images; the server's build function answers it and the
// client receives the report text or the reason nothing was built.
func TestCallReturnsTheServersBuildReport(t *testing.T) {
	dir := t.TempDir()
	var asked [][]string
	server := buildtools.Server{Directory: dir, Wait: yield, Build: func(_ context.Context, images []string) (string, bool, error) {
		asked = append(asked, images)
		if len(images) == 1 && images[0] == "nope" {
			return "", false, errors.New(`"nope" is not an image this commit builds`)
		}
		return "ok currency", false, nil
	}}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	n := 0
	id := func() string { n++; return strconv.Itoa(n) }
	got, err := buildtools.Call(context.Background(), dir, buildtools.Request{Images: []string{"currency"}}, id, yield)
	if err != nil || got.Text != "ok currency" || got.Error != "" {
		t.Fatalf("got %+v, %v", got, err)
	}
	refused, err := buildtools.Call(context.Background(), dir, buildtools.Request{Images: []string{"nope"}}, id, yield)
	if err != nil || refused.Error == "" {
		t.Fatalf("got %+v, %v; want the refusal in the response", refused, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(asked[0], []string{"currency"}) || len(asked) != 2 {
		t.Fatalf("server saw %v", asked)
	}
}

// Without a server (a worker that is not a deployer task) the client fails
// fast instead of waiting forever.
func TestCallWithoutADirectoryFails(t *testing.T) {
	_, err := buildtools.Call(context.Background(), t.TempDir()+"/missing", buildtools.Request{}, func() string { return "x" }, yield)
	if err == nil {
		t.Fatal("want an error when no tool directory exists")
	}
}

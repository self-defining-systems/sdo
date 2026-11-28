package buildtools_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	server := buildtools.Server{Directory: dir, Wait: yield, Build: func(_ context.Context, images []string, _ func(string)) (string, bool, error) {
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
	got, err := buildtools.Call(context.Background(), dir, buildtools.Request{Images: []string{"currency"}}, id, yield, nil)
	if err != nil || got.Text != "ok currency" || got.Error != "" {
		t.Fatalf("got %+v, %v", got, err)
	}
	refused, err := buildtools.Call(context.Background(), dir, buildtools.Request{Images: []string{"nope"}}, id, yield, nil)
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
	_, err := buildtools.Call(context.Background(), t.TempDir()+"/missing", buildtools.Request{}, func() string { return "x" }, yield, nil)
	if err == nil {
		t.Fatal("want an error when no tool directory exists")
	}
}

// The client prints the server's progress lines while the build runs, before
// the response: a caller reading the output sees each image as it finishes.
func TestCallStreamsProgressBeforeTheResponse(t *testing.T) {
	dir := t.TempDir()
	release := make(chan struct{})
	server := buildtools.Server{Directory: dir, Wait: yield, Build: func(_ context.Context, _ []string, progress func(string)) (string, bool, error) {
		progress("  ok      a  (1 of 2, 3s)")
		<-release
		return "report", false, nil
	}}
	if err := server.Prepare(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Serve(ctx) }()
	out := &syncBuffer{seen: make(chan struct{}, 1)}
	result := make(chan buildtools.Response, 1)
	go func() {
		got, _ := buildtools.Call(context.Background(), dir, buildtools.Request{}, func() string { return "p" }, yield, out)
		result <- got
	}()
	<-out.seen
	if got := out.String(); got != "  ok      a  (1 of 2, 3s)\n" {
		t.Fatalf("progress before the response = %q", got)
	}
	close(release)
	if got := <-result; got.Text != "report" {
		t.Fatalf("response %+v", got)
	}
}

type syncBuffer struct {
	mu   sync.Mutex
	b    strings.Builder
	seen chan struct{}
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case s.seen <- struct{}{}:
	default:
	}
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

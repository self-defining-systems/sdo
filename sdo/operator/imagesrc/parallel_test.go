package imagesrc_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
)

// gate counts resolutions in flight and lets a test hold them.
type gate struct {
	mu      sync.Mutex
	active  int
	peak    int
	arrived chan string
	release chan struct{}
}

func newGate() *gate {
	return &gate{arrived: make(chan string, 16), release: make(chan struct{})}
}

func (g *gate) source(fail map[string]error) funcSource {
	return func(ctx context.Context, req imagesrc.Request) (imagesrc.Resolved, error) {
		g.mu.Lock()
		g.active++
		g.peak = max(g.peak, g.active)
		g.mu.Unlock()
		defer func() {
			g.mu.Lock()
			g.active--
			g.mu.Unlock()
		}()
		g.arrived <- req.Image
		select {
		case <-g.release:
		case <-ctx.Done():
			return imagesrc.Resolved{}, ctx.Err()
		}
		if err := fail[req.Image]; err != nil {
			return imagesrc.Resolved{}, err
		}
		return imagesrc.Resolved{Ref: imagesrctest.Repository(req.Image) + "@" + imagesrctest.Digest(req.Image), Inputs: imagesrc.Inputs{SourceImage: req.Image}}, nil
	}
}

func concurrentCatalog(t *testing.T, s funcSource, limit int) *imagesrc.Catalog {
	t.Helper()
	return catalogWith(t, "build", imagesrc.Source[*emptypb.Empty](s), imagesrc.Options{Concurrency: limit})
}

// A source registered with Concurrency resolves up to that many images at
// once: independent image builds no longer wait for one another.
func TestConcurrentSourceResolvesImagesInParallelWithinItsLimit(t *testing.T) {
	g := newGate()
	c := concurrentCatalog(t, g.source(nil), 2)
	images := []string{"r/a:1", "r/b:1", "r/c:1"}
	done := make(chan error, 1)
	var got map[string]imagesrc.Resolved
	go func() {
		var err error
		got, err = c.ResolveAll(context.Background(), images, nil, imagesrc.Request{})
		done <- err
	}()
	// Two resolutions must be in flight together before either is released.
	<-g.arrived
	<-g.arrived
	close(g.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if g.peak != 2 || len(got) != 3 {
		t.Fatalf("peak in flight = %d, resolved %d; want 2 and all 3", g.peak, len(got))
	}
}

// Plan faults of a concurrent source are joined in the order the images were
// given, whatever order the builds finished in.
func TestConcurrentSourceJoinsFailuresInImageOrder(t *testing.T) {
	g := newGate()
	close(g.release)
	c := concurrentCatalog(t, g.source(map[string]error{"r/b:1": &typedCause{"compile error in r/b:1"}, "r/d:1": &typedCause{"compile error in r/d:1"}}), 4)
	_, err := c.ResolveAll(context.Background(), []string{"r/a:1", "r/b:1", "r/c:1", "r/d:1"}, nil, imagesrc.Request{})
	if err == nil || strings.Index(err.Error(), "r/b:1") > strings.Index(err.Error(), "r/d:1") {
		t.Fatalf("err = %v; want both failures, r/b:1 first", err)
	}
	var attributed *imagesrc.Error
	if !errors.As(err, &attributed) || attributed.Image != "r/b:1" {
		t.Fatalf("errors.As = %#v; want the first failing image", attributed)
	}
}

// An infrastructure outcome of one concurrent resolution cancels the others
// and is returned alone, so the coordinator's retry recognizes it.
func TestConcurrentSourceStopsEveryResolutionAtAnInfrastructureOutcome(t *testing.T) {
	infra := &imagesrc.ResolveError{Kind: imagesrc.ResolveTransient, Err: errors.New("rate limited")}
	src := funcSource(func(ctx context.Context, req imagesrc.Request) (imagesrc.Resolved, error) {
		if req.Image == "r/b:1" {
			return imagesrc.Resolved{}, infra
		}
		<-ctx.Done()
		return imagesrc.Resolved{}, ctx.Err()
	})
	c := concurrentCatalog(t, src, 3)
	_, err := c.ResolveAll(context.Background(), []string{"r/a:1", "r/b:1", "r/c:1"}, nil, imagesrc.Request{})
	var marker interface{ InfrastructureOutcome() bool }
	if !errors.As(err, &marker) || !marker.InfrastructureOutcome() || errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want the infrastructure outcome alone", err)
	}
}

func TestRegisterRejectsANegativeConcurrency(t *testing.T) {
	c := imagesrc.NewCatalog()
	if err := imagesrc.Register(c, "s", imagesrc.Source[*emptypb.Empty](&imagesrctest.Fake{}), imagesrc.Options{Concurrency: -1}); err == nil {
		t.Fatal("a negative concurrency must be refused")
	}
}

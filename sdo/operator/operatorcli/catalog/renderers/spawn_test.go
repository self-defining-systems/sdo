package renderers_test

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/sandboxio"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

// H-iso follow-up: a Helm render child that could not be started (EAGAIN or
// ENOMEM at fork) is a host resource blip. It must reach the deployment
// source classification as transient, so a good commit is not held as broken;
// only failures after the child runs are verdicts on the chart.
func TestRenderChildThatCannotStartIsTransientNotInvalid(t *testing.T) {
	src := &contracts.DeploymentSource{RendererId: "helm"}
	cfg, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "c"})
	if err != nil {
		t.Fatal(err)
	}
	src.Config = cfg
	fsys := rendertest.MapFS{"c/Chart.yaml": "apiVersion: v2\nname: c\nversion: 0.1.0\n", "c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\n"}
	cases := map[string]struct {
		err       error
		transient bool
	}{
		"fork EAGAIN":         {fmt.Errorf("sandboxed helm-render: start child: %w: %w", sandboxio.ErrSpawn, syscall.EAGAIN), true},
		"fork ENOMEM":         {fmt.Errorf("sandboxed helm-render: start child: %w: %w", sandboxio.ErrSpawn, syscall.ENOMEM), true},
		"killed from outside": {fmt.Errorf("sandboxed helm-render: %w: signal: killed", sandboxio.ErrInterrupted), true},
		"our wall clock":      {fmt.Errorf("sandboxed helm-render stopped after 30s: %w", sandboxio.ErrWallClock), false},
		"child OOM":           {&sandboxio.Error{Name: "helm-render", Kind: sandboxio.KindMemory, Detail: "out of memory"}, false},
		"child output":        {&sandboxio.Error{Name: "helm-render", Kind: sandboxio.KindOutput, Detail: "too big"}, false},
		"child crash":         {&sandboxio.Error{Name: "helm-render", Kind: sandboxio.KindCrash, Detail: "exit 2"}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cat := renderers.CatalogWithChildRunner(func(context.Context, string, []byte, sandboxio.Limits) ([]byte, error) { return nil, tc.err })
			_, err := rendered.Desired(t.Context(), fsys, render.Env{Namespace: "n"}, cat, src)
			if err == nil {
				t.Fatal("render succeeded")
			}
			if got := errors.Is(err, rendered.ErrSourceUnavailable); got != tc.transient {
				t.Fatalf("unavailable = %v, want %v: %v", got, tc.transient, err)
			}
			if !tc.transient && !errors.Is(err, rendered.ErrSourceInvalid) {
				t.Fatalf("child failure must stay invalid: %v", err)
			}
		})
	}
}

// review main-5 F3: the render's 30 s clock is the child runner's to start,
// once the child holds a slot. Waiting for a slot ends with the caller's
// context and is transient, never a verdict on the chart.
func TestRenderQueueWaitIsNotTheChartsTime(t *testing.T) {
	src := &contracts.DeploymentSource{RendererId: "helm"}
	cfg, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "c"})
	if err != nil {
		t.Fatal(err)
	}
	src.Config = cfg
	fsys := rendertest.MapFS{"c/Chart.yaml": "apiVersion: v2\nname: c\nversion: 0.1.0\n", "c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\n"}
	var seen sandboxio.Limits
	var hadDeadline bool
	queued := func(ctx context.Context, _ string, _ []byte, limits sandboxio.Limits) ([]byte, error) {
		seen = limits
		_, hadDeadline = ctx.Deadline()
		<-ctx.Done() // both slots held: the child never starts
		return nil, fmt.Errorf("sandboxed helm-render not started: %w", context.Cause(ctx))
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	clock, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer stop()
	go func() { <-clock.Done(); cancel() }()
	_, err = rendered.Desired(ctx, fsys, render.Env{Namespace: "n"}, renderers.CatalogWithChildRunner(queued), src)
	if !errors.Is(err, rendered.ErrSourceUnavailable) || errors.Is(err, rendered.ErrSourceInvalid) {
		t.Fatalf("a render that never left the queue must be unavailable, got %v", err)
	}
	if hadDeadline {
		t.Error("the runner's context already carries a deadline: the clock started before the slot")
	}
	if seen.Wall != helm.RenderTimeLimit {
		t.Errorf("runner told to bound the render by %s, want %s", seen.Wall, helm.RenderTimeLimit)
	}
}

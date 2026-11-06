package memoryhost

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"testing/synctest"
	"time"
)

type serializedDetector struct {
	release chan struct{}
	calls   int
}

func (*serializedDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "synthetic"} }
func (d *serializedDetector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	d.calls++
	if d.calls == 1 {
		<-d.release
	}
	return []sdk.Finding{{Summary: v.Namespace()}}, nil
}
func TestConcurrentDetectorEvaluationWaitsAndUsesFreshView(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &Server{}
		d := &serializedDetector{release: make(chan struct{})}
		first := make(chan string, 1)
		second := make(chan string, 1)
		go func() {
			_, err := server.runDetector(context.Background(), d, sdktest.Snapshot{NamespaceName: "old"})
			first <- err
		}()
		synctest.Wait()
		go func() {
			findings, err := server.runDetector(context.Background(), d, sdktest.Snapshot{NamespaceName: "fresh"})
			if err != "" {
				second <- err
				return
			}
			second <- findings[0].GetSummary()
		}()
		synctest.Wait()
		select {
		case result := <-second:
			close(d.release)
			t.Fatalf("concurrent observation refused instead of waiting: %s", result)
		default:
		}
		close(d.release)
		if err := <-first; err != "" {
			t.Fatal(err)
		}
		if result := <-second; result != "fresh" {
			t.Fatalf("fresh evaluation = %s", result)
		}
		if d.calls != 2 {
			t.Fatalf("calls=%d", d.calls)
		}
	})
}

func TestWaitingDetectorEvaluationHonorsDeadlineWithoutOverlap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &Server{}
		d := &serializedDetector{release: make(chan struct{})}
		first := make(chan struct{})
		go func() { server.runDetector(context.Background(), d, sdktest.Snapshot{}); close(first) }()
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, failure := server.runDetector(ctx, d, sdktest.Snapshot{})
		if failure == "" || d.calls != 1 {
			t.Fatalf("waiting evaluation overlapped or escaped its deadline: %s, calls=%d", failure, d.calls)
		}
		close(d.release)
		<-first
	})
}

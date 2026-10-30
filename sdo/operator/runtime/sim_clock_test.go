package runtime

import (
	"sync"
	"testing"
	"time"
)

func TestSimClockConcurrentReadsAndAdvances(t *testing.T) {
	clock := newSimClock()
	start := clock.Now()
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func(writer bool) {
			defer workers.Done()
			previous := start
			for tick := 0; tick < 1000; tick++ {
				if writer {
					clock.Advance(time.Nanosecond)
				}
				now := clock.Now()
				if now.Before(previous) {
					t.Error("virtual time moved backwards during concurrent reads")
				}
				previous = now
			}
		}(worker%2 == 0)
	}
	workers.Wait()
	if got := clock.Now().Sub(start); got != 2000*time.Nanosecond {
		t.Fatalf("concurrent advances lost time: %v", got)
	}
}

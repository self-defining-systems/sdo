package runtime

import (
	"sort"
	"time"
)

// dueRead is one declared read (a pod read or vantage probe, with any lookups
// that run with it) that is due this cycle.
type dueRead struct {
	// index is the read's place in declaration order.
	index int
	// last is when the read last gave an answer; zero when it never did.
	last time.Time
	// cost is the reads that start when it runs: one, plus its lookups.
	cost int
	// first starts it before every read without it (a fresh read's queries).
	first bool
}

// rotate picks the due reads a cycle starts: those marked first, then least
// recently answered first (never answered first), then in declaration order, while their cost fits
// budget. The rest keep their last answer and stay due, so declarations whose
// load fits the budget per base interval rotate through it, and no cycle
// starts more than budget reads.
func rotate(due []dueRead, budget int) map[int]bool {
	sort.SliceStable(due, func(i, j int) bool {
		if due[i].first != due[j].first {
			return due[i].first
		}
		if !due[i].last.Equal(due[j].last) {
			return due[i].last.Before(due[j].last)
		}
		return due[i].index < due[j].index
	})
	start := map[int]bool{}
	used := 0
	for _, read := range due {
		if used+read.cost <= budget {
			start[read.index] = true
			used += read.cost
		}
	}
	return start
}

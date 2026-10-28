package broker_test

import (
	"os"
	"sdo.dev/operator/broker"
	"testing"
)

func operatorCLIFixture(t *testing.T) string { return broker.OperatorCLIFixture(t) }

func TestOperatorCLIFixtureReusesExecutable(t *testing.T) {
	first := operatorCLIFixture(t)
	if first != operatorCLIFixture(t) || first != broker.OperatorCLIFixture(t) {
		t.Fatal("CLI fixture must reuse the same executable")
	}
	if _, err := os.Stat(first); err != nil {
		t.Fatal(err)
	}
}

// Allocation takes an index, so adding a named case needs no placement edit.
func TestAcceptanceCaseAllocationIsExhaustiveAndBalanced(t *testing.T) {
	for size := 1; size <= 100; size++ {
		counts := make([]int, acceptanceShardCount)
		for index := 0; index < size; index++ {
			owner := automaticAcceptanceShard(index)
			if owner < 0 || owner >= len(counts) {
				t.Fatalf("case %d has invalid owner %d", index, owner)
			}
			counts[owner]++
		}
		min, max := counts[0], counts[0]
		total := 0
		for _, count := range counts {
			total += count
			if count < min {
				min = count
			}
			if count > max {
				max = count
			}
		}
		if total != size || max-min > 1 {
			t.Fatalf("%d cases must be covered once and balanced: %v", size, counts)
		}
	}
}

package wire

import (
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Cache writes (creation) and cache reads are independent counters: a turn can write more cache than it reads.
func TestUsageCacheWritesAreIndependentOfCacheReads(t *testing.T) {
	for name, usage := range map[string]*contracts.UsageMetrics{
		"no read field": {InputTokens: 1000, CachedInputTokens: 10, CacheWriteInputTokens: 500},
		"read field":    {InputTokens: 1000, CachedInputTokens: 10, CacheReadInputTokens: proto.Int64(10), CacheWriteInputTokens: 500},
	} {
		if err := Validate(usage); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

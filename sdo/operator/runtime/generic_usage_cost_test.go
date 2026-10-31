package runtime

import (
	"fmt"
	"strings"
	"testing"
)

// BenchmarkGenericThrottlingParse measures the optional extra cAdvisor payload
// without requiring a kubelet. Transport latency and kubelet generation cost are
// deliberately not inferred from this local benchmark.
func BenchmarkGenericThrottlingParse(b *testing.B) {
	var text strings.Builder
	for i := 0; i < 2000; i++ {
		fmt.Fprintf(&text, "container_cpu_cfs_periods_total{namespace=\"tenant\",pod=\"p-%d\",container=\"worker\"} 100\ncontainer_cpu_cfs_throttled_periods_total{namespace=\"tenant\",pod=\"p-%d\",container=\"worker\"} 30\n", i, i)
	}
	data := []byte(text.String())
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := parseUsageCounters(data, "tenant"); err != nil {
			b.Fatal(err)
		}
	}
}

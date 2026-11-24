package prober

import (
	"sync"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

const (
	// Keep the default request-metrics lookback plus its rolling window.
	httpReceiptAge = 6 * time.Minute
	// Bound the payload even during high-rate bursts and on long probe paths.
	maxHTTPReceipts = 1024
	maxReceiptPath  = 256
)

type httpJournal struct {
	mu      sync.Mutex
	entries []traffic.HTTPReceipt
	next    int
}

func (j *httpJournal) record(receipt traffic.HTTPReceipt) {
	if len(receipt.PathKey) > maxReceiptPath {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.entries) < maxHTTPReceipts {
		j.entries = append(j.entries, receipt)
		return
	}
	j.entries[j.next] = receipt
	j.next = (j.next + 1) % maxHTTPReceipts
}

func (j *httpJournal) recent(now time.Time) []traffic.HTTPReceipt {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []traffic.HTTPReceipt
	for i := range j.entries {
		receipt := j.entries[(j.next+i)%len(j.entries)]
		if now.Sub(receipt.Answered) <= httpReceiptAge {
			out = append(out, receipt)
		}
	}
	return out
}

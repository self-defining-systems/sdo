package traffic

import (
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"time"
)

// HTTPReceiptWindowID is the reserved observation key for process-wide HTTP
// receipts. It cannot be the name of a workload.
const HTTPReceiptWindowID = "@http-requests"

// HTTPReceipt records the request interval and the response seen by the probe.
type HTTPReceipt struct {
	Service string `json:"service"`
	Port    int    `json:"port"`
	// PathKey contains one digest per nonempty route segment, so saved
	// receipts never contain path parameter values.
	PathKey  string    `json:"path_key"`
	Method   string    `json:"method"`
	SentAt   time.Time `json:"sent_at"`
	Answered time.Time `json:"answered_at"`
	Status   int       `json:"status"`
}

// HTTPPathKey matches literal segments of a request path without keeping the
// original segments in the observation. Empty segments preserve separators.
func HTTPPathKey(path string) string {
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if segment != "" {
			sum := sha256.Sum256([]byte(segment))
			segments[i] = base64.RawURLEncoding.EncodeToString(sum[:16])
		}
	}
	return strings.Join(segments, "/")
}

package runtime

import (
	"time"
)

func fastSyncPolicy() InformerSyncPolicy {
	return InformerSyncPolicy{Attempts: 3, Timeout: 100 * time.Millisecond, Backoff: time.Millisecond, Budget: 5 * time.Second}
}

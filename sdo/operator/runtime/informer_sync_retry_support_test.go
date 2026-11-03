package runtime

import "time"

func fastSyncPolicy() InformerSyncPolicy {
	return InformerSyncPolicy{Timeout: 100 * time.Millisecond, Backoff: time.Millisecond, MaxBackoff: time.Second}
}

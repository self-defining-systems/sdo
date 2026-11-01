package runtime

import (
	kclock "k8s.io/utils/clock"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

func timerClock(clock kclock.WithTicker) kclock.WithTicker {
	if clock != nil {
		return clock
	}
	return lifecycleio.Clock()
}

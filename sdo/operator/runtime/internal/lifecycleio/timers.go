package lifecycleio

import kclock "k8s.io/utils/clock"

// Clock supplies production timers through client-go's injectable clock contract.
func Clock() kclock.WithTickerAndDelayedExecution { return kclock.RealClock{} }

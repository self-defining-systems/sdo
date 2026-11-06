package safefs

import "sync/atomic"

var tempCounter atomic.Uint64

func nextTemp() uint64 { return tempCounter.Add(1) }

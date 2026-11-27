package memory

// maxUnprobedMultiple bounds how much less often than the probed reads the
// others run, which bounds how old their answers can get.
const maxUnprobedMultiple = 4

// readMultiples spreads Go's own reads over a load budget: p reads on probed
// user paths and u others, each read on a multiple of the base interval, so
// that p/kp + u/ku <= budget reads per base interval and
// ku <= maxUnprobedMultiple*kp. kp is the smallest multiple for which such a ku
// exists, and ku the smallest no smaller than kp that fits the budget. Where the
// others would fall further behind, the probed reads give up part of their share
// and run less often: ranking decides how often a read runs, never whether it
// exists. With no probed reads both are the multiple that fits all u.
func readMultiples(p, u, budget int) (kp, ku int) {
	ceilDiv := func(a, b int) int { return (a + b - 1) / b }
	if p == 0 {
		k := max(1, ceilDiv(u, budget))
		return k, k
	}
	for kp = 1; ; kp++ {
		// The others may use budget - p/kp reads per base interval, that is
		// (budget*kp - p)/kp; with u > 0 that must be positive. A larger kp
		// leaves more room and allows a larger ku, so the loop ends.
		room := budget*kp - p
		switch {
		case u == 0 && room >= 0:
			return kp, kp
		case u > 0 && room > 0:
			if ku = max(kp, ceilDiv(u*kp, room)); ku <= maxUnprobedMultiple*kp {
				return kp, ku
			}
		}
	}
}

//go:build integration

package runtime

// Random-plan seeds that pin an old regression or kill a recorded controller
// mutant. A new regression registers its seed next to its own scenario or test
// (registerPRSeed), not here, so PRs do not edit the same lines.
func init() {
	// Seeds 1 to 3 together kill both recorded controller mutants and take
	// about 50 s.
	registerPRSeed(1, "kills recorded controller mutants (with seeds 2 and 3)")
	registerPRSeed(2, "kills recorded controller mutants (with seeds 1 and 3)")
	registerPRSeed(3, "kills recorded controller mutants (with seeds 1 and 2)")
	// Seed 10: a due dispatch retry that something else held made the
	// controller loop spin on a frozen clock (NextWake kept returning the due
	// time); without the fix it hangs rather than fails.
	registerPRSeed(10, "due dispatch retry held elsewhere spun the loop on a frozen clock")
	// Seed 13: a hung detector plus a host crash delayed every other detector
	// (the manager held locks across the evaluation); seeds 23 and 27 reach the
	// same path in the nightly sweep.
	registerPRSeed(13, "hung detector plus host crash delayed every other detector")
	// Seed 19 kills the mutant that never ends a hung agent (its first
	// dispatch hangs, and nothing but the controller can stop it).
	registerPRSeed(19, "kills the mutant that never ends a hung agent")
	// Seed 35 kills the mutant that ends the process on a transient API error
	// (startup reads under errors).
	registerPRSeed(35, "kills the mutant that exits on a transient API error")
}

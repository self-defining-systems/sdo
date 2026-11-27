package journeycheck

import "time"

// Options is the input of the generated journey-check program.
type Options struct {
	Namespace string
	SeedPath  string
	Only      []string
	Alter     bool
	// Soak runs passing scenarios for this long; 0 skips it.
	Soak time.Duration
	// Plan reads the schedule only: no cluster, nothing sent.
	Plan            bool
	Once            bool
	Verbose         bool
	DetectionTarget time.Duration
}

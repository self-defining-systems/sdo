package lifecycleio

import "time"

// Now supplies the production default for runtime's injected observation clocks.
func Now() time.Time { return time.Now() }

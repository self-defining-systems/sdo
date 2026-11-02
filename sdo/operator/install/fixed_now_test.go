package install

import "time"

// fixedNow is the instant tests use wherever the code under test takes an injected clock.
var fixedNow = time.Date(2026, time.March, 4, 5, 6, 7, 0, time.UTC)

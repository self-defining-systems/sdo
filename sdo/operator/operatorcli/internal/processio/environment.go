package processio

import (
	"os"
	"time"
)

// Environment reads CLI process configuration at the I/O boundary.
func Environment(key string) string { return os.Getenv(key) }

// Now reads the wall clock for staged onboarding timestamps.
func Now() time.Time { return time.Now() }

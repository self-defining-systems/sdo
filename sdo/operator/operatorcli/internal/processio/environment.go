package processio

import "os"

// Environment reads CLI process configuration at the I/O boundary.
func Environment(key string) string { return os.Getenv(key) }

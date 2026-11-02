package operatorcli

import "sdo.dev/operator/operatorcli/internal/processio"

// Environment reads one process environment variable for the command entry point.
func Environment(key string) string { return processio.Environment(key) }

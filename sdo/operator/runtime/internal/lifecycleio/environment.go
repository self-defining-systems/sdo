package lifecycleio

import "os"

// TimeScaleEnvironment reads the development timing setting at the runtime edge.
func TimeScaleEnvironment() string { return os.Getenv("SDO_TIME_SCALE") }

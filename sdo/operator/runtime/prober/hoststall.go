package prober

import (
	"os"
	"strconv"
	"strings"
)

// hostCPUPressurePath is the kernel-wide CPU pressure file. Inside a
// container /proc/pressure is not namespaced, so it measures the whole host,
// unlike the container's own cgroup cpu.pressure.
const hostCPUPressurePath = "/proc/pressure/cpu"

// HostCPUStall reads the host-wide CPU stall where the prober runs: the
// percent of the last ten seconds some runnable task waited for a CPU (PSI
// "some" avg10). False when the kernel does not report pressure.
func HostCPUStall() (float64, bool) {
	data, err := os.ReadFile(hostCPUPressurePath)
	if err != nil {
		return 0, false
	}
	return ParseCPUPressure(string(data))
}

// ParseCPUPressure returns the "some" avg10 value of a PSI pressure file.
func ParseCPUPressure(text string) (float64, bool) {
	for line := range strings.SplitSeq(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "some" {
			continue
		}
		for _, field := range fields[1:] {
			if value, ok := strings.CutPrefix(field, "avg10="); ok {
				percent, err := strconv.ParseFloat(value, 64)
				return percent, err == nil && percent >= 0
			}
		}
	}
	return 0, false
}

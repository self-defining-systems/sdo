package bench

import "testing"

func TestParseCanaryReadsMemoryPressure(t *testing.T) {
	c, err := parseCanary([]byte("psi-mem-full 65.12\n{\"rc\":0,\"ip_ms\":3,\"hung\":0}\n"))
	if err != nil || c.MemPSIFull != 65.12 {
		t.Fatalf("%+v %v", c, err)
	}
	if c, _ = parseCanary([]byte("{\"rc\":0,\"ip_ms\":3,\"hung\":0}\n")); c.MemPSIFull != 0 {
		t.Fatalf("no PSI file is no pressure signal: %+v", c)
	}
}

package vantage

import (
	"fmt"
	"strconv"
)

// The commands run in the helper container with a POSIX shell and the
// BusyBox tools (adjtimex, awk, nc, wget), in the Pod's network namespace. A
// peer is passed as positional arguments, never spliced into the script.
// After each dial a command prints the node's CPU stall ("psi <avg10>" from
// /proc/pressure/cpu, which a container does not namespace), so a client dial
// records host load as a prober dial does.
const clock = `now() { adjtimex | awk '/tv_sec/{s=$2} /tv_usec/{u=$2} END{print s, u}'; }
psi() { awk '$1=="some"{split($2,a,"="); if(a[1]=="avg10") print "psi", a[2]}' /proc/pressure/cpu 2>/dev/null; }
`

var scripts = map[Kind]string{
	TCPConnect: clock + `i=0; while [ $i -lt ` + strconv.Itoa(DialsPerProbe) + ` ]; do i=$((i+1))
a=$(now); out=$(nc -z -w ` + strconv.Itoa(int(DialTimeout.Seconds())) + ` "$1" "$2" </dev/null 2>&1); rc=$?; b=$(now)
echo "dial $rc $a $b $(echo "$out" | head -c 160 | tr '\n' ' ')"; psi; done
`,
	HTTPGet: clock + `i=0; while [ $i -lt ` + strconv.Itoa(DialsPerProbe) + ` ]; do i=$((i+1))
a=$(now); out=$(wget -q -S -O /dev/null -T ` + strconv.Itoa(int(DialTimeout.Seconds())) + ` "http://$1:$2$3" 2>&1); rc=$?; b=$(now)
echo "dial $rc $a $b $(echo "$out" | grep -E 'HTTP/|wget:' | head -n 2 | head -c 160 | tr '\n' ' ')"; psi; done
`,
}

// Command returns the fixed argument vector the helper runs for a probe.
func Command(probe Probe) ([]string, error) {
	script, ok := scripts[probe.Kind]
	if !ok {
		return nil, fmt.Errorf("unknown vantage kind %q", probe.Kind)
	}
	args := []string{"sh", "-c", script, "sdo-vantage"}
	if probe.Kind.Dials() {
		if probe.To == nil {
			return nil, fmt.Errorf("vantage probe %q has no peer", probe.ID)
		}
		args = append(args, probe.To.Host, strconv.Itoa(probe.To.Port), probe.To.Path)
	}
	return args, nil
}

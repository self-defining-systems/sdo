package vantage

import (
	"fmt"
	"strconv"
)

// The commands run in the helper container with a POSIX shell and the
// BusyBox tools (adjtimex, awk, df, nc, tc, wget). They read only: /proc,
// the target container's cgroup and /dev/shm through its process root
// (/proc/PID/root), and the Pod's network namespace. The target processes
// are those outside the helper's own cgroup. A peer is passed as positional
// arguments, never spliced into the script.
var targets = `self=$(cat /proc/self/cgroup); pids=""; n=0
for d in /proc/[0-9]*; do c=$(cat "$d/cgroup" 2>/dev/null) || continue; [ "$c" = "$self" ] && continue
pids="$pids ${d#/proc/}"; n=$((n+1)); [ $n -ge ` + strconv.Itoa(MaxProcesses) + ` ] && break; done
first=""; for p in $pids; do first=$p; break; done
[ -n "$first" ] || { echo "no target process is visible" >&2; exit 3; }
root=/proc/$first/root
`

const clock = `now() { adjtimex | awk '/tv_sec/{s=$2} /tv_usec/{u=$2} END{print s, u}'; }
`

var scripts = map[Kind]string{
	TCPConnect: clock + `i=0; while [ $i -lt ` + strconv.Itoa(DialsPerProbe) + ` ]; do i=$((i+1))
a=$(now); out=$(nc -z -w ` + strconv.Itoa(int(DialTimeout.Seconds())) + ` "$1" "$2" </dev/null 2>&1); rc=$?; b=$(now)
echo "dial $rc $a $b $(echo "$out" | head -c 160 | tr '\n' ' ')"; done
`,
	HTTPGet: clock + `i=0; while [ $i -lt ` + strconv.Itoa(DialsPerProbe) + ` ]; do i=$((i+1))
a=$(now); out=$(wget -q -S -O /dev/null -T ` + strconv.Itoa(int(DialTimeout.Seconds())) + ` "http://$1:$2$3" 2>&1); rc=$?; b=$(now)
echo "dial $rc $a $b $(echo "$out" | grep -E 'HTTP/|wget:' | head -n 2 | head -c 160 | tr '\n' ' ')"; done
`,
	Qdisc: `tc qdisc show`,
	ProcLimits: targets + `for p in $pids; do echo "pid $p $(head -c 32 /proc/$p/comm 2>/dev/null)"
grep -E '^Max (open files|processes) ' /proc/$p/limits 2>/dev/null
if ls /proc/$p/fd >/dev/null 2>&1; then echo "fds $(ls /proc/$p/fd | wc -l)"; else echo "fds ?"; fi; done
`,
	ProcState: targets + `for p in $pids; do s=$(cat /proc/$p/stat 2>/dev/null) || continue; s=${s##*) }
echo "pid $p ${s%% *} $(head -c 32 /proc/$p/comm 2>/dev/null)"; done
`,
	CgroupCPU:     targets + `for f in cpu.max cpu.stat cpu.pressure; do echo "== $f"; cat "$root/sys/fs/cgroup/$f" 2>&1; done`,
	CgroupMemory:  targets + `for f in memory.max memory.high memory.current memory.events memory.pressure; do echo "== $f"; cat "$root/sys/fs/cgroup/$f" 2>&1; done`,
	CgroupIO:      targets + `for f in io.max io.pressure; do echo "== $f"; cat "$root/sys/fs/cgroup/$f" 2>&1; done`,
	ShmUsage:      targets + `df -k "$root/dev/shm" 2>/dev/null || df -k /dev/shm`,
	SocketSummary: `awk 'FNR>1 { split($3, a, ":"); print a[1] ":" a[2], $4 }' /proc/net/tcp /proc/net/tcp6 2>/dev/null | sort | uniq -c | sort -rn | head -n 200`,
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

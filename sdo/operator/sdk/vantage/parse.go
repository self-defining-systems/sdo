package vantage

import (
	"encoding/hex"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Facts of each kind. Counts and sizes are decimal integers; percentages and
// pressure averages are decimals.
const (
	// Dialing kinds.
	FactStatus = "status" // HTTP status of the last answered request
	// Qdisc.
	FactQdiscs     = "qdiscs"      // "dev:kind" pairs, space separated, in output order
	FactNetemDevs  = "netem"       // devices with a netem qdisc, space separated; absent when none
	FactNetemLines = "netem_qdisc" // the first netem qdisc line, for evidence
	FactShaping    = "shaping"     // devices with any shaping qdisc (netem, tbf, htb, cake), space separated
	// ProcLimits.
	FactProcesses     = "processes"
	FactNofileSoftMin = "nofile_soft_min" // lowest open-files soft limit ("unlimited" counts as no limit)
	FactNofileHardMin = "nofile_hard_min"
	FactNofileMinPID  = "nofile_soft_min_pid"
	FactNofileMinComm = "nofile_soft_min_comm"
	FactNprocSoftMin  = "nproc_soft_min"
	FactFDsMax        = "fds_max"        // most open descriptors of one process
	FactFDUsePctMax   = "fd_use_pct_max" // highest open descriptors / soft limit, percent
	FactFDsUnreadable = "fds_unreadable" // processes whose descriptors could not be listed
	// ProcState.
	FactRunning         = "running"
	FactSleeping        = "sleeping"
	FactStopped         = "stopped"
	FactZombie          = "zombie"
	FactUninterruptible = "uninterruptible"
	FactStoppedPIDs     = "stopped_pids" // "pid:comm" pairs, space separated
	FactZombiePIDs      = "zombie_pids"
	FactDiskPIDs        = "uninterruptible_pids"
	// CgroupCPU.
	FactCPUMax           = "cpu_max"              // raw cpu.max
	FactCPULimitMillis   = "cpu_limit_millicores" // absent when unlimited
	FactCPUPeriods       = "nr_periods"
	FactCPUThrottled     = "nr_throttled"
	FactCPUThrottledUsec = "throttled_usec"
	FactCPUThrottledPct  = "throttled_pct" // nr_throttled / nr_periods since the cgroup started, percent
	// Pressure (cpu, memory, io).
	FactPressureSome10 = "pressure_some_avg10"
	FactPressureSome60 = "pressure_some_avg60"
	FactPressureFull10 = "pressure_full_avg10"
	// CgroupMemory.
	FactMemoryMax     = "memory_max" // bytes; absent when unlimited
	FactMemoryHigh    = "memory_high"
	FactMemoryCurrent = "memory_current"
	FactMemoryUsePct  = "memory_use_pct"
	FactOOMKill       = "oom_kill"
	FactOOM           = "oom"
	FactMemoryEvMax   = "max_events"
	// CgroupIO.
	FactIOMax = "io_max" // io.max lines joined with "; "; absent when unlimited
	// ShmUsage.
	FactShmSizeKB  = "size_kb"
	FactShmUsedKB  = "used_kb"
	FactShmAvailKB = "available_kb"
	FactShmUsedPct = "used_pct"
	// SocketSummary: "state.<STATE>" counts all sockets in a state and
	// "peer.<ip:port>" lists a remote peer's states as STATE=count pairs.
	FactSockets = "sockets"
)

// Parse turns a probe command's output into a reading's facts, or, for a
// dialing kind, its dials. It fails when the output is not the command's.
func Parse(kind Kind, stdout string, observedAt time.Time) (facts map[string]string, dials []Dial, err error) {
	switch kind {
	case TCPConnect, HTTPGet:
		dials, err = parseDials(kind, stdout, observedAt)
		facts = map[string]string{}
		for _, dial := range slices.Backward(dials) {
			if dial.Status != "" {
				facts[FactStatus] = dial.Status
				break
			}
		}
		return facts, dials, err
	case Qdisc:
		return parseQdisc(stdout), nil, nil
	case ProcLimits:
		facts, err = parseLimits(stdout)
	case ProcState:
		facts, err = parseStates(stdout)
	case CgroupCPU, CgroupMemory, CgroupIO:
		facts, err = parseCgroup(kind, stdout)
	case ShmUsage:
		facts, err = parseDF(stdout)
	case SocketSummary:
		facts, err = parseSockets(stdout)
	default:
		err = fmt.Errorf("unknown vantage kind %q", kind)
	}
	return facts, nil, err
}

func parseDials(kind Kind, stdout string, observedAt time.Time) ([]Dial, error) {
	var dials []Dial
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 || fields[0] != "dial" {
			continue
		}
		numbers := make([]int64, 5)
		for i := range numbers {
			n, err := strconv.ParseInt(fields[i+1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("malformed dial line %q", line)
			}
			numbers[i] = n
		}
		start := time.Unix(numbers[1], numbers[2]*1000)
		end := time.Unix(numbers[3], numbers[4]*1000)
		message := strings.TrimSpace(strings.Join(fields[6:], " "))
		dial := Dial{At: end.UTC(), Duration: max(0, end.Sub(start)), OK: numbers[0] == 0}
		if kind == HTTPGet {
			dial.Status = httpStatus(message)
			if code, err := strconv.Atoi(dial.Status); err == nil {
				dial.OK = code >= 100 && code <= 499
			}
		}
		if !dial.OK {
			dial.Class, dial.Error = dialFailure(kind, message, dial)
		}
		dials = append(dials, dial)
	}
	if len(dials) == 0 {
		return nil, fmt.Errorf("no dial was reported")
	}
	// The helper's clock dates the dials; the observation time bounds them,
	// so a skewed node clock cannot date a dial after its observation.
	if shift := dials[len(dials)-1].At.Sub(observedAt); shift > 0 {
		for i := range dials {
			dials[i].At = dials[i].At.Add(-shift)
		}
	}
	return dials, nil
}

func httpStatus(message string) string {
	index := strings.Index(message, "HTTP/")
	if index < 0 {
		return ""
	}
	fields := strings.Fields(message[index:])
	if len(fields) < 2 {
		return ""
	}
	if _, err := strconv.Atoi(fields[1]); err != nil {
		return ""
	}
	return fields[1]
}

// dialFailure classifies a failed dial (traffic.LinkFailureClass values).
func dialFailure(kind Kind, message string, dial Dial) (string, string) {
	lower := strings.ToLower(message)
	text := message
	if text == "" {
		text = "no answer"
	}
	switch {
	case dial.Status != "":
		return "not_serving", "HTTP " + dial.Status
	case strings.Contains(lower, "bad address") || strings.Contains(lower, "unknown host") || strings.Contains(lower, "resolve"):
		return "transport_error", text
	case strings.Contains(lower, "refused"):
		return "refused", text
	case strings.Contains(lower, "reset"):
		return "reset", text
	case dial.Duration >= DialTimeout*9/10 || strings.Contains(lower, "timed out"):
		return "timeout", "no connection within " + DialTimeout.String()
	case kind == TCPConnect:
		// BusyBox nc -z reports a refused connection by its exit code alone.
		return "refused", text
	}
	return "transport_error", text
}

var shapingQdiscs = map[string]bool{"netem": true, "tbf": true, "htb": true, "cake": true, "hfsc": true, "cbq": true}

func parseQdisc(stdout string) map[string]string {
	facts := map[string]string{}
	var pairs, netem, shaping []string
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "qdisc" {
			continue
		}
		kind, dev := fields[1], ""
		if index := slices.Index(fields, "dev"); index >= 0 && index+1 < len(fields) {
			dev = fields[index+1]
		}
		pairs = append(pairs, dev+":"+kind)
		if kind == "netem" {
			if !slices.Contains(netem, dev) {
				netem = append(netem, dev)
			}
			if _, ok := facts[FactNetemLines]; !ok {
				facts[FactNetemLines] = strings.TrimSpace(line)
			}
		}
		if shapingQdiscs[kind] && !slices.Contains(shaping, dev) {
			shaping = append(shaping, dev)
		}
	}
	facts[FactQdiscs] = strings.Join(pairs, " ")
	if len(netem) > 0 {
		facts[FactNetemDevs] = strings.Join(netem, " ")
	}
	if len(shaping) > 0 {
		facts[FactShaping] = strings.Join(shaping, " ")
	}
	return facts
}

const unlimited = -1

func limitValue(value string) int64 {
	if value == "unlimited" {
		return unlimited
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return unlimited
	}
	return n
}

func parseLimits(stdout string) (map[string]string, error) {
	type process struct {
		pid, comm                   string
		nofileSoft, nofileHard, fds int64
		nprocSoft                   int64
		fdsKnown                    bool
	}
	var processes []*process
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		switch {
		case len(fields) >= 2 && fields[0] == "pid":
			processes = append(processes, &process{pid: fields[1], comm: strings.Join(fields[2:], " "), nofileSoft: unlimited, nofileHard: unlimited, nprocSoft: unlimited})
		case len(processes) == 0:
		case strings.HasPrefix(line, "Max open files") && len(fields) >= 5:
			processes[len(processes)-1].nofileSoft, processes[len(processes)-1].nofileHard = limitValue(fields[3]), limitValue(fields[4])
		case strings.HasPrefix(line, "Max processes") && len(fields) >= 4:
			processes[len(processes)-1].nprocSoft = limitValue(fields[2])
		case len(fields) == 2 && fields[0] == "fds":
			if n, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
				processes[len(processes)-1].fds, processes[len(processes)-1].fdsKnown = n, true
			}
		}
	}
	if len(processes) == 0 {
		return nil, fmt.Errorf("no process limits were reported")
	}
	facts := map[string]string{FactProcesses: strconv.Itoa(len(processes))}
	var softMin, hardMin, nprocMin, fdsMax int64 = unlimited, unlimited, unlimited, -1
	usePct, unreadable := -1.0, 0
	lower := func(current, value int64) bool {
		return value != unlimited && (current == unlimited || value < current)
	}
	for _, p := range processes {
		if lower(softMin, p.nofileSoft) {
			softMin = p.nofileSoft
			facts[FactNofileMinPID], facts[FactNofileMinComm] = p.pid, p.comm
		}
		if lower(hardMin, p.nofileHard) {
			hardMin = p.nofileHard
		}
		if lower(nprocMin, p.nprocSoft) {
			nprocMin = p.nprocSoft
		}
		if !p.fdsKnown {
			unreadable++
			continue
		}
		fdsMax = max(fdsMax, p.fds)
		if p.nofileSoft > 0 {
			usePct = max(usePct, float64(p.fds)*100/float64(p.nofileSoft))
		}
	}
	for key, value := range map[string]int64{FactNofileSoftMin: softMin, FactNofileHardMin: hardMin, FactNprocSoftMin: nprocMin} {
		if value != unlimited {
			facts[key] = strconv.FormatInt(value, 10)
		}
	}
	if fdsMax >= 0 {
		facts[FactFDsMax] = strconv.FormatInt(fdsMax, 10)
	}
	if usePct >= 0 {
		facts[FactFDUsePctMax] = strconv.FormatFloat(usePct, 'f', 1, 64)
	}
	facts[FactFDsUnreadable] = strconv.Itoa(unreadable)
	return facts, nil
}

func parseStates(stdout string) (map[string]string, error) {
	counts := map[string]int{}
	lists := map[string][]string{}
	seen := 0
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "pid" {
			continue
		}
		seen++
		entry := fields[1] + ":" + strings.Join(fields[3:], "_")
		switch fields[2] {
		case "R":
			counts[FactRunning]++
		case "S", "I":
			counts[FactSleeping]++
		case "T", "t":
			counts[FactStopped]++
			lists[FactStoppedPIDs] = append(lists[FactStoppedPIDs], entry)
		case "Z", "X":
			counts[FactZombie]++
			lists[FactZombiePIDs] = append(lists[FactZombiePIDs], entry)
		case "D":
			counts[FactUninterruptible]++
			lists[FactDiskPIDs] = append(lists[FactDiskPIDs], entry)
		}
	}
	if seen == 0 {
		return nil, fmt.Errorf("no process states were reported")
	}
	facts := map[string]string{FactProcesses: strconv.Itoa(seen)}
	for _, key := range []string{FactRunning, FactSleeping, FactStopped, FactZombie, FactUninterruptible} {
		facts[key] = strconv.Itoa(counts[key])
	}
	for key, list := range lists {
		if len(list) > 16 {
			list = list[:16]
		}
		facts[key] = strings.Join(list, " ")
	}
	return facts, nil
}

func parseCgroup(kind Kind, stdout string) (map[string]string, error) {
	files := map[string][]string{}
	current := ""
	for _, line := range strings.Split(stdout, "\n") {
		if name, ok := strings.CutPrefix(line, "== "); ok {
			current = strings.TrimSpace(name)
			files[current] = nil
			continue
		}
		if current != "" && strings.TrimSpace(line) != "" {
			files[current] = append(files[current], strings.TrimSpace(line))
		}
	}
	readable := func(name string) ([]string, bool) {
		lines, ok := files[name]
		if !ok || len(lines) == 0 || strings.Contains(lines[0], "No such file") || strings.Contains(lines[0], "Permission denied") || strings.HasPrefix(lines[0], "cat:") {
			return nil, false
		}
		return lines, true
	}
	facts := map[string]string{}
	keyed := func(lines []string) map[string]string {
		out := map[string]string{}
		for _, line := range lines {
			if fields := strings.Fields(line); len(fields) == 2 {
				out[fields[0]] = fields[1]
			}
		}
		return out
	}
	pressure := func(name string) {
		lines, ok := readable(name)
		if !ok {
			return
		}
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			for _, field := range fields[1:] {
				key, value, _ := strings.Cut(field, "=")
				switch {
				case fields[0] == "some" && key == "avg10":
					facts[FactPressureSome10] = value
				case fields[0] == "some" && key == "avg60":
					facts[FactPressureSome60] = value
				case fields[0] == "full" && key == "avg10":
					facts[FactPressureFull10] = value
				}
			}
		}
	}
	switch kind {
	case CgroupCPU:
		lines, ok := readable("cpu.max")
		if !ok {
			return nil, cgroupError(files, "cpu.max")
		}
		facts[FactCPUMax] = lines[0]
		if fields := strings.Fields(lines[0]); len(fields) == 2 && fields[0] != "max" {
			quota, errQ := strconv.ParseInt(fields[0], 10, 64)
			period, errP := strconv.ParseInt(fields[1], 10, 64)
			if errQ == nil && errP == nil && period > 0 {
				facts[FactCPULimitMillis] = strconv.FormatInt(quota*1000/period, 10)
			}
		}
		if lines, ok := readable("cpu.stat"); ok {
			stat := keyed(lines)
			facts[FactCPUPeriods], facts[FactCPUThrottled], facts[FactCPUThrottledUsec] = stat["nr_periods"], stat["nr_throttled"], stat["throttled_usec"]
			periods, _ := strconv.ParseFloat(stat["nr_periods"], 64)
			throttled, _ := strconv.ParseFloat(stat["nr_throttled"], 64)
			if periods > 0 {
				facts[FactCPUThrottledPct] = strconv.FormatFloat(throttled*100/periods, 'f', 1, 64)
			}
		}
		pressure("cpu.pressure")
	case CgroupMemory:
		lines, ok := readable("memory.max")
		if !ok {
			return nil, cgroupError(files, "memory.max")
		}
		if lines[0] != "max" {
			facts[FactMemoryMax] = lines[0]
		}
		if lines, ok := readable("memory.high"); ok && lines[0] != "max" {
			facts[FactMemoryHigh] = lines[0]
		}
		if lines, ok := readable("memory.current"); ok {
			facts[FactMemoryCurrent] = lines[0]
			used, _ := strconv.ParseFloat(lines[0], 64)
			if limit, err := strconv.ParseFloat(facts[FactMemoryMax], 64); err == nil && limit > 0 {
				facts[FactMemoryUsePct] = strconv.FormatFloat(used*100/limit, 'f', 1, 64)
			}
		}
		if lines, ok := readable("memory.events"); ok {
			events := keyed(lines)
			facts[FactOOMKill], facts[FactOOM], facts[FactMemoryEvMax] = events["oom_kill"], events["oom"], events["max"]
		}
		pressure("memory.pressure")
	case CgroupIO:
		if _, ok := files["io.max"]; !ok {
			return nil, cgroupError(files, "io.max")
		}
		if lines, ok := readable("io.max"); ok {
			facts[FactIOMax] = strings.Join(lines, "; ")
		} else if len(files["io.max"]) > 0 {
			return nil, cgroupError(files, "io.max")
		}
		pressure("io.pressure")
	}
	for key, value := range facts {
		if value == "" {
			delete(facts, key)
		}
	}
	return facts, nil
}

func cgroupError(files map[string][]string, name string) error {
	if lines := files[name]; len(lines) > 0 {
		return fmt.Errorf("container cgroup %s unreadable: %s", name, lines[0])
	}
	return fmt.Errorf("container cgroup %s was not reported", name)
}

func parseDF(stdout string) (map[string]string, error) {
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		fields := strings.Fields(lines[i])
		if len(fields) < 6 {
			continue
		}
		size, errS := strconv.ParseInt(fields[1], 10, 64)
		used, errU := strconv.ParseInt(fields[2], 10, 64)
		available, errA := strconv.ParseInt(fields[3], 10, 64)
		if errS != nil || errU != nil || errA != nil {
			continue
		}
		facts := map[string]string{
			FactShmSizeKB: strconv.FormatInt(size, 10), FactShmUsedKB: strconv.FormatInt(used, 10), FactShmAvailKB: strconv.FormatInt(available, 10),
		}
		if size > 0 {
			facts[FactShmUsedPct] = strconv.FormatFloat(float64(used)*100/float64(size), 'f', 1, 64)
		}
		return facts, nil
	}
	return nil, fmt.Errorf("no /dev/shm usage was reported")
}

var tcpStates = map[string]string{
	"01": "ESTABLISHED", "02": "SYN_SENT", "03": "SYN_RECV", "04": "FIN_WAIT1", "05": "FIN_WAIT2", "06": "TIME_WAIT",
	"07": "CLOSE", "08": "CLOSE_WAIT", "09": "LAST_ACK", "0A": "LISTEN", "0B": "CLOSING",
}

// procAddress decodes a /proc/net/tcp address ("0100007F:1F90").
func procAddress(value string) (string, bool) {
	host, port, ok := strings.Cut(value, ":")
	if !ok {
		return "", false
	}
	raw, err := hex.DecodeString(host)
	if err != nil || len(raw) != 4 && len(raw) != 16 {
		return "", false
	}
	// The kernel prints each 32-bit word in host (little-endian) order.
	for i := 0; i+4 <= len(raw); i += 4 {
		raw[i], raw[i+1], raw[i+2], raw[i+3] = raw[i+3], raw[i+2], raw[i+1], raw[i]
	}
	p, err := strconv.ParseUint(port, 16, 16)
	if err != nil {
		return "", false
	}
	ip := net.IP(raw)
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return net.JoinHostPort(ip.String(), strconv.FormatUint(p, 10)), true
}

func parseSockets(stdout string) (map[string]string, error) {
	states := map[string]int{}
	peers := map[string]map[string]int{}
	total := 0
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		count, err := strconv.Atoi(fields[0])
		state, known := tcpStates[strings.ToUpper(fields[2])]
		peer, ok := procAddress(fields[1])
		if err != nil || !known || !ok {
			continue
		}
		total += count
		states[state] += count
		if state == "LISTEN" {
			continue
		}
		if peers[peer] == nil {
			peers[peer] = map[string]int{}
		}
		peers[peer][state] += count
	}
	facts := map[string]string{FactSockets: strconv.Itoa(total)}
	for state, count := range states {
		facts["state."+state] = strconv.Itoa(count)
	}
	type peerCount struct {
		peer  string
		count int
	}
	var ranked []peerCount
	for peer, byState := range peers {
		n := 0
		for _, count := range byState {
			n += count
		}
		ranked = append(ranked, peerCount{peer, n})
	}
	slices.SortFunc(ranked, func(a, b peerCount) int {
		if a.count != b.count {
			return b.count - a.count
		}
		return strings.Compare(a.peer, b.peer)
	})
	for _, entry := range ranked[:min(len(ranked), MaxPeers)] {
		var parts []string
		for state, count := range peers[entry.peer] {
			parts = append(parts, state+"="+strconv.Itoa(count))
		}
		slices.Sort(parts)
		facts["peer."+entry.peer] = strings.Join(parts, ",")
	}
	return facts, nil
}

// PeerStates returns a socket summary's per-state counts for one remote peer
// ("10.96.0.10:5432").
func (r Reading) PeerStates(peer string) map[string]int {
	out := map[string]int{}
	for _, part := range strings.Split(r.Facts["peer."+peer], ",") {
		state, count, ok := strings.Cut(part, "=")
		if n, err := strconv.Atoi(count); ok && err == nil {
			out[state] = n
		}
	}
	return out
}

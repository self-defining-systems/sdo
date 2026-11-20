package bench

// Core reservation: a host's lane block is declared in hosts.yaml (`reserve:`) and applied by commands through the root
// helper (deploy/host/lane-reserve), never by hand. Admission uses the recorded block: every lane's cpuset is inside it,
// a host with no reservation admits no lanes unless the spec or host says `isolation: one-way-ok`, and a host whose slices
// drifted from the record admits nothing until a human looks.

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Isolation modes of a row.
const (
	IsolationTwoWay  = "two-way"    // the other slices were shrunk to the complement of the block: nobody else uses the lane cores
	IsolationOneWay  = "one-way"    // the lanes stay on their block but other work may still use it
	IsolationOneWayO = "one-way-ok" // no reservation at all; the spec or host accepted that, and its rows are flagged
)

// HelperPath is where the root helper is installed (AliasPath is the old name, kept as a link).
const (
	HelperPath = "/usr/local/sbin/lane-reserve"
	AliasPath  = "/usr/local/sbin/sdo-eval-reserve"
)

// HostReserve is a host's declared lane block (hosts.yaml `reserve:`): explicit cpus, or cores of a NUMA node.
type HostReserve struct {
	CPUs            string `json:"cpus,omitempty"`
	Cores           int    `json:"cores,omitempty"`
	NUMA            *int   `json:"numa,omitempty"`
	Memory          string `json:"memory,omitempty"`
	HostReserveCPUs int    `json:"host_reserve_cpus,omitempty"`
}

func (r *HostReserve) validate() error {
	if r == nil {
		return nil
	}
	switch {
	case r.CPUs != "" && (r.Cores != 0 || r.NUMA != nil):
		return errors.New("reserve: cpus or cores with numa, not both")
	case r.CPUs != "":
		if _, err := ParseCPUList(r.CPUs); err != nil {
			return fmt.Errorf("reserve.cpus: %v", err)
		}
	case r.Cores > 0 && r.NUMA != nil && *r.NUMA >= 0:
	default:
		return errors.New("reserve: cpus LIST, or cores N with numa M")
	}
	if _, err := ParseSize(r.Memory); err != nil {
		return fmt.Errorf("reserve.memory: %v", err)
	}
	if r.HostReserveCPUs < 0 {
		return errors.New("reserve.host_reserve_cpus must not be negative")
	}
	return nil
}

// args are the helper's `block reserve` arguments for the declared block.
func (r HostReserve) args() []string {
	var a []string
	if r.CPUs != "" {
		a = append(a, "--cpus", r.CPUs)
	} else {
		a = append(a, "--cores", strconv.Itoa(r.Cores), "--numa", strconv.Itoa(*r.NUMA))
	}
	if r.Memory != "" {
		a = append(a, "--memory", r.Memory)
	}
	return append(a, "--host-reserve-cpus", strconv.Itoa(r.HostReserveCPUs))
}

// Reservation is what the bench recorded for a host: the block that is active (<root>/reservations/<host>.json).
type Reservation struct {
	Host    string    `json:"host"`
	Block   string    `json:"block"`
	Memory  string    `json:"memory,omitempty"`
	Adopted bool      `json:"adopted,omitempty"`
	TwoWay  bool      `json:"two_way"`
	At      time.Time `json:"at"`
	// Foreign (in memory only) names the slices that hold containers pinned inside the block: the host is one-way now.
	Foreign string `json:"-"`
}

func (r Root) reservationPath(host string) string { return r.path("reservations", host+".json") }

// LoadReservations reads every host's record.
func (r Root) LoadReservations() (map[string]Reservation, error) {
	out := map[string]Reservation{}
	ents, err := os.ReadDir(r.path("reservations"))
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	} else if err != nil {
		return nil, err
	}
	for _, e := range ents {
		var rs Reservation
		if strings.HasSuffix(e.Name(), ".json") && readJSON(r.path("reservations", e.Name()), &rs) == nil {
			out[rs.Host] = rs
		}
	}
	return out, nil
}

// BlockStatus is what the helper reports about a host.
type BlockStatus struct {
	Block  string            // "" : no block
	TwoWay bool              // the other slices are the complement of the block
	Slices map[string]string // slice -> AllowedCPUs
	// Defects are lane slices that hold a process directly (cgroup v2 then refuses the node containers' scopes under
	// them): "lanes-x.slice: reason".
	Defects []string
	// Unexcluded are the top-level slices (other than lanes.slice) whose cpus still reach into the block; Foreign are
	// those that cannot be shrunk because they hold a container pinned inside the block ("slice: cgroup").
	Unexcluded, Foreign []string
}

func parseBlockStatus(out string) BlockStatus {
	st := BlockStatus{Slices: map[string]string{}}
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		switch {
		case len(f) == 2 && f[0] == "block" && f[1] != "-":
			st.Block = f[1]
		case len(f) == 2 && f[0] == "two-way":
			st.TwoWay = f[1] == "yes"
		case len(f) >= 2 && f[0] == "unexcluded":
			st.Unexcluded = append(st.Unexcluded, f[1])
		case len(f) >= 2 && f[0] == "foreign-pinned":
			st.Foreign = append(st.Foreign, f[1])
		case len(f) >= 3 && f[0] == "defect":
			st.Defects = append(st.Defects, f[1]+": "+strings.Join(f[2:], " "))
		case len(f) >= 3 && f[0] == "slice":
			st.Slices[f[1]] = strings.TrimPrefix(f[2], "AllowedCPUs=")
		}
	}
	return st
}

// Helper runs one verb of the root helper on a host (sudo -n on the installed path).
type Helper func(ctx context.Context, host string, args ...string) (string, error)

// HelperVia is the Helper that runs scripts through run (local or ssh).
func HelperVia(run runFn) Helper {
	return func(ctx context.Context, host string, args ...string) (string, error) {
		q := make([]string, len(args))
		for i, a := range args {
			q[i] = shellQuote(a)
		}
		return run(ctx, host, "sudo -n "+HelperPath+" "+strings.Join(q, " "))
	}
}

// LaneSlice is the systemd slice of a lane under lanes.slice (a dash would nest it, so it becomes an underscore).
func LaneSlice(lane string) string { return "lanes-" + strings.ReplaceAll(lane, "-", "_") + ".slice" }

// laneExecPrefix is what every step of a reserved host starts under: the helper puts the step's process (and so every
// build, agent, hook and kubectl it starts) into the lane's cgroup before it runs.
func laneExecPrefix(s Step) string {
	if s.Limits == nil || s.Limits.Block == "" {
		return ""
	}
	return "sudo -n --preserve-env " + HelperPath + " lane-exec " + shellQuote(s.Lane) + " -- "
}

// isolationOf says how a row on host h is isolated: the mode and, for a reserved host, the block; why is non-empty when
// no lane may be admitted on it now.
func isolationOf(h Host, s Spec, rs map[string]Reservation, name string, drift map[string]string) (mode, block, why string) {
	if d := drift[name]; d != "" {
		return "", "", "host " + name + " reservation drifted: " + d
	}
	if r, ok := rs[name]; ok {
		if s.Isolation == IsolationTwoWay && !r.TwoWay {
			if r.Foreign != "" {
				return "", "", "host " + name + " is one-way (foreign pinned inside block in " + r.Foreign + ") and the spec requires two-way"
			}
			return "", "", "host " + name + " is one-way (its other slices still use the block) and the spec requires two-way"
		}
		if r.TwoWay {
			return IsolationTwoWay, r.Block, ""
		}
		return IsolationOneWay, r.Block, ""
	}
	if s.Isolation == IsolationOneWayO || h.Isolation == IsolationOneWayO {
		return IsolationOneWayO, "", ""
	}
	return "", "", "host " + name + " has no active reservation (lanes host reserve or adopt) and neither the spec nor the host says isolation: one-way-ok"
}

// checkReservations rereads the records and compares each host's slices with its record (at most once a minute per host;
// a failed read is drift too: unknown is not admitted).
func (m *Manager) checkReservations(ctx context.Context) {
	rs, err := m.Root.LoadReservations()
	if err != nil {
		m.logf("reservations: %v", err)
	}
	if m.drift == nil {
		m.drift, m.driftAt = map[string]string{}, map[string]time.Time{}
	}
	if m.foreign == nil {
		m.foreign = map[string]string{}
	}
	for name, r := range rs {
		if f := m.foreign[name]; f != "" {
			r.TwoWay, r.Foreign = false, f
			rs[name] = r
		}
	}
	m.reservations = rs
	helper := m.Helper
	if helper == nil {
		helper = HelperVia(hostRunner(m.inv))
	}
	for name, r := range rs {
		if _, ok := m.inv.Hosts[name]; !ok {
			continue
		}
		if time.Since(m.driftAt[name]) < m.driftEvery() && m.driftAt[name] != (time.Time{}) {
			continue
		}
		m.driftAt[name] = time.Now()
		out, err := helper(ctx, name, "block", "status")
		var why string
		if err != nil {
			why = "the helper could not report the slices: " + clip(err.Error(), 160)
		} else {
			st := parseBlockStatus(out)
			rec := r // the record as written: an in-memory one-way override is not what the helper is compared with
			if rec.Foreign != "" {
				rec.TwoWay, rec.Foreign = true, ""
			}
			why = driftOf(rec, st)
			// Only foreign-pinned slices keep the block shared: the host is one-way for now (the allocator already avoids
			// the cores such containers are pinned to), not drifted.
			if why == "" && rec.TwoWay && !st.TwoWay {
				m.foreign[name] = strings.Join(st.Foreign, ", ")
			} else {
				delete(m.foreign, name)
			}
		}
		if why != m.drift[name] {
			if why != "" {
				m.logf("ALARM host %s: reservation drift, admission stopped: %s", name, why)
				_ = appendJSONL(m.Root.path("alarms.jsonl"), map[string]any{"at": time.Now().UTC(), "host": name, "alarm": "reservation-drift", "detail": why})
			} else {
				m.logf("host %s: reservation matches its record again, admission resumes", name)
			}
		}
		m.drift[name] = why
		if f := m.foreign[name]; f != "" {
			r.TwoWay, r.Foreign = false, f
		} else if r.Foreign != "" {
			r.TwoWay, r.Foreign = true, "" // the override is over: the record as written
		}
		m.reservations[name] = r
	}
}

func (m *Manager) driftEvery() time.Duration {
	if m.DriftEvery > 0 {
		return m.DriftEvery
	}
	return time.Minute
}

// driftOf says how the live slices differ from the record ("" : they match).
func driftOf(r Reservation, st BlockStatus) string {
	switch {
	case st.Block != r.Block:
		return fmt.Sprintf("block is %q, the record says %q", st.Block, r.Block)
	case r.TwoWay && !st.TwoWay && len(st.Unexcluded) > 0:
		return "slice " + strings.Join(st.Unexcluded, ", ") + " does not exclude the block"
	case r.TwoWay && !st.TwoWay && len(st.Foreign) == 0:
		return "the other slices no longer exclude the block (AllowedCPUs changed)"
	case st.Slices["lanes.slice"] != r.Block:
		return fmt.Sprintf("lanes.slice AllowedCPUs is %q, not %q", st.Slices["lanes.slice"], r.Block)
	case len(st.Defects) > 0:
		return "lane slice defect: " + strings.Join(st.Defects, "; ")
	}
	return ""
}

// HostCommand is `lanes host reserve|unreserve|status|adopt|install-helper`.
func HostCommand(ctx context.Context, root Root, args []string, out, errw io.Writer, helper Helper, run runFn) int {
	if len(args) == 0 {
		fmt.Fprintln(errw, "usage: lanes host reserve HOST|all | unreserve HOST [--drain] | status [HOST] | adopt HOST [--cpus LIST] [--prev SLICE=LIST]... | install-helper HOST [--sudo-password-file PATH]")
		return 2
	}
	inv, err := LoadInventory(string(root))
	if err != nil {
		fmt.Fprintln(errw, err)
		return 1
	}
	verb, rest := args[0], args[1:]
	fs := flag.NewFlagSet(verb, flag.ContinueOnError)
	fs.SetOutput(errw)
	drain := fs.Bool("drain", false, "unreserve: proceed although lanes are running")
	cpus := fs.String("cpus", "", "adopt: the block that exists")
	var prevs []string
	fs.Func("prev", "adopt: SLICE=LIST, what that slice's AllowedCPUs was before the hand-made reservation (repeatable; default unrestricted)", func(v string) error {
		prevs = append(prevs, v)
		return nil
	})
	pwFile := fs.String("sudo-password-file", "", "install-helper: feed sudo -S from this file instead of asking on the terminal (for tools)")
	var names []string
	for len(rest) > 0 {
		if strings.HasPrefix(rest[0], "-") {
			if err := fs.Parse(rest); err != nil {
				return 2
			}
			rest = fs.Args()
			continue
		}
		names, rest = append(names, rest[0]), rest[1:]
	}
	hosts, err := hostsNamed(inv, verb, names)
	if err != nil {
		fmt.Fprintln(errw, err)
		return 2
	}
	rc := 0
	for _, h := range hosts {
		var err error
		switch verb {
		case "reserve":
			err = reserveHost(ctx, root, inv, h, helper, out)
		case "unreserve":
			err = unreserveHost(ctx, root, h, *drain, helper, out)
		case "adopt":
			err = adoptHost(ctx, root, inv, h, *cpus, prevs, helper, out)
		case "status":
			err = statusHost(ctx, root, inv, h, helper, run, out)
		case "install-helper":
			err = installHelper(ctx, inv.Hosts[h], h, *pwFile, out, errw)
		default:
			fmt.Fprintf(errw, "unknown host verb %q\n", verb)
			return 2
		}
		if err != nil {
			fmt.Fprintf(errw, "%s: %v\n", h, err)
			rc = 1
		}
	}
	return rc
}

func hostsNamed(inv Inventory, verb string, names []string) ([]string, error) {
	var all []string
	for n := range inv.Hosts {
		all = append(all, n)
	}
	sort.Strings(all)
	switch {
	case len(names) == 0 && verb == "status":
		return all, nil
	case len(names) == 1 && names[0] == "all" && verb != "unreserve" && verb != "install-helper":
		if verb == "reserve" {
			var withBlock []string
			for _, n := range all {
				if inv.Hosts[n].Reserve != nil {
					withBlock = append(withBlock, n)
				}
			}
			return withBlock, nil
		}
		return all, nil
	case len(names) == 1:
		if _, ok := inv.Hosts[names[0]]; !ok {
			return nil, fmt.Errorf("host %q is not in hosts.yaml", names[0])
		}
		return names, nil
	}
	return nil, fmt.Errorf("name one host (or all)")
}

func recordReservation(ctx context.Context, root Root, host string, helper Helper, adopted bool, memory string) (Reservation, error) {
	o, err := helper(ctx, host, "block", "status")
	if err != nil {
		return Reservation{}, err
	}
	st := parseBlockStatus(o)
	if st.Block == "" {
		return Reservation{}, errors.New("the helper reports no block after the change")
	}
	r := Reservation{Host: host, Block: st.Block, Memory: memory, Adopted: adopted, TwoWay: st.TwoWay, At: time.Now().UTC()}
	return r, writeJSON(root.reservationPath(host), r)
}

func reserveHost(ctx context.Context, root Root, inv Inventory, host string, helper Helper, out io.Writer) error {
	rsv := inv.Hosts[host].Reserve
	if rsv == nil {
		return errors.New("no reserve: block in hosts.yaml")
	}
	o, err := helper(ctx, host, append([]string{"block", "reserve"}, rsv.args()...)...)
	fmt.Fprint(out, host+":\n"+o)
	if err != nil {
		return err
	}
	r, err := recordReservation(ctx, root, host, helper, false, rsv.Memory)
	if err == nil {
		fmt.Fprintf(out, "%s: reserved %s (%s)\n", host, r.Block, modeName(r.TwoWay))
	}
	return err
}

func modeName(twoWay bool) string {
	if twoWay {
		return IsolationTwoWay
	}
	return IsolationOneWay
}

// busyLanes are the lanes on host that run or build something.
func busyLanes(root Root, host string) []string {
	lanes, _ := root.Lanes()
	var busy []string
	for _, l := range lanes {
		if l.Host == host && (l.Step != nil || l.State == LaneBuilding || l.State == LaneRunning) {
			busy = append(busy, l.Lane)
		}
	}
	return busy
}

func unreserveHost(ctx context.Context, root Root, host string, drain bool, helper Helper, out io.Writer) error {
	if busy := busyLanes(root, host); len(busy) > 0 && !drain {
		return fmt.Errorf("lanes are running (%s); wait, or pass --drain to unreserve anyway (they keep running unreserved and the host admits nothing new)", strings.Join(busy, ", "))
	}
	o, err := helper(ctx, host, "block", "unreserve")
	fmt.Fprint(out, host+":\n"+o)
	if err != nil {
		return err
	}
	if err := os.Remove(root.reservationPath(host)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	fmt.Fprintf(out, "%s: unreserved\n", host)
	return nil
}

func adoptHost(ctx context.Context, root Root, inv Inventory, host, cpus string, prevs []string, helper Helper, out io.Writer) error {
	if cpus == "" {
		if r := inv.Hosts[host].Reserve; r != nil {
			cpus = r.CPUs
		}
	}
	if cpus == "" {
		return errors.New("adopt needs --cpus LIST (or reserve.cpus in hosts.yaml)")
	}
	args := []string{"block", "adopt", "--cpus", cpus}
	for _, p := range prevs {
		if s, _, ok := strings.Cut(p, "="); !ok || s == "" {
			return fmt.Errorf("--prev %q: want SLICE=LIST (LIST may be empty for unrestricted)", p)
		}
		args = append(args, "--prev", p)
	}
	if o, err := helper(ctx, host, args...); err != nil {
		return err
	} else {
		fmt.Fprint(out, o)
	}
	r, err := recordReservation(ctx, root, host, helper, true, "")
	if err == nil {
		fmt.Fprintf(out, "%s: adopted %s (%s), nothing changed\n", host, r.Block, modeName(r.TwoWay))
	}
	return err
}

// containerProbe lists the host's containers as `NAME CGROUPPARENT CPUSET`.
const containerProbe = `docker ps --format '{{.Names}}' 2>/dev/null | while read -r c; do ` +
	`echo "$c $(docker inspect -f '{{.HostConfig.CgroupParent}}' "$c") $(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$c")"; done`

// ContainerPin is one running container with a cpuset, as containerProbe reports it.
type ContainerPin struct {
	Name   string
	Parent string // the cgroup parent
	Set    string // the cpuset as docker prints it
	CPUs   []int
}

// ours reports whether the container is one of our lanes or the eval slice (everything else is foreign).
func (c ContainerPin) ours() bool {
	return strings.HasPrefix(c.Parent, "lanes") || c.Parent == EvalSlice
}

// parseContainerProbe reads containerProbe output; containers without a cpuset pin are left out.
func parseContainerProbe(out string) []ContainerPin {
	var pins []ContainerPin
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(l)
		if len(f) < 3 {
			continue
		}
		set, err := ParseCPUList(f[2])
		if err != nil || len(set) == 0 {
			continue
		}
		pins = append(pins, ContainerPin{Name: f[0], Parent: f[1], Set: f[2], CPUs: set})
	}
	return pins
}

// ForeignPins lists the containers in containerProbe output that are not ours and are pinned to some cpus.
func ForeignPins(out string) []ContainerPin {
	var foreign []ContainerPin
	for _, c := range parseContainerProbe(out) {
		if !c.ours() {
			foreign = append(foreign, c)
		}
	}
	return foreign
}

func statusHost(ctx context.Context, root Root, inv Inventory, host string, helper Helper, run runFn, out io.Writer) error {
	rs, _ := root.LoadReservations()
	r, recorded := rs[host]
	o, herr := helper(ctx, host, "block", "status")
	st := parseBlockStatus(o)
	switch {
	case herr != nil:
		fmt.Fprintf(out, "%s: helper unavailable (%s); record: %s\n", host, clip(herr.Error(), 120), recordLine(r, recorded))
		return nil
	case st.Block == "":
		fmt.Fprintf(out, "%s: no block (record: %s); one-way-ok only\n", host, recordLine(r, recorded))
		return nil
	}
	fmt.Fprintf(out, "%s: block %s, %s", host, st.Block, modeName(st.TwoWay))
	if recorded {
		if d := driftOf(r, st); d != "" {
			fmt.Fprintf(out, ", DRIFT: %s", d)
		}
	} else {
		fmt.Fprint(out, ", no record (lanes host adopt)")
	}
	fmt.Fprintln(out)
	for _, d := range st.Defects {
		fmt.Fprintf(out, "  DEFECT lane slice %s\n", d)
	}
	block, _ := ParseCPUList(st.Block)
	in := map[int]bool{}
	for _, c := range block {
		in[c] = true
	}
	cs, err := run(ctx, host, containerProbe)
	if err != nil {
		fmt.Fprintf(out, "  containers: probe failed: %s\n", clip(err.Error(), 120))
		return nil
	}
	for _, c := range parseContainerProbe(cs) {
		inside := 0
		for _, cpu := range c.CPUs {
			if in[cpu] {
				inside++
			}
		}
		switch {
		case c.ours() && inside < len(c.CPUs):
			fmt.Fprintf(out, "  our container %s is pinned to %s, outside the block\n", c.Name, c.Set)
		case !c.ours() && inside > 0:
			fmt.Fprintf(out, "  foreign container %s is pinned to %s, inside the block\n", c.Name, c.Set)
		}
	}
	return nil
}

func recordLine(r Reservation, ok bool) string {
	if !ok {
		return "none"
	}
	return fmt.Sprintf("%s %s", r.Block, modeName(r.TwoWay))
}

// helperFiles are what install-helper puts on a host: the helper, its alias, the slice units and the sudoers rule.
var helperFiles = []string{"lane-reserve", "lanes.slice", "eval.slice", "sdo-eval-reserve-gc.service", "sdo-eval-reserve-gc.timer"}

// HelperSourceDir is where install-helper reads deploy/host from (a var for tests).
var HelperSourceDir = filepath.Join("deploy", "host")

// installScript is the one script that runs as root: it carries the files itself, so it works over ssh. It runs under
// `sudo bash` (see installHelper), so the sudo inside it only reuses root.
func installScript(dir, user string, read func(string) ([]byte, error)) (string, error) {
	var b strings.Builder
	b.WriteString("set -e\nT=$(mktemp -d); trap 'rm -rf \"$T\"' EXIT\n")
	for _, f := range helperFiles {
		data, err := read(filepath.Join(dir, f))
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "echo %s | base64 -d > \"$T/%s\"\n", base64.StdEncoding.EncodeToString(data), f)
	}
	rule := "# lane-reserve: the one command the lane bench runs as root (installed by `lanes host install-helper`).\n" +
		user + " ALL=(root) NOPASSWD:SETENV: " + HelperPath + ", " + AliasPath + "\n"
	fmt.Fprintf(&b, "echo %s | base64 -d > \"$T/sudoers\"\n", base64.StdEncoding.EncodeToString([]byte(rule)))
	b.WriteString(`sudo visudo -cf "$T/sudoers" >/dev/null
sudo install -m 0755 "$T/lane-reserve" ` + HelperPath + `
sudo ln -sf lane-reserve ` + AliasPath + `
sudo install -m 0644 "$T/lanes.slice" "$T/eval.slice" "$T/sdo-eval-reserve-gc.service" "$T/sdo-eval-reserve-gc.timer" /etc/systemd/system/
sudo install -m 0440 "$T/sudoers" /etc/sudoers.d/lane-reserve
sudo systemctl daemon-reload
sudo systemctl enable --now sdo-eval-reserve-gc.timer
sudo systemctl start eval.slice lanes.slice
echo installed ` + HelperPath + `
`)
	return b.String(), nil
}

// installHelper runs the install script as root on the host. The script is copied to a temporary file first (never the
// remote shell's stdin), then run as `sudo bash FILE`: interactively over `ssh -t`, so a human types the password once, or
// with a password file fed to `sudo -S` on stdin for tools. Nothing is retried; the temporary file is always removed.
func installHelper(ctx context.Context, h Host, name, pwFile string, out, errw io.Writer) error {
	user := os.Getenv("USER")
	if user == "" {
		user = "$(id -un)"
	}
	script, err := installScript(HelperSourceDir, user, os.ReadFile)
	if err != nil {
		return err
	}
	var pw []byte
	if pwFile != "" {
		if pw, err = os.ReadFile(pwFile); err != nil {
			return err
		}
	}
	sh := sshHost{name: name, h: h}
	// local: a temp file here; remote: one on the host, written by a plain non-sudo ssh whose stdin is the script.
	var path string
	if h.Local {
		f, err := os.CreateTemp("", "lane-install-*.sh")
		if err != nil {
			return err
		}
		path = f.Name()
		_, werr := f.WriteString(script)
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return werr
		}
	} else {
		b, err := sh.run(ctx, []byte(script), `f=$(mktemp /tmp/lane-install.XXXXXX) && cat > "$f" && echo "$f"`)
		if err != nil {
			return err
		}
		if path = strings.TrimSpace(string(b)); path == "" {
			return errors.New("the host returned no temporary file name")
		}
	}
	defer func() {
		if h.Local {
			os.Remove(path)
		} else {
			_, _ = sh.run(context.Background(), nil, "rm -f "+shellQuote(path))
		}
	}()
	sudo := "sudo bash " + shellQuote(path)
	if pwFile != "" {
		sudo = `sudo -S -p "" bash ` + shellQuote(path)
	}
	ictx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var cmd *exec.Cmd
	if h.Local {
		cmd = exec.CommandContext(ictx, "sh", "-c", sudo)
	} else {
		f := strings.Fields(sh.sshForRsync())
		args := append([]string{}, f[1:]...)
		if pwFile == "" {
			args = append(args, "-t")
		}
		cmd = exec.CommandContext(ictx, f[0], append(args, h.SSH, sudo)...)
	}
	if pwFile != "" {
		cmd.Stdin = bytes.NewReader(pw)
	} else {
		cmd.Stdin = os.Stdin
	}
	cmd.Stdout, cmd.Stderr = out, errw
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("install on %s: %v", name, err)
	}
	return nil
}

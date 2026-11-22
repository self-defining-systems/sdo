package bench

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Preflight is `fuzzops-bench preflight`: a few minutes of checks, before any lane is built, for the failures
// that otherwise show up only after a 30-minute lane build (docs/bench.md, "Preflight"). It prints one
// PASS/FAIL line per check per host and never reads a Secret or credential value: the credential check only
// runs a call with the credential file mounted.

// Per-lane needs used by the capacity check (an SDO GitLab lane holds about 12-16 GB and 40 GB).
const (
	laneMemBytes  = 16 << 30
	laneDiskBytes = 40 << 30
)

// PreflightOptions are the flags of the verb.
type PreflightOptions struct {
	// MaxLoadPerCore is the highest accepted load5 per core (the usual load gate of a lane is 1.0).
	MaxLoadPerCore float64
	Timeout        time.Duration
	// AllowMemoryDiag lists substrings of memory-check diagnostics that do not fail the memory check (for example a
	// telemetry source the lane binds at install).
	AllowMemoryDiag []string
}

// PreflightResult is one line of the report.
type PreflightResult struct {
	Check, Host, Detail string
	OK                  bool
	order               int
}

func (r PreflightResult) String() string {
	v := "PASS"
	if !r.OK {
		v = "FAIL"
	}
	return fmt.Sprintf("%s  %-9s %-28s %s", v, r.Host, r.Check, r.Detail)
}

// runFn runs a shell script on a host (locally or over ssh) and returns its combined stdout.
type runFn func(ctx context.Context, host, script string) (string, error)

// Preflight loads the specs (files, or directories of them), runs every check and writes the report to w.
// It returns an error when any check failed.
func Preflight(ctx context.Context, root Root, specPaths []string, opt PreflightOptions, w io.Writer) error {
	if opt.MaxLoadPerCore <= 0 {
		opt.MaxLoadPerCore = 1.0
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 5 * time.Minute
	}
	inv, err := LoadInventory(string(root))
	if err != nil {
		return err
	}
	specs, err := loadPreflightSpecs(root, specPaths)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, opt.Timeout)
	defer cancel()
	results := runPreflight(ctx, inv, specs, opt, hostRunner(inv))
	bad := 0
	for _, r := range results {
		fmt.Fprintln(w, r)
		if !r.OK {
			bad++
		}
	}
	if bad > 0 {
		return fmt.Errorf("preflight: %d of %d checks FAILED", bad, len(results))
	}
	fmt.Fprintf(w, "preflight: all %d checks passed\n", len(results))
	return nil
}

// loadPreflightSpecs reads each path (a spec file, or a directory of *.yaml/*.yml specs); none given: <root>/specs.
func loadPreflightSpecs(root Root, paths []string) ([]specFile, error) {
	if len(paths) == 0 {
		paths = []string{filepath.Join(string(root), "specs")}
	}
	var files []string
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !st.IsDir() {
			files = append(files, p)
			continue
		}
		for _, pat := range []string{"*.yaml", "*.yml"} {
			m, _ := filepath.Glob(filepath.Join(p, pat))
			files = append(files, m...)
		}
	}
	sort.Strings(files)
	var out []specFile
	for _, f := range files {
		s, err := LoadSpec(f)
		if err != nil {
			return nil, err
		}
		out = append(out, specFile{s, filepath.Dir(f)})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("preflight: no spec found in %v", paths)
	}
	return out, nil
}

type specFile struct {
	Spec
	dir string // relative paths in the spec resolve here
}

func (s specFile) baselineDir() string {
	b := s.Operator.Baseline
	if b == "" || filepath.IsAbs(b) {
		return b
	}
	return filepath.Join(s.dir, b)
}

// hostRunner runs scripts on the local machine or over the host's ssh.
func HostRunner(inv Inventory) runFn { return hostRunner(inv) }

func hostRunner(inv Inventory) runFn {
	return func(ctx context.Context, host, script string) (string, error) {
		h := inv.Hosts[host]
		if h.Local {
			cmd := exec.CommandContext(ctx, "sh", "-c", script)
			var out, errb bytes.Buffer
			cmd.Stdout, cmd.Stderr = &out, &errb
			if err := cmd.Run(); err != nil {
				return out.String(), fmt.Errorf("%v: %s", err, tail(errb.String(), 200))
			}
			return out.String(), nil
		}
		out, err := sshHost{name: host, h: h}.run(ctx, nil, script)
		return string(out), err
	}
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func tail(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}

// envPrefix is the shell preamble of every script on h: the host's step environment, and the usual tool paths.
func envPrefix(h Host) string {
	var b strings.Builder
	for _, kv := range h.EnvList() {
		k, v, _ := strings.Cut(kv, "=")
		fmt.Fprintf(&b, "export %s=%s; ", k, shellQuote(v))
	}
	b.WriteString(`export PATH="$PATH:$HOME/.local/go/bin:$HOME/.local/bin:/usr/local/go/bin"; `)
	return b.String()
}

// runPreflight runs all checks, hosts in parallel, and returns the results sorted by check then host.
func runPreflight(ctx context.Context, inv Inventory, specs []specFile, opt PreflightOptions, run runFn) []PreflightResult {
	subj := subjectChecks(newHookSubject())
	var mu sync.Mutex
	var results []PreflightResult
	add := func(order int, check, host string, ok bool, f string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		results = append(results, PreflightResult{Check: check, Host: host, OK: ok, Detail: fmt.Sprintf(f, a...), order: order})
	}

	// Which specs run on which hosts.
	onHost := map[string][]specFile{}
	for _, s := range specs {
		usable := map[string]bool{}
		for _, h := range inv.UsableHosts(s.Spec) {
			usable[h] = true
			onHost[h] = append(onHost[h], s)
		}
		for _, h := range s.Hosts {
			if !usable[h] {
				add(0, "pins["+s.Name+"]", h, false, "host is not in hosts.yaml or has no checkout of fuzzops %s%s", s.Fuzzops, hooksNote(s.Spec))
			}
		}
	}
	var hosts []string
	for h := range onHost {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)

	// Raw facts per host, gathered in parallel; comparisons across hosts follow.
	var wg sync.WaitGroup
	imageIDs := map[string]map[string]string{} // host -> image -> id ("" missing)
	kindImage := map[string]string{}
	var facts sync.Mutex
	for _, h := range hosts {
		h := h
		wg.Add(1)
		go func() {
			defer wg.Done()
			hs := onHost[h]
			host := inv.Hosts[h]
			pre := envPrefix(host)

			// 9 tools resolve where the steps run: in exactly the environment of a step of each workload (steptools.go)
			workloads := map[string]bool{}
			for _, s := range hs {
				workloads[s.Workload] = true
			}
			if len(workloads) == 0 {
				workloads[""] = true
			}
			var names []string
			for w := range workloads {
				names = append(names, w)
			}
			wok, wd := judgeWorkloadEnv(host, names)
			add(9, "workload-env", h, wok, "%s", wd)
			for w := range workloads {
				tout, terr := run(ctx, h, toolsProbe(host, w))
				tok, td := judgeTools(tout, terr)
				label := "tools"
				if w != "" {
					label += "[" + w + "]"
				}
				add(9, label, h, tok, "%s", td)
			}

			// 7 capacity
			planned := plannedLanes(inv, h, hs)
			var out string
			var err error
			if host.IsEks() {
				out, err = run(ctx, h, pre+eksAccountScript())
				ok, d := judgeEksAccount(out, err, host.EKS)
				add(7, "eks-account", h, ok, "%s", d)
			} else if host.IsVcluster() {
				out, err = run(ctx, h, pre+sharedClusterScript(host))
				ok, d := judgeSharedCluster(out, err, planned)
				add(7, "shared-cluster", h, ok, "%s", d)
			} else if out, err = run(ctx, h, pre+capacityScript(host)); err != nil {
				add(7, "capacity", h, false, "probe failed: %v", err)
			} else {
				c, perr := parseCapacity(out)
				if perr != nil {
					add(7, "capacity", h, false, "%v", perr)
				} else {
					up := 0
					for _, cl := range host.Clusters {
						if c.Running[cl+"-control-plane"] {
							up++
						}
					}
					ok, d := c.judge(max(planned-up, 0), opt.MaxLoadPerCore)
					add(7, "capacity", h, ok, "%s", d)
				}
			}

			// 4 + 5 kind node image and images
			need := requiredImages(host, hs, subj)
			out, err = run(ctx, h, pre+imagesScript(need))
			if err != nil {
				add(5, "images", h, false, "probe failed: %v", err)
			} else {
				ids, kv := parseImages(out)
				facts.Lock()
				imageIDs[h] = ids
				if !host.IsVcluster() && !host.IsEks() {
					kindImage[h] = effectiveKindImage(host, kv)
				}
				facts.Unlock()
			}

			// 1-3 the subject: credentials, sink, proto binding (its own preflight verb, or the SDO default)
			subj.Host(ctx, inv, h, hs, pre, run, add)

			// 8 pack identity
			seenPack := map[string]bool{}
			for _, s := range hs {
				fz := inv.FuzzopsPath(s.Fuzzops, h)
				key := fz + "|" + s.Fuzzops + "|" + s.App
				if fz == "" || seenPack[key] {
					continue
				}
				seenPack[key] = true
				out, err := run(ctx, h, pre+packScript(fz, s.Fuzzops, s.App))
				if err != nil {
					add(8, "pack["+s.App+"@"+s.Fuzzops+"]", h, false, "probe failed: %v", err)
					continue
				}
				ok, d := comparePack(out, s.Fuzzops)
				add(8, "pack["+s.App+"@"+s.Fuzzops+"]", h, ok, "%s", d)
			}
		}()
	}

	// 6 the subject on the manager's own host (the SDO default: memory, where the baselines are).
	subj.Local(ctx, inv, specs, opt, run, add, &wg)
	wg.Wait()

	// Cross-host comparisons.
	for _, r := range compareImages(imageIDs) {
		add(5, "images", r.host, r.ok, "%s", r.detail)
	}
	for _, r := range compareKind(kindImage) {
		r = preloadNote(inv.Hosts[r.host], r)
		add(4, "kind-node-image", r.host, r.ok, "%s", r.detail)
	}

	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.order != b.order {
			return a.order < b.order
		}
		if a.Check != b.Check {
			return a.Check < b.Check
		}
		return a.Host < b.Host
	})
	return results
}

func hooksNote(s Spec) string {
	if s.Operator.Hooks == "" {
		return ""
	}
	return " and hooks " + s.Operator.Hooks + " " + s.Operator.Revision
}

func anyBaseline(specs []specFile) bool {
	for _, s := range specs {
		if s.Operator.Baseline != "" {
			return true
		}
	}
	return false
}

func inDir(dir, snippet string) string {
	return "cd " + shellQuote(dir) + " && uv run -q python - <<'PREFLIGHT_PY'\n" + snippet + "\nPREFLIGHT_PY\n"
}

func short(s string) string {
	if len(s) > 9 {
		return s[:9]
	}
	return s
}

// ---- 7 capacity ---------------------------------------------------------------------------------------------

// plannedLanes is how many lanes the specs may use on host: the sum of their lanes, at most the host's clusters.
func plannedLanes(inv Inventory, host string, specs []specFile) int {
	n := 0
	for _, s := range specs {
		n += s.Lanes
	}
	if c := len(inv.Hosts[host].Clusters); n > c {
		n = c
	}
	return n
}

// sharedClusterScript lists the shared cluster nodes ("node NAME Ready|NotReady"); the kubeconfig may be readable only
// through its group (copied to a temporary file with sg).
func sharedClusterScript(h Host) string {
	return fmt.Sprintf(`K=%s; g=${VCLUSTER_HOST_GROUP:-docker}; T=; [ -r "$K" ] || { T=$(mktemp); sg "$g" -c "cat '$K'" >"$T" 2>/dev/null; K=$T; }; `+
		`kubectl --kubeconfig "$K" get nodes -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.conditions[?(@.type=="Ready")]}{.status}{end}{"\n"}{end}' 2>/dev/null | awk 'NF==2{print "node", $1, ($2=="True"?"Ready":"NotReady")}'; [ -z "$T" ] || rm -f "$T"; true`, shellQuote(h.HostKubeconfig))
}

// judgeSharedCluster: the probe saw nodes, and enough of them are Ready (one per planned lane is not needed:
// lanes share the nodes; the scheduler keeps off NotReady ones).
func judgeSharedCluster(out string, err error, planned int) (bool, string) {
	if err != nil {
		return false, fmt.Sprintf("probe failed: %v", err)
	}
	var ready, notReady []string
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		if len(f) == 3 && f[0] == "node" {
			if f[2] == "Ready" {
				ready = append(ready, f[1])
			} else {
				notReady = append(notReady, f[1])
			}
		}
	}
	if len(ready) == 0 {
		return false, "shared cluster has no Ready node (or its kubeconfig is unreadable)"
	}
	d := fmt.Sprintf("%d Ready node(s) for %d lane(s): %s", len(ready), planned, strings.Join(ready, ","))
	if len(notReady) > 0 {
		d += "; NotReady (lanes avoid them): " + strings.Join(notReady, ",")
	}
	return true, d
}

// platformTools are the external programs the steps, hooks and probes run; a kind host also needs kind.
func platformTools(h Host) []string {
	if h.IsEks() {
		t := []string{"kubectl", "docker", "aws", "eksctl"} // no kernel to probe and no kind
		if h.IsEksVirtual() {
			// vcluster lanes: host-up installs with helm, up/down run the vcluster CLI, the host lock is a flock, the
			// NodePort rule asks checkip with curl: a host without them fails its first `up` half-made
			t = append(t, "helm", "vcluster", "flock", "curl")
		}
		return t
	}
	t := []string{"kubectl", "docker", "ip"}
	if !h.IsVcluster() {
		t = append(t, "kind")
	}
	return t
}

// toolsScript prints "missing TOOL" for every platform tool that does not resolve in the environment the steps run in
// (the caller prefixes the host's envPrefix).
func toolsScript(h Host) string {
	return `for t in ` + strings.Join(platformTools(h), " ") + `; do command -v "$t" >/dev/null 2>&1 || echo "missing $t"; done; true`
}

func judgeTools(out string, err error) (bool, string) {
	if err != nil {
		return false, fmt.Sprintf("probe failed: %v", err)
	}
	var missing []string
	for _, l := range strings.Split(out, "\n") {
		if t, ok := strings.CutPrefix(strings.TrimSpace(l), "missing "); ok {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		return false, "not found on PATH: " + strings.Join(missing, ", ")
	}
	return true, "all platform tools found"
}

func capacityScript(h Host) string {
	st := shellQuote(h.Staging)
	return fmt.Sprintf(`mkdir -p %[1]s; echo "load5 $(cut -d' ' -f2 /proc/loadavg)"; echo "cores $(nproc)"; `+
		`echo "memavail_kb $(awk '/MemAvailable/{print $2}' /proc/meminfo)"; `+
		`echo "disk_staging_kb $(df -Pk %[1]s | tail -n 1 | awk '{print $4}')"; `+
		`d=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null); [ -n "$d" ] && [ -d "$d" ] && echo "disk_docker_kb $(df -Pk "$d" 2>/dev/null | tail -n 1 | awk '{print $4}')"; `+
		`for n in $(docker ps --format '{{.Names}}' 2>/dev/null); do echo "container $n"; done; true`, st)
}

// capacity is what a host reports.
type capacity struct {
	Load5, Cores            float64
	MemAvail                int64
	DiskStaging, DiskDocker int64 // free bytes; DiskDocker -1 when unknown
	Running                 map[string]bool
}

func parseCapacity(out string) (capacity, error) {
	c := capacity{DiskDocker: -1, Running: map[string]bool{}}
	have := map[string]bool{}
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		if len(f) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(f[1], 64)
		switch f[0] {
		case "container":
			c.Running[f[1]] = true
			continue
		}
		if err != nil {
			continue
		}
		have[f[0]] = true
		switch f[0] {
		case "load5":
			c.Load5 = v
		case "cores":
			c.Cores = v
		case "memavail_kb":
			c.MemAvail = int64(v) * 1024
		case "disk_staging_kb":
			c.DiskStaging = int64(v) * 1024
		case "disk_docker_kb":
			c.DiskDocker = int64(v) * 1024
		}
	}
	for _, k := range []string{"load5", "cores", "memavail_kb", "disk_staging_kb"} {
		if !have[k] {
			return c, fmt.Errorf("capacity probe gave no %s: %q", k, tail(out, 120))
		}
	}
	if c.Cores < 1 {
		return c, fmt.Errorf("capacity probe: %v cores", c.Cores)
	}
	return c, nil
}

// judge compares the host with what planned lanes need (lanes already running hold their resources already,
// so only the lanes not yet up count: pass the number still to build).
func (c capacity) judge(toBuild int, maxLoad float64) (bool, string) {
	perCore := c.Load5 / c.Cores
	needMem, needDisk := int64(toBuild)*laneMemBytes, int64(toBuild)*laneDiskBytes
	disk := c.DiskStaging
	if c.DiskDocker >= 0 && c.DiskDocker < disk {
		disk = c.DiskDocker
	}
	var bad []string
	if perCore > maxLoad {
		bad = append(bad, fmt.Sprintf("load5 per core %.2f > %.2f", perCore, maxLoad))
	}
	if c.MemAvail < needMem {
		bad = append(bad, fmt.Sprintf("MemAvailable %s < %s needed", gb(c.MemAvail), gb(needMem)))
	}
	if disk < needDisk {
		bad = append(bad, fmt.Sprintf("free disk %s < %s needed", gb(disk), gb(needDisk)))
	}
	d := fmt.Sprintf("load5 %.1f / %.0f cores = %.2f, MemAvailable %s, free disk %s, lanes to build %d (needs %s mem, %s disk)",
		c.Load5, c.Cores, perCore, gb(c.MemAvail), gb(disk), toBuild, gb(needMem), gb(needDisk))
	if len(bad) > 0 {
		return false, strings.Join(bad, "; ") + " [" + d + "]"
	}
	return true, d
}

func gb(b int64) string { return fmt.Sprintf("%.0f GB", float64(b)/(1<<30)) }

// ---- 4 kind node image, 5 images ------------------------------------------------------------------------------

// requiredImages are the images every spec needs on host: the kind node image and, for an SDO operator, the
// controller, responder and script-tools images of its tag.
func requiredImages(h Host, specs []specFile, subj subjectChecks) []string {
	set := map[string]bool{}
	if k := h.Env["KIND_NODE_IMAGE"]; k != "" {
		set[k] = true
	}
	for _, i := range subj.Images(specs) {
		set[i] = true
	}
	var out []string
	for i := range set {
		out = append(out, i)
	}
	sort.Strings(out)
	return out
}

func short8(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func settingString(m map[string]any, key, def string) string {
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

func imagesScript(images []string) string {
	var b strings.Builder
	// The identity is a hash of the layers and the config: it is the same on hosts whose docker stores images
	// differently (a plain image id is not).
	for _, i := range images {
		fmt.Fprintf(&b, "echo \"image %[1]s $(docker image inspect --format '{{json .RootFS.Layers}}{{json .Config}}' %[1]s 2>/dev/null | sha256sum | cut -c1-16 | grep -v '^e3b0c442' || echo MISSING)\"; ", shellQuote(i))
	}
	b.WriteString(`echo "kind $(kind version 2>/dev/null | head -n 1)"; true`)
	return b.String()
}

// parseImages reads "image NAME ID" lines and the "kind VERSION" line.
func parseImages(out string) (map[string]string, string) {
	ids := map[string]string{}
	kind := ""
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		switch {
		case len(f) == 3 && f[0] == "image":
			ids[f[1]] = f[2]
		case len(f) >= 1 && f[0] == "kind":
			kind = strings.Join(f[1:], " ")
		}
	}
	return ids, kind
}

// effectiveKindImage is the node image a lane on the host gets: KIND_NODE_IMAGE, else kind's own default,
// which depends on the host's kind version.
func effectiveKindImage(h Host, kindVersion string) string {
	if k := h.Env["KIND_NODE_IMAGE"]; k != "" {
		return k
	}
	return "kind-default (" + kindVersion + ")"
}

// preloadNote: a host with KIND_NODE_PRELOAD starts its lanes' workers with the pack's images in containerd
// (deploy/node-preload.sh: a cloned containerd directory mounted at /var/lib/containerd). The node image is still
// KIND_NODE_IMAGE, so the cross-host comparison is on it alone; a preload without a pinned base is a failure
// because the template would be keyed to kind's moving default.
func preloadNote(h Host, v hostVerdict) hostVerdict {
	pre := h.Env["KIND_NODE_PRELOAD"]
	if pre == "" {
		return v
	}
	if h.Env["KIND_NODE_IMAGE"] == "" {
		return hostVerdict{v.host, "KIND_NODE_PRELOAD=" + pre + " needs KIND_NODE_IMAGE (the base the preloaded images are keyed to)", false}
	}
	v.detail += " (workers start with the images of " + pre + " preloaded; the node image is unchanged)"
	return v
}

type hostVerdict struct {
	host, detail string
	ok           bool
}

// compareKind: every host's lanes use the same node image.
func compareKind(byHost map[string]string) []hostVerdict {
	var hosts []string
	distinct := map[string]bool{}
	for h, i := range byHost {
		hosts = append(hosts, h)
		distinct[i] = true
	}
	sort.Strings(hosts)
	var out []hostVerdict
	for _, h := range hosts {
		if len(distinct) == 1 {
			out = append(out, hostVerdict{h, byHost[h], true})
			continue
		}
		var all []string
		for _, o := range hosts {
			all = append(all, o+"="+byHost[o])
		}
		out = append(out, hostVerdict{h, "node images differ between hosts: " + strings.Join(all, ", "), false})
	}
	return out
}

// compareImages: every required image is on its host, with the same id as on every other host that has it.
func compareImages(byHost map[string]map[string]string) []hostVerdict {
	var hosts []string
	for h := range byHost {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	ids := map[string]map[string]bool{} // image -> distinct ids over hosts that have it
	for _, h := range hosts {
		for img, id := range byHost[h] {
			if id == "MISSING" {
				continue
			}
			if ids[img] == nil {
				ids[img] = map[string]bool{}
			}
			ids[img][id] = true
		}
	}
	var out []hostVerdict
	for _, h := range hosts {
		var bad []string
		var imgs []string
		for img := range byHost[h] {
			imgs = append(imgs, img)
		}
		sort.Strings(imgs)
		for _, img := range imgs {
			switch id := byHost[h][img]; {
			case id == "MISSING":
				bad = append(bad, img+" missing")
			case len(ids[img]) > 1:
				bad = append(bad, fmt.Sprintf("%s id %s differs between hosts", img, short(strings.TrimPrefix(id, "sha256:"))))
			}
		}
		if len(bad) > 0 {
			out = append(out, hostVerdict{h, strings.Join(bad, "; "), false})
		} else {
			out = append(out, hostVerdict{h, fmt.Sprintf("%d images present, ids equal on every host", len(imgs)), true})
		}
	}
	return out
}

// ---- 8 pack identity --------------------------------------------------------------------------------------------

// packScript prints the pack directory as committed at rev and as it is on disk (blob id and path per file).
func packScript(fuzzopsDir, rev, app string) string {
	return fmt.Sprintf(`cd %[1]s || exit 3; `+
		`git rev-parse --verify -q %[2]s'^{commit}' >/dev/null || { echo NOCOMMIT; exit 0; }; `+
		`echo "HEAD $(git rev-parse HEAD)"; echo COMMITTED; git ls-tree -r %[2]s -- %[3]s | awk '{print $3, $4}'; `+
		`echo WORKING; find %[3]s -type f -not -path '*/.git/*' | sort | while IFS= read -r f; do echo "$(git hash-object -- "$f") $f"; done`,
		shellQuote(fuzzopsDir), shellQuote(rev), shellQuote(app))
}

// comparePack compares the on-disk pack with the one committed at the spec's revision.
func comparePack(out, rev string) (bool, string) {
	if strings.Contains(out, "NOCOMMIT") {
		return false, "checkout does not have commit " + rev
	}
	head := ""
	committed, working := map[string]string{}, map[string]string{}
	sec := ""
	for _, l := range strings.Split(out, "\n") {
		l = strings.TrimSpace(l)
		switch {
		case l == "COMMITTED" || l == "WORKING":
			sec = l
		case strings.HasPrefix(l, "HEAD "):
			head = strings.TrimPrefix(l, "HEAD ")
		case l != "" && sec != "":
			sha, path, ok := strings.Cut(l, " ")
			if !ok {
				continue
			}
			if sec == "COMMITTED" {
				committed[path] = sha
			} else {
				working[path] = sha
			}
		}
	}
	if len(committed) == 0 {
		return false, "no pack files at commit " + rev
	}
	var diff []string
	for p, s := range committed {
		switch w, ok := working[p]; {
		case !ok:
			diff = append(diff, "missing "+p)
		case w != s:
			diff = append(diff, "modified "+p)
		}
	}
	for p := range working {
		if _, ok := committed[p]; !ok {
			diff = append(diff, "extra "+p)
		}
	}
	sort.Strings(diff)
	if len(diff) > 0 {
		if len(diff) > 3 {
			diff = append(diff[:3], fmt.Sprintf("+%d more", len(diff)-3))
		}
		return false, "pack differs from commit " + rev + ": " + strings.Join(diff, ", ")
	}
	return true, fmt.Sprintf("%d files identical to commit %s (checkout HEAD %s)", len(committed), rev, short(head))
}

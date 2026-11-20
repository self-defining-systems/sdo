package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
	subj := subjectChecks(hookSubject{})
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

			// 7 capacity
			planned := plannedLanes(inv, h, hs)
			var out string
			var err error
			if host.IsVcluster() {
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
				if !host.IsVcluster() {
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

// ---- 1 credentials --------------------------------------------------------------------------------------------

type credCheck struct {
	Provider, Model, Effort, AuthFile, Image string
	// Env is the agent's extra environment (operator settings config.agent.extra_env, "K=V"): a provider reached
	// through another backend (CLAUDE_CODE_USE_VERTEX=1 with GOOGLE_APPLICATION_CREDENTIALS) is only reached with it.
	Env   []string
	Specs []string
}

// agentCredentialsDir is where the agent pod mounts the credential file (.credentials.json for claude, auth.json for
// codex); an extra_env entry may name a path in it (GOOGLE_APPLICATION_CREDENTIALS=/sdo/credentials/.credentials.json).
const agentCredentialsDir = "/sdo/credentials"

// agentExtraEnv is settings config.agent.extra_env: a list of "K=V" strings, or a map.
func agentExtraEnv(set map[string]any) []string {
	cfg, _ := set["config"].(map[string]any)
	agent, _ := cfg["agent"].(map[string]any)
	var out []string
	switch e := agent["extra_env"].(type) {
	case []any:
		for _, v := range e {
			if s, ok := v.(string); ok && strings.Contains(s, "=") {
				out = append(out, s)
			}
		}
	case map[string]any:
		for k, v := range e {
			out = append(out, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(out)
	}
	return out
}

func (c credCheck) label() string { return strings.Join(c.Specs, ",") }

// credentialChecks lists the distinct (provider, model, effort, credential file, image) the specs' lanes will use.
func credentialChecks(specs []specFile) []credCheck {
	idx := map[string]int{}
	var out []credCheck
	for _, s := range specs {
		if s.Operator.Hooks == "" {
			continue
		}
		set := s.Operator.Settings
		provider := settingString(set, "provider", "codex")
		auth := "~/.codex/auth.json"
		if provider == "claude" {
			auth = "~/.claude/.credentials.json"
		}
		c := credCheck{Provider: provider, Model: settingString(set, "model", ""), Effort: settingString(set, "effort", ""),
			AuthFile: settingString(set, "auth_file", auth),
			Image:    "sdo-responder:" + settingString(set, "image_tag", "fz-"+short8(s.Operator.Revision)), Env: agentExtraEnv(set)}
		k := fmt.Sprint(c.Provider, c.Model, c.Effort, c.AuthFile, c.Image, c.Env)
		if i, ok := idx[k]; ok {
			out[i].Specs = append(out[i].Specs, s.Name)
			continue
		}
		idx[k] = len(out)
		c.Specs = []string{s.Name}
		out = append(out, c)
	}
	return out
}

// homePath makes a "~/x" path usable inside a double-quoted shell word.
func homePath(p string) string {
	if strings.HasPrefix(p, "~/") {
		return `"$HOME/` + strings.ReplaceAll(p[2:], `"`, `\"`) + `"`
	}
	return shellQuote(p)
}

// script runs one tiny call in the responder image as the agent pod makes it: the lane's credential file mounted
// read-only where the pod mounts it (agentCredentialsDir) and copied into the CLI's home as the agent does, with the
// agent's extra environment. The file is never printed.
func (c credCheck) script() string {
	var inner, file string
	if c.Provider == "claude" {
		file = ".credentials.json"
		inner = `mkdir -p $HOME/.claude && cp ` + agentCredentialsDir + `/` + file + ` $HOME/.claude/.credentials.json && claude -p`
		if c.Model != "" {
			inner += " --model " + shellQuote(c.Model)
		}
		if c.Effort != "" {
			inner += " --effort " + shellQuote(c.Effort)
		}
		inner += ` "Reply with the single word ok"`
	} else {
		file = "auth.json"
		inner = `mkdir -p $HOME/.codex && cp ` + agentCredentialsDir + `/` + file + ` $HOME/.codex/auth.json && CODEX_HOME=$HOME/.codex codex exec --skip-git-repo-check`
		if c.Model != "" {
			inner += " -m " + shellQuote(c.Model)
		}
		if c.Effort != "" {
			inner += " -c " + shellQuote("model_reasoning_effort="+c.Effort)
		}
		inner += ` "Reply with the single word ok"`
	}
	var env strings.Builder
	for _, e := range c.Env {
		env.WriteString("-e " + shellQuote(e) + " ")
	}
	return fmt.Sprintf(`timeout 100 docker run --rm -v %s:%s/%s:ro -e HOME=/tmp/h %s--entrypoint sh %s -c %s 2>&1`,
		homePath(c.AuthFile), agentCredentialsDir, file, env.String(), shellQuote(c.Image), shellQuote(inner))
}

var okWord = regexp.MustCompile(`(?i)(^|\W)ok(\W|$)`)

// judgeCall: the call must exit 0 and its last line must contain "ok"; otherwise its last line is the reason
// (for example "You've hit your weekly limit").
func judgeCall(out string, err error) (bool, string) {
	last := lastLine(out)
	if err != nil {
		if last == "" {
			last = err.Error()
		}
		return false, "call failed: " + tail(last, 160)
	}
	if !okWord.MatchString(last) {
		return false, "unexpected answer: " + tail(last, 160)
	}
	return true, "answered ok"
}

func lastLine(s string) string {
	last := ""
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			last = l
		}
	}
	return last
}

// ---- 2 sink, 3 proto --------------------------------------------------------------------------------------------

// sinkSnippet runs the pinned sink's transcript counter over a synthetic Claude session file.
const sinkSnippet = `import pathlib, sys, tempfile
from benchmarks.fuzzops import sink
with tempfile.TemporaryDirectory() as t:
    d = pathlib.Path(t)
    f = d / "claude" / "projects" / "-synthetic" / "session.jsonl"
    f.parent.mkdir(parents=True)
    f.write_text('{"type": "user"}\n')
    n = sink.count_rollouts(d)
if n != 1:
    print("PREFLIGHT counted %d transcripts in a synthetic Claude session, want 1" % n)
    sys.exit(1)
print("PREFLIGHT ok: counts a Claude session file")`

// protoSnippet validates a ProgramRun for every binding source the pinned code can emit (the constants of its
// candidate producer, plus the known set) with SDO's own protovalidate rules, as an incident request carries them.
const protoSnippet = `import pathlib, re, sys
from sdo.contracts import proto
from sdo.contracts._gen.sdodev.contracts.v1alpha1 import program_pb2 as p
src = pathlib.Path("sdo/operator/runtime/program_candidates.go")
text = src.read_text() if src.is_file() else ""
sources = sorted(set(re.findall(r'BindingSource\w*\s*=\s*"(\w+)"', text)) | {"finding", "hint", "sweep", "suspect", "credited"})
bad = []
for s in sources:
    run = p.ProgramRun(program_id="synthetic", binding_source=s)
    run.verdict.result = 1
    run.duration.FromMilliseconds(5)
    try:
        proto.validate(run)
    except ValueError as e:
        bad.append("%s (%s)" % (s, str(e)[-60:]))
if bad:
    print("PREFLIGHT rejected binding sources: " + "; ".join(bad))
    sys.exit(1)
print("PREFLIGHT ok: %d binding sources valid" % len(sources))`

// judgeSnippet: a snippet exits 0 and prints a "PREFLIGHT" line.
func judgeSnippet(out string, err error) (bool, string) {
	line := ""
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "PREFLIGHT ") {
			line = strings.TrimPrefix(l, "PREFLIGHT ")
		}
	}
	if err != nil {
		if line == "" {
			line = lastLine(out)
		}
		if line == "" {
			line = err.Error()
		}
		return false, tail(line, 220)
	}
	if line == "" {
		return false, "no result line: " + tail(out, 120)
	}
	return true, line
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

// ---- 6 memory -----------------------------------------------------------------------------------------------------

type baselineJSON struct {
	Namespace string `json:"namespace"`
	Bundle    string `json:"bundle"`
	Branch    string `json:"branch"`
	Commit    string `json:"commit"`
}

func readBaselineJSON(dir string) (baselineJSON, error) {
	var b baselineJSON
	raw, err := os.ReadFile(filepath.Join(dir, "baseline.json"))
	if err != nil {
		return b, err
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return b, fmt.Errorf("%s/baseline.json: %v", dir, err)
	}
	if b.Bundle == "" || b.Branch == "" || b.Commit == "" {
		return b, fmt.Errorf("%s/baseline.json: bundle, branch and commit are required", dir)
	}
	return b, nil
}

// memoryScript clones the baseline's bundle, prints its head, builds the pinned sdo-operator and runs
// `sdo-operator memory migrate` on the clone: what the controller does on its first memory read (upkeep to the
// current schema), then the same accepted-memory audit as `memory check --accepted`, without replay.
func memoryScript(baselineDir string, b baselineJSON, hooksDir string) string {
	opRoot := shellQuote(filepath.Join(hooksDir, "sdo", "operator"))
	return fmt.Sprintf(`T=$(mktemp -d) || exit 2; trap 'rm -rf "$T"' EXIT; `+
		`git clone -q -b %[1]s %[2]s "$T/m" || { echo "PREFLIGHT clone failed"; exit 2; }; echo "HEAD $(git -C "$T/m" rev-parse HEAD)"; `+
		`(cd %[3]s && go build -o "$T/sdo-operator" ./cmd/sdo-operator) || { echo "PREFLIGHT go build sdo-operator failed"; exit 2; }; `+
		`mkdir "$T/w"; SDO_OPERATOR_ROOT=%[3]s "$T/sdo-operator" memory migrate "$T/m" --worktree-root "$T/w" --replay off >"$T/out" 2>&1; rc=$?; sed 's/^/CHECK /' "$T/out"; echo "EXIT $rc"; true`,
		shellQuote(b.Branch), shellQuote(filepath.Join(baselineDir, b.Bundle)), opRoot)
}

// judgeMemory: the bundle's head is the commit baseline.json names and the memory check printed no diagnostics.
func judgeMemory(out string, err error, commit string, allow []string) (bool, string) {
	if err != nil {
		return false, "probe failed: " + tail(err.Error()+" "+out, 200)
	}
	head, exit := "", "?"
	var diag []string
	for _, l := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(l, "EXIT "):
			exit = strings.TrimSpace(strings.TrimPrefix(l, "EXIT "))
		case strings.HasPrefix(l, "HEAD "):
			head = strings.TrimSpace(strings.TrimPrefix(l, "HEAD "))
		case strings.HasPrefix(l, "CHECK "):
			diag = append(diag, strings.TrimPrefix(l, "CHECK "))
		case strings.HasPrefix(l, "PREFLIGHT "):
			return false, strings.TrimPrefix(l, "PREFLIGHT ")
		}
	}
	if head != commit {
		return false, fmt.Sprintf("bundle head %s, baseline.json says %s", short(head), short(commit))
	}
	var left []string
	for _, d := range diag {
		allowed := false
		for _, a := range allow {
			allowed = allowed || strings.Contains(d, a)
		}
		if !allowed {
			left = append(left, d)
		}
	}
	if exit != "0" && (len(left) > 0 || len(diag) == 0) {
		return false, "memory migrate+check exit " + exit + ": " + clip(strings.Join(left, " | "), 200)
	}
	if len(diag) > 0 {
		return true, fmt.Sprintf("bundle head matches baseline.json; memory check clean (%d allowed diagnostics)", len(diag))
	}
	return true, "bundle head matches baseline.json; memory check clean"
}

//go:build integration

package deploy_test

// Tests of node-preload.sh and kind-lane.sh's preload (docs/bench.md, "Images: one registry per host") against
// stub docker, kind and kubectl: the template's key, that an existing template is not rebuilt, that a clone shares
// the immutable files and copies the databases, that the nodes keep the base image, and that a missing or failing
// preload leaves today's cluster config.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

const base = "kindest/node:v1.35.0@sha256:aaaa"

// stub docker: `run --entrypoint sh ... -c SCRIPT args` runs the script here with /w/ pointing at the preload
// root; the build container's `cp -a /var/lib/containerd /out/containerd` writes a small fake containerd directory.
const stubDocker = `#!/usr/bin/env bash
echo "docker $*" >> "$STUB_LOG"
case "$1 $2" in
  "image inspect") exit 0 ;;
  "network inspect") exit 1 ;;
  "inspect -f") case "$3" in *bench-evidence*) printf '%s' "$STUB_MOUNT" ;; *CgroupParent*) printf '%s\n' "$STUB_CGROUP_PARENT" ;; *HostConfig.Memory*) printf '%s\n' "${STUB_NODE_MEMORY-4294967296}" ;; *) echo "$STUB_BASE sha256:id" ;; esac; exit 0 ;;
  "images --format") echo "$STUB_BASE"; exit 0 ;;
esac
case "$1" in
  run)
    case "$*" in
      *fzkernelcheck*)
        # a real docker run of the probe script, with the stub iptables (STUB_NO_MANGLE / STUB_NO_COMMENT make them reject as a
        # kernel without that netfilter option would); STUB_DOCKER_FAIL=N is docker itself failing, STUB_DOCKER_HANG a hung daemon
        [ -n "$STUB_DOCKER_HANG" ] && exec sleep 30
        [ -n "$STUB_DOCKER_FAIL" ] && { echo "docker: Error response from daemon: stub failure" >&2; exit "$STUB_DOCKER_FAIL"; }
        while [ "$1" != -c ]; do shift; done; shift
        PATH="$STUB_DIR/ipt:$PATH" exec sh -c "$1" ;;
      *fzpreload*)
        [ -n "$STUB_FAIL_BUILD" ] && exit 1
        for a in "$@"; do case "$a" in *:/out) echo "${a%:/out}" > "$STUB_DIR/out" ;; esac; done ;;
      *"--entrypoint sh"*)
        while [ "$1" != -c ]; do shift; done; shift
        script=${1//\/w\//$FUZZOPS_PRELOAD_ROOT/}; shift
        exec sh -c "$script" "$@" ;;
    esac ;;
  exec)
    case "$*" in
      *"images ls -q"*) printf 'docker.io/library/redis:7\nregistry.example/app/web:v1\n' ;;
      *"cp -a /var/lib/containerd /out/containerd"*)
        d=$(cat "$STUB_DIR/out")/containerd
        mkdir -p "$d/io.containerd.metadata.v1.bolt" "$d/io.containerd.content.v1.content/blobs/sha256" "$d/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs"
        echo db > "$d/io.containerd.metadata.v1.bolt/meta.db"
        echo blob > "$d/io.containerd.content.v1.content/blobs/sha256/b1"
        echo layer > "$d/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs/f" ;;
    esac ;;
esac
exit 0
`

const stubKind = `#!/usr/bin/env bash
echo "kind $*" >> "$STUB_LOG"
case "$1 $2" in
  "get clusters") exit 0 ;;
  "get nodes") echo lane-control-plane; exit 0 ;;
  "create cluster")
    while [ $# -gt 0 ]; do
      [ "$1" = --config ] && cp "$2" "$STUB_DIR/kind-config"
      [ "$1" = --image ] && echo "$2" > "$STUB_DIR/kind-image-flag"
      [ "$1" = --kubeconfig ] && : > "$2"
      shift
    done
    exit 0 ;;
esac
`

type env struct {
	t         *testing.T
	dir, root string
	vars      []string
}

func setup(t *testing.T) *env {
	t.Helper()
	for _, tool := range []string{"bash", "flock", "sha256sum", "cp", "find"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skip(tool + " not available")
		}
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	os.MkdirAll(bin, 0o755)
	for name, body := range map[string]string{"docker": stubDocker, "kind": stubKind, "kubectl": "#!/bin/sh\ncat >/dev/null\n", "curl": "#!/bin/sh\nexit 0\n",
		"mount": "#!/bin/sh\n[ -z \"$STUB_NO_OVERLAY\" ]\n", "umount": "#!/bin/sh\nexit 0\n"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// the iptables the kernel-check probe finds "in the node image": rejects as the kernel options it stands for are absent
	ipt := filepath.Join(dir, "ipt")
	os.MkdirAll(ipt, 0o755)
	for _, name := range []string{"iptables-legacy", "iptables-nft"} {
		body := "#!/bin/sh\n" +
			"case \"$*\" in *\"-N \"*) [ -z \"$STUB_NO_MANGLE\" ] || { echo 'iptables v1.8: can'\\''t initialize iptables table `mangle'\\'': Table does not exist' >&2; exit 3; } ;; esac\n" +
			"case \"$*\" in *\"-m comment\"*) [ -z \"$STUB_NO_COMMENT\" ] || { echo 'iptables v1.8: Couldn'\\''t load match `comment'\\'': No such file or directory' >&2; exit 2; } ;; esac\nexit 0\n"
		if err := os.WriteFile(filepath.Join(ipt, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	pack := filepath.Join(dir, "pack")
	os.MkdirAll(pack, 0o755)
	os.WriteFile(filepath.Join(pack, "images.sh"), []byte("#!/bin/sh\nprintf 'redis:7\\nregistry.example/app/web:v1\\n'\n"), 0o755)
	root := filepath.Join(dir, "preload")
	return &env{t: t, dir: dir, root: root, vars: append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "STUB_DIR="+dir,
		"STUB_LOG="+filepath.Join(dir, "log"), "STUB_BASE="+base, "STUB_MOUNT="+filepath.Join(dir, "ev"), "FUZZOPS_PRELOAD_ROOT="+root, "KIND_NODE_PRELOAD="+pack)}
}

func (e *env) with(kv ...string) []string { return append(append([]string{}, e.vars...), kv...) }

func (e *env) run(vars []string, args ...string) (string, error) {
	e.t.Helper()
	cmd := exec.Command("bash", args...)
	cmd.Env = vars
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (e *env) log() string { b, _ := os.ReadFile(filepath.Join(e.dir, "log")); return string(b) }

func TestPreloadKeyFollowsBaseAndList(t *testing.T) {
	e := setup(t)
	key := func(args ...string) string {
		out, err := e.run(e.vars, append([]string{"node-preload.sh", "key"}, args...)...)
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return strings.TrimSpace(out)
	}
	a := key(base, "redis:7", "web:v1")
	if a != key(base, "web:v1", "redis:7") {
		t.Fatal("the key depends on the order of the image list")
	}
	if a == key(base, "redis:7", "web:v2") {
		t.Fatal("a different image list has the same key")
	}
	if a == key("kindest/node:v1.35.0@sha256:bbbb", "redis:7", "web:v1") {
		t.Fatal("a different base digest has the same key")
	}
}

func TestPreloadEnsureBuildsOnceAndFailsClean(t *testing.T) {
	e := setup(t)
	out, err := e.run(e.with("STUB_FAIL_BUILD=1"), "node-preload.sh", "ensure", base, "redis:7")
	if err == nil {
		t.Fatalf("a failed build must fail: %s", out)
	}
	if ents, _ := os.ReadDir(filepath.Join(e.root, "templates")); len(ents) != 0 {
		t.Fatalf("a failed build left %v", ents)
	}
	out, err = e.run(e.vars, "node-preload.sh", "ensure", base, "redis:7")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	key := strings.TrimSpace(out)
	if _, err := os.Stat(filepath.Join(e.root, "templates", key, "containerd", "io.containerd.metadata.v1.bolt", "meta.db")); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(e.dir, "log"), nil, 0o644)
	if _, err := e.run(e.vars, "node-preload.sh", "ensure", base, "redis:7"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(e.log(), "fzpreload") {
		t.Fatalf("an existing template was rebuilt:\n%s", e.log())
	}
}

func inode(t *testing.T, p string) (ino uint64, links uint64) {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	st := fi.Sys().(*syscall.Stat_t)
	return st.Ino, uint64(st.Nlink)
}

func TestPreloadCloneSharesImmutableFilesAndCopiesDatabases(t *testing.T) {
	e := setup(t)
	out, err := e.run(e.vars, "node-preload.sh", "ensure", base, "redis:7")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	key := strings.TrimSpace(out)
	out, err = e.run(e.vars, "node-preload.sh", "clone", base, key, "lane1", "1")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	dir := strings.TrimSpace(out)
	tpl := filepath.Join(e.root, "templates", key, "containerd")
	for _, rel := range []string{"io.containerd.content.v1.content/blobs/sha256/b1", "io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs/f"} {
		a, n := inode(t, filepath.Join(tpl, rel))
		b, _ := inode(t, filepath.Join(dir, rel))
		if a != b || n < 2 {
			t.Fatalf("%s should be a hard link of the template's", rel)
		}
	}
	a, _ := inode(t, filepath.Join(tpl, "io.containerd.metadata.v1.bolt/meta.db"))
	b, _ := inode(t, filepath.Join(dir, "io.containerd.metadata.v1.bolt/meta.db"))
	if a == b {
		t.Fatal("the metadata database must be a copy: containerd writes it in place")
	}
	if _, err := e.run(e.vars, "node-preload.sh", "drop", "lane1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); err == nil {
		t.Fatal("drop left the lane directory")
	}
	if _, err := os.Stat(tpl); err != nil {
		t.Fatal("drop must not touch the template")
	}
}

func (e *env) laneConfig(vars []string) string {
	e.t.Helper()
	os.Remove(filepath.Join(e.dir, "kind-config"))
	os.Remove(filepath.Join(e.dir, "kind-image-flag"))
	out, err := e.run(vars, "kind-lane.sh", "up", "lane", filepath.Join(e.dir, "kc"), filepath.Join(e.dir, "ev"))
	if err != nil {
		e.t.Fatalf("%v: %s", err, out)
	}
	cfg, err := os.ReadFile(filepath.Join(e.dir, "kind-config"))
	if err != nil {
		e.t.Fatal(err)
	}
	return string(cfg)
}

func TestLanePreloadMountsWorkersOnlyAndKeepsTheBaseImage(t *testing.T) {
	e := setup(t)
	cfg := e.laneConfig(e.with("KIND_NODE_IMAGE="+base, "KIND_NODE_PRELOAD_BUILD=on"))
	if strings.Count(cfg, "containerPath: /var/lib/containerd") != 2 {
		t.Fatalf("both workers need a containerd directory:\n%s", cfg)
	}
	if i := strings.Index(cfg, "role: worker"); strings.Contains(cfg[:i], "/var/lib/containerd") {
		t.Fatalf("the control plane must not get one:\n%s", cfg)
	}
	if b, _ := os.ReadFile(filepath.Join(e.dir, "kind-image-flag")); strings.TrimSpace(string(b)) != base {
		t.Fatalf("the node image must stay the base, got %q", b)
	}
	rec, err := os.ReadFile(filepath.Join(e.dir, "kc.node-image"))
	if err != nil || !strings.Contains(string(rec), `"preload_template":"`) || strings.Contains(string(rec), `"preload_template":""`) {
		t.Fatalf("lane record: %v %s", err, rec)
	}
	for _, d := range []string{"lane-w1", "lane-w2"} {
		if _, err := os.Stat(filepath.Join(e.root, "lanes", d)); err != nil {
			t.Fatalf("%s: %v", d, err)
		}
	}
	if out, err := e.run(e.vars, "kind-lane.sh", "down", "lane"); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if ents, _ := os.ReadDir(filepath.Join(e.root, "lanes")); len(ents) != 0 {
		t.Fatalf("down left %v", ents)
	}
}

func TestLaneWithoutAPreloadIsTodaysCluster(t *testing.T) {
	e := setup(t)
	for name, vars := range map[string][]string{
		"build off, no template": e.with("KIND_NODE_IMAGE="+base, "KIND_NODE_PRELOAD_BUILD=off"),
		"failing build":          e.with("KIND_NODE_IMAGE="+base, "STUB_FAIL_BUILD=1"),
		"no pinned base":         e.with(),
	} {
		cfg := e.laneConfig(vars)
		if strings.Contains(cfg, "/var/lib/containerd") {
			t.Fatalf("%s: no preload expected:\n%s", name, cfg)
		}
	}
	// the base image flag is today's, unless no base is pinned
	e.laneConfig(e.with("KIND_NODE_IMAGE="+base, "KIND_NODE_PRELOAD_BUILD=off"))
	if b, _ := os.ReadFile(filepath.Join(e.dir, "kind-image-flag")); strings.TrimSpace(string(b)) != base {
		t.Fatalf("today's path passes --image %s, got %q", base, b)
	}
}

// A relative KIND_NODE_PRELOAD names a pack directory under the fuzzops root, whatever the caller's cwd
// (kind-lane.sh itself runs from deploy/).
func TestLanePreloadPackResolvesAgainstTheFuzzopsRoot(t *testing.T) {
	e := setup(t)
	repo := filepath.Join(e.dir, "repo")
	os.MkdirAll(filepath.Join(repo, "examples", "demo"), 0o755)
	os.MkdirAll(filepath.Join(repo, "deploy"), 0o755)
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range ents {
		if strings.HasSuffix(f.Name(), ".sh") {
			b, _ := os.ReadFile(f.Name())
			os.WriteFile(filepath.Join(repo, "deploy", f.Name()), b, 0o755)
		}
	}
	os.WriteFile(filepath.Join(repo, "examples", "demo", "images.sh"), []byte("#!/bin/sh\nprintf 'redis:7\\nregistry.example/app/web:v1\\n'\n"), 0o755)
	elsewhere := filepath.Join(e.dir, "elsewhere")
	os.MkdirAll(elsewhere, 0o755)
	run := func(pack string) string {
		os.Remove(filepath.Join(e.dir, "kind-config"))
		cmd := exec.Command("bash", filepath.Join(repo, "deploy", "kind-lane.sh"), "up", "lane", filepath.Join(e.dir, "kc"), filepath.Join(e.dir, "ev"))
		cmd.Dir = elsewhere
		cmd.Env = e.with("KIND_NODE_IMAGE="+base, "KIND_NODE_PRELOAD="+pack)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return string(out)
	}
	run("examples/demo")
	cfg, _ := os.ReadFile(filepath.Join(e.dir, "kind-config"))
	if !strings.Contains(string(cfg), "/var/lib/containerd") {
		t.Fatalf("a relative pack dir was not preloaded:\n%s", cfg)
	}
	// a pack that cannot list its images: the message names the pack dir and what failed
	out := run("examples/missing")
	if !strings.Contains(out, "examples/missing") || !strings.Contains(out, "images.sh") || strings.Contains(out, "image list failed: no preload") {
		t.Fatalf("the failure must name the pack's images.sh and its path:\n%s", out)
	}
}

// A cluster that already exists (adopted by a new bench root) gets its kubeconfig written under that root from
// `kind get kubeconfig`, whether or not the file or its directory was there.
func TestLaneUpOnExistingClusterRewritesTheKubeconfig(t *testing.T) {
	e := setup(t)
	kind := `#!/usr/bin/env bash
case "$1 $2" in
  "get clusters") echo lane; exit 0 ;;
  "get nodes") echo lane-control-plane; exit 0 ;;
  "get kubeconfig") echo "current-context: kind-lane"; exit 0 ;;
  "create cluster") echo "create must not run" >&2; exit 1 ;;
esac
`
	if err := os.WriteFile(filepath.Join(e.dir, "bin", "kind"), []byte(kind), 0o755); err != nil {
		t.Fatal(err)
	}
	kc := filepath.Join(e.dir, "newroot", "kube", "lane")
	for _, pre := range []bool{false, true} {
		os.RemoveAll(filepath.Join(e.dir, "newroot"))
		if pre {
			os.MkdirAll(filepath.Dir(kc), 0o755)
			os.WriteFile(kc, []byte("stale"), 0o600)
		}
		if out, err := e.run(e.with("KIND_NODE_PRELOAD="), "kind-lane.sh", "up", "lane", kc, filepath.Join(e.dir, "ev")); err != nil {
			t.Fatalf("existing=%v: %v: %s", pre, err, out)
		}
		if b, _ := os.ReadFile(kc); !strings.Contains(string(b), "kind-lane") {
			t.Fatalf("existing=%v: kubeconfig not rewritten: %q", pre, b)
		}
	}
}

// A cluster adopted under a new bench root keeps its original evidence mount; the new root's evidence directory
// must lead to it, and a cluster without the mount is refused.
func TestLaneUpOnExistingClusterFollowsItsEvidenceMount(t *testing.T) {
	e := setup(t)
	kind := `#!/usr/bin/env bash
case "$1 $2" in
  "get clusters") echo lane; exit 0 ;;
  "get nodes") echo lane-control-plane; exit 0 ;;
  "get kubeconfig") echo "current-context: kind-lane"; exit 0 ;;
esac
`
	docker := `#!/bin/sh
case "$1" in
  inspect) printf '%s' "$STUB_MOUNT" ;;
esac
exit 0
`
	for name, body := range map[string]string{"kind": kind, "docker": docker} {
		os.WriteFile(filepath.Join(e.dir, "bin", name), []byte(body), 0o755)
	}
	old := filepath.Join(e.dir, "oldroot", "evidence", "lane")
	os.MkdirAll(old, 0o755)
	kc := filepath.Join(e.dir, "newroot", "kube", "lane")
	ev := filepath.Join(e.dir, "newroot", "evidence", "lane")
	up := func() (string, error) {
		return e.run(e.with("KIND_NODE_PRELOAD=", "STUB_MOUNT="+old), "kind-lane.sh", "up", "lane", kc, ev)
	}
	for i := 0; i < 2; i++ { // the second run finds the link already in place
		if out, err := up(); err != nil {
			t.Fatalf("run %d: %v: %s", i, err, out)
		}
		os.WriteFile(filepath.Join(ev, "probe"), []byte("x"), 0o644)
		if _, err := os.Stat(filepath.Join(old, "probe")); err != nil {
			t.Fatalf("run %d: %s does not lead to the cluster's mount source %s", i, ev, old)
		}
	}
	// a non-empty real directory that the cluster does not write to is not silently replaced
	os.Remove(ev)
	os.MkdirAll(ev, 0o755)
	os.WriteFile(filepath.Join(ev, "keep"), []byte("x"), 0o644)
	if out, err := up(); err == nil || !strings.Contains(out, "not empty") {
		t.Fatalf("want a clear refusal, got %v: %s", err, out)
	}
	// no evidence mount at all
	if out, err := e.run(e.with("KIND_NODE_PRELOAD=", "STUB_MOUNT="), "kind-lane.sh", "up", "lane", kc, filepath.Join(e.dir, "ev2")); err == nil || !strings.Contains(out, "no /bench-evidence mount") {
		t.Fatalf("want refusal for a cluster without the mount, got %v: %s", err, out)
	}
}

// A preload root on a filesystem that cannot hold an overlayfs upper layer (NFS, for one) mounts fine as a hostPath
// but every pod's overlay mount then fails with "invalid argument": the preload must be refused at once, before
// any template is built or lane directory cloned, not found out as a broken lane.
func TestPreloadRefusesARootThatCannotHoldOverlayLayers(t *testing.T) {
	e := setup(t)
	bad := e.with("STUB_NO_OVERLAY=1", "KIND_NODE_IMAGE="+base)
	out, err := e.run(bad, "node-preload.sh", "check", base)
	if err == nil || !strings.Contains(out, "overlay") || !strings.Contains(out, "FUZZOPS_PRELOAD_ROOT") {
		t.Fatalf("check must fail naming overlay and FUZZOPS_PRELOAD_ROOT: %v %s", err, out)
	}
	if _, err := e.run(e.vars, "node-preload.sh", "check", base); err != nil {
		t.Fatalf("a usable root must pass: %v", err)
	}
	if out, err := e.run(bad, "node-preload.sh", "ensure", base, "redis:7"); err == nil {
		t.Fatalf("ensure must not build on an unusable root: %s", out)
	}
	if strings.Contains(e.log(), "fzpreload") {
		t.Fatalf("a template build started on an unusable root:\n%s", e.log())
	}
	// the lane is not made without the preload: asking for one on a bad root fails the lane up
	kc := filepath.Join(e.dir, "kc")
	out, err = e.run(bad, "kind-lane.sh", "up", "lane1", kc, filepath.Join(e.dir, "ev"))
	if err == nil || !strings.Contains(out, "overlay") {
		t.Fatalf("lane up must fail loudly: %v %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(e.dir, "kind-config")); err == nil {
		t.Fatal("a cluster was created despite the unusable preload root")
	}
}

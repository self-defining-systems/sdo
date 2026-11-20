//go:build integration

package deploy_test

// Tests of ensure-images.sh against stub docker and kind: a node that has an image is left alone, kind load
// is asked only for the nodes that lack it, and when docker cannot pull it or kind cannot load it the nodes
// pull it with crictl.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The stub cluster has nodes n-control-plane, n-w1 and n-w2; each node's images are lines "REF<TAB>ID DIGESTS..." of
// $STUB_DIR/img-<node>. The host's docker has the image when STUB_HOST_HAS is set: its value is "ID DIGESTS...".
const imgDocker = `#!/usr/bin/env bash
echo "docker $*" >> "$STUB_LOG"
case "$1 $2" in
  "image inspect")
    [ -n "$STUB_HOST_HAS" ] || exit 1
    [ "$3" = -f ] && echo "$STUB_HOST_HAS"
    exit 0 ;;
  "pull -q") [ -z "$STUB_DOCKER_PULL_FAIL" ]; exit $? ;;
esac
if [ "$1" = exec ]; then
  node=$2; shift 2
  [ "$1" = crictl ] || exit 0
  shift
  case "$1" in
    inspecti)
      shift
      if [ "$1" = -o ]; then ref=$5; else ref=$1; fi
      line=$(grep -F "$ref	" "$STUB_DIR/img-$node" 2>/dev/null | head -1) || exit 1
      [ -n "$line" ] || exit 1
      [ "$1" = -o ] && echo "${line#*	}"
      exit 0 ;;
    pull) [ -n "$STUB_CRICTL_FAIL" ] && exit 1; printf '%s\tpulled\n' "$2" >> "$STUB_DIR/img-$node"; exit 0 ;;
  esac
fi
exit 0
`

const imgKind = `#!/usr/bin/env bash
echo "kind $*" >> "$STUB_LOG"
case "$1 $2" in
  "get nodes") printf 'n-control-plane\nn-w1\nn-w2\n'; exit 0 ;;
  "load docker-image")
    [ -n "$STUB_LOAD_FAIL" ] && exit 1
    [ -n "$STUB_LOAD_SILENT" ] && exit 0
    nodes=""; img=""
    while [ $# -gt 0 ]; do case "$1" in --nodes) nodes=$2; shift ;; --name) shift ;; load|docker-image) ;; *) img=$1 ;; esac; shift; done
    for n in ${nodes//,/ }; do
      grep -vF "docker.io/library/$img	" "$STUB_DIR/img-$n" > "$STUB_DIR/img-$n.new" 2>/dev/null; mv "$STUB_DIR/img-$n.new" "$STUB_DIR/img-$n"
      printf 'docker.io/library/%s\t%s\n' "$img" "$STUB_HOST_HAS" >> "$STUB_DIR/img-$n"
    done
    exit 0 ;;
esac
exit 0
`

type imgEnv struct {
	t    *testing.T
	dir  string
	vars []string
}

func imgSetup(t *testing.T) *imgEnv {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	os.MkdirAll(bin, 0o755)
	for name, body := range map[string]string{"docker": imgDocker, "kind": imgKind} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return &imgEnv{t: t, dir: dir, vars: append(os.Environ(), "PATH="+bin+":"+os.Getenv("PATH"), "STUB_DIR="+dir, "STUB_LOG="+filepath.Join(dir, "log"))}
}

// has: the node holds each ref (its image ID and digests are "id").
func (e *imgEnv) has(node string, refs ...string) { e.hasID(node, "id", refs...) }

func (e *imgEnv) hasID(node, id string, refs ...string) {
	e.t.Helper()
	var b strings.Builder
	for _, r := range refs {
		b.WriteString(r + "\t" + id + "\n")
	}
	if err := os.WriteFile(filepath.Join(e.dir, "img-"+node), []byte(b.String()), 0o644); err != nil {
		e.t.Fatal(err)
	}
}

func (e *imgEnv) nodeHolds(node, ref string) bool {
	b, _ := os.ReadFile(filepath.Join(e.dir, "img-"+node))
	return strings.Contains("\n"+string(b), "\n"+ref+"\t")
}

func (e *imgEnv) run(kv ...string) (string, string, error) {
	e.t.Helper()
	return e.runImages([]string{"mongo:3.4"}, kv...)
}

func (e *imgEnv) runImages(images []string, kv ...string) (string, string, error) {
	e.t.Helper()
	cmd := exec.Command("bash", append([]string{"ensure-images.sh", "lane"}, images...)...)
	cmd.Env = append(append([]string{}, e.vars...), kv...)
	out, err := cmd.CombinedOutput()
	log, _ := os.ReadFile(filepath.Join(e.dir, "log"))
	return string(out), string(log), err
}

const mongo = "docker.io/library/mongo:3.4"

func TestEnsureImagesLeavesNodesThatHaveTheImage(t *testing.T) {
	e := imgSetup(t)
	e.has("n-w1", mongo)
	e.has("n-w2", mongo)
	out, log, err := e.run()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if strings.Contains(log, "kind load") || strings.Contains(log, "docker pull") || strings.Contains(log, "crictl pull") {
		t.Fatalf("a cluster that has the image loaded or pulled it:\n%s", log)
	}
	if strings.Contains(log, "n-control-plane") {
		t.Fatalf("the control-plane node takes no app pods:\n%s", log)
	}
}

func TestEnsureImagesLoadsOnlyTheNodesThatLackIt(t *testing.T) {
	e := imgSetup(t)
	e.has("n-w1", mongo)
	out, log, err := e.run("STUB_HOST_HAS=id")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !strings.Contains(log, "kind load docker-image --name lane --nodes n-w2 mongo:3.4") || strings.Contains(log, "--nodes n-w1") {
		t.Fatalf("want one kind load for n-w2 only:\n%s", log)
	}
	if !e.nodeHolds("n-w2", mongo) || strings.Contains(log, "crictl pull") {
		t.Fatalf("n-w2 does not hold the image or fell back needlessly:\n%s", log)
	}
}

func TestEnsureImagesPullsOnTheNodeWhenKindLoadFails(t *testing.T) {
	e := imgSetup(t)
	out, log, err := e.run("STUB_HOST_HAS=id", "STUB_LOAD_FAIL=1")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !e.nodeHolds("n-w1", mongo) || !e.nodeHolds("n-w2", mongo) || !strings.Contains(log, "exec n-w1 crictl pull "+mongo) {
		t.Fatalf("the nodes did not pull it themselves:\n%s", log)
	}
}

// kind load can exit 0 without the node having the image (the digest it cannot find).
func TestEnsureImagesPullsOnTheNodeWhenKindLoadDeliversNothing(t *testing.T) {
	e := imgSetup(t)
	out, _, err := e.run("STUB_HOST_HAS=id", "STUB_LOAD_SILENT=1")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !e.nodeHolds("n-w1", mongo) || !e.nodeHolds("n-w2", mongo) {
		t.Fatal("a silent kind load left the nodes without the image")
	}
}

func TestEnsureImagesPullsOnTheNodeWhenDockerCannotPull(t *testing.T) {
	e := imgSetup(t)
	out, log, err := e.run("STUB_DOCKER_PULL_FAIL=1")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if strings.Contains(log, "kind load") {
		t.Fatalf("kind load was asked for an image the host does not have:\n%s", log)
	}
	if !e.nodeHolds("n-w1", mongo) || !e.nodeHolds("n-w2", mongo) {
		t.Fatalf("the nodes did not pull it themselves:\n%s", log)
	}
}

func TestEnsureImagesFailsNamingTheImageWhenNothingWorks(t *testing.T) {
	e := imgSetup(t)
	out, _, err := e.run("STUB_DOCKER_PULL_FAIL=1", "STUB_CRICTL_FAIL=1")
	if err == nil || !strings.Contains(out, "mongo:3.4") {
		t.Fatalf("want a failure that names the image, got %v: %s", err, out)
	}
}

// A local tag that was rebuilt (hotel's fuzzops/hotel:dev) is a different image under the same name: the nodes
// that hold the old one are loaded again, as `kind load` does.
func TestEnsureImagesReloadsAStaleLocalTag(t *testing.T) {
	e := imgSetup(t)
	e.hasID("n-w1", "old", mongo)
	e.hasID("n-w2", "new", mongo)
	out, log, err := e.run("STUB_HOST_HAS=new")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if !strings.Contains(log, "--nodes n-w1 mongo:3.4") || strings.Contains(log, "n-w2 mongo") || strings.Contains(log, "n-w1,n-w2") {
		t.Fatalf("want one kind load for n-w1 (stale), none for n-w2 (same image ID):\n%s", log)
	}
	if !e.nodeHolds("n-w1", mongo) {
		t.Fatal("n-w1 does not hold the image")
	}
}

func TestEnsureImagesSkipsANodeWhoseRepoDigestMatchesDocker(t *testing.T) {
	e := imgSetup(t)
	e.hasID("n-w1", "sha256:other docker.io/library/mongo@sha256:dd", mongo)
	e.hasID("n-w2", "sha256:other docker.io/library/mongo@sha256:dd", mongo)
	out, log, err := e.run("STUB_HOST_HAS=sha256:mine docker.io/library/mongo@sha256:dd")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if strings.Contains(log, "kind load") || strings.Contains(log, "crictl pull") {
		t.Fatalf("the same registry image (repo digest) was loaded again (docker's containerd store has another image ID):\n%s", log)
	}
}

func TestEnsureImagesTrustsADigestReference(t *testing.T) {
	const ref = "mongo@sha256:0123"
	e := imgSetup(t)
	e.hasID("n-w1", "x", "docker.io/library/"+ref)
	e.hasID("n-w2", "x", "docker.io/library/"+ref)
	out, log, err := e.runImages([]string{ref}, "STUB_HOST_HAS=different")
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	if strings.Contains(log, "kind load") || strings.Contains(log, "docker pull") || strings.Contains(log, "crictl pull") {
		t.Fatalf("a node that has the digest was touched:\n%s", log)
	}
	// and a node without it gets it (a failed load falls back to the node's pull)
	e2 := imgSetup(t)
	out, log, err = e2.runImages([]string{ref}, "STUB_DOCKER_PULL_FAIL=1")
	if err != nil || !e2.nodeHolds("n-w1", "docker.io/library/"+ref) {
		t.Fatalf("%v: %s\n%s", err, out, log)
	}
}

func TestEnsureImagesRefusesAnEmptyList(t *testing.T) {
	e := imgSetup(t)
	out, _, err := e.runImages(nil)
	if err == nil || !strings.Contains(out, "no images") || !strings.Contains(out, "lane") {
		t.Fatalf("an empty list must fail and name the cluster: %v: %s", err, out)
	}
}

// A locally built image is in no registry: when kind load cannot deliver it, the error names it.
func TestEnsureImagesNamesALocalImageItCannotDeliver(t *testing.T) {
	e := imgSetup(t)
	out, _, err := e.runImages([]string{"fuzzops/hotel:dev"}, "STUB_HOST_HAS=id", "STUB_LOAD_FAIL=1", "STUB_CRICTL_FAIL=1")
	if err == nil || !strings.Contains(out, "fuzzops/hotel:dev") {
		t.Fatalf("%v: %s", err, out)
	}
}

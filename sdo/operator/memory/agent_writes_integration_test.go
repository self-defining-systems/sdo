//go:build integration

package memory

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type recording struct {
	t     *testing.T
	root  string
	dir   string
	store *apihistory.Store
	now   time.Time
	next  int
}

// newRecording opens a live recording for the incident the way the sidecar does: the writer lock is held, so
// an unpaired start is an open request rather than a dead one.
func newRecording(t *testing.T, incident string) *recording {
	t.Helper()
	root := t.TempDir()
	relative, err := PrepareIncidentSession(root, "", incident)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, relative, "api-history")
	lock, err := apihistory.BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.Close() })
	store, err := apihistory.Open(dir, incident, apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return &recording{t: t, root: root, dir: dir, store: store, now: time.Now().UTC().Add(-time.Hour)}
}

type call struct {
	verb, group, version, resource, namespace, name, subresource string
	status                                                       int
	body                                                         string
	command                                                      []string
	container                                                    string
	open                                                         bool
}

func (r *recording) do(c call) {
	r.t.Helper()
	r.next++
	id := strings.Repeat("a", 8) + string(rune('a'+r.next))
	r.now = r.now.Add(time.Minute)
	start := apihistory.Event{RequestID: id, Phase: "start", Started: r.now, Verb: c.verb, Group: c.group, Version: c.version, Resource: c.resource, Namespace: c.namespace, Name: c.name, Subresource: c.subresource, Container: c.container, Command: c.command, Reserved: true}
	r.store.Record(start, nil)
	if c.open {
		return
	}
	end := start
	end.Phase, end.Status, end.Finished = "end", c.status, r.now.Add(time.Second)
	if c.subresource == "exec" || c.subresource == "attach" {
		end.SessionEnd = r.now.Add(30 * time.Second)
		end.Finished = r.now.Add(time.Second)
	}
	r.store.Record(end, []byte(c.body))
}

func (r *recording) writes(incident string) (kinds []string, pending bool, receipts []string, err error) {
	r.t.Helper()
	got, pending, _, err := AgentWrites(r.root, incident, time.Now().UTC())
	for _, receipt := range got {
		kinds = append(kinds, receipt.GetKind())
		target := receipt.GetTarget()
		receipts = append(receipts, target)
	}
	return kinds, pending, receipts, err
}

func TestAgentWritesDerivesARawPatchFromTheRecording(t *testing.T) {
	r := newRecording(t, "inc")
	r.do(call{verb: "patch", group: "apps", version: "v1", resource: "deployments", namespace: "app", name: "api", status: 200})
	r.do(call{verb: "get", version: "v1", resource: "pods", namespace: "app", status: 200})
	r.do(call{verb: "patch", group: "apps", version: "v1", resource: "deployments", namespace: "app", name: "front", status: 403})
	r.do(call{verb: "post", group: "authorization.k8s.io", version: "v1", resource: "selfsubjectaccessreviews", status: 201})
	r.store.Close()
	got, pending, _, err := AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || pending {
		t.Fatalf("err %v pending %v", err, pending)
	}
	if len(got) != 1 {
		t.Fatalf("only the successful persisting write is a write: %v", got)
	}
	w := got[0]
	if w.GetKind() != "api-patch" || !strings.HasPrefix(w.GetActionId(), "sdo-api-") || !w.GetSuccess() {
		t.Fatalf("receipt %v", w)
	}
	if len(w.GetResources()) != 1 || w.GetResources()[0].GetKind() != "Deployment" || w.GetResources()[0].GetApiVersion() != "apps/v1" || w.GetResources()[0].GetName() != "api" || w.GetResources()[0].GetNamespace() != "app" {
		t.Fatalf("the written object is named from the request: %v", w.GetResources())
	}
}

func TestAgentWritesCountsAnyObjectNoFindingNamesAndSecrets(t *testing.T) {
	r := newRecording(t, "inc")
	r.do(call{verb: "put", version: "v1", resource: "configmaps", namespace: "app", name: "settings", status: 200})
	r.do(call{verb: "delete", version: "v1", resource: "secrets", namespace: "app", name: "db-credentials", status: 200})
	r.store.Close()
	got, _, _, err := AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || len(got) != 2 {
		t.Fatalf("every persisting write is a receipt: %v %v", got, err)
	}
	if got[0].GetResources()[0].GetKind() != "ConfigMap" || got[1].GetResources()[0].GetKind() != "Secret" {
		t.Fatalf("kinds come from the resource: %v", got)
	}
}

func TestAgentWritesTreatsAnOpenExecAsPendingAndAnEndedOneAsAWrite(t *testing.T) {
	r := newRecording(t, "inc")
	r.do(call{verb: "get", version: "v1", resource: "pods", namespace: "app", name: "db-0", subresource: "exec", container: "db", command: []string{"psql", "<withheld>"}, open: true})
	t.Cleanup(r.store.Close)
	_, pending, _, err := AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || !pending {
		t.Fatalf("an open exec session keeps settlement pending: %v %v", pending, err)
	}
}

func TestAgentWritesRecordsAnEstablishedExecSession(t *testing.T) {
	r := newRecording(t, "inc")
	r.do(call{verb: "get", version: "v1", resource: "pods", namespace: "app", name: "db-0", subresource: "exec", container: "db", command: []string{"psql", "<withheld>"}, status: 101})
	r.store.Close()
	got, pending, _, err := AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || pending || len(got) != 1 {
		t.Fatalf("%v %v %v", got, pending, err)
	}
	w := got[0]
	if w.GetKind() != "api-exec" || w.GetResources()[0].GetKind() != "Pod" || !strings.Contains(w.GetDetails(), "exit status and effect not observed") || !strings.Contains(w.GetDetails(), "psql") {
		t.Fatalf("%v", w)
	}
	if !w.GetCompletedAt().AsTime().After(w.GetStartedAt().AsTime().Add(20 * time.Second)) {
		t.Fatalf("an exec spans its session: %v", w)
	}
}

func TestAgentWritesCountsAWriteGapAsAWriteAtItsTime(t *testing.T) {
	r := newRecording(t, "inc")
	r.do(call{verb: "get", version: "v1", resource: "pods", namespace: "app", status: 200, body: strings.Repeat(" ", apihistory.MaxProjectionBytes+1)})
	r.store.Close()
	got, _, _, err := AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || len(got) != 0 {
		t.Fatalf("a lost read body leaves no gap: %v %v", got, err)
	}
	// The recorder restarts: it may have lost a write while it was gone.
	restarted, err := apihistory.Open(r.dir, "inc", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Close()
	got, _, _, err = AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || len(got) != 1 || got[0].GetKind() != apihistory.GapKind || !got[0].GetSuccess() || len(got[0].GetResources()) != 0 {
		t.Fatalf("a gap is a write at its time, naming no object: %v %v", got, err)
	}
	if time.Since(got[0].GetCompletedAt().AsTime()) > time.Minute {
		t.Fatalf("a gap counts at the restart, not at the first write: %v", got[0])
	}
}

func TestAgentWritesMarksTheHelpersTheAgentCreates(t *testing.T) {
	r := newRecording(t, "inc")
	pod := `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"probe","namespace":"app"}}`
	r.do(call{verb: "post", version: "v1", resource: "pods", namespace: "app", status: 201, body: pod})
	r.do(call{verb: "delete", version: "v1", resource: "pods", namespace: "app", name: "probe", status: 200})
	r.do(call{verb: "delete", version: "v1", resource: "pods", namespace: "app", name: "api-7f9c", status: 200})
	r.do(call{verb: "patch", group: "apps", version: "v1", resource: "deployments", namespace: "app", name: "api", status: 200})
	r.store.Close()
	got, _, _, err := AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || len(got) != 4 {
		t.Fatalf("%v %v", got, err)
	}
	var kinds []string
	for _, w := range got {
		kinds = append(kinds, w.GetKind())
	}
	want := []string{"api-helper-create", "api-helper-delete", "api-delete", "api-patch"}
	if strings.Join(kinds, ",") != strings.Join(want, ",") {
		t.Fatalf("kinds %v want %v", kinds, want)
	}
	if got[0].GetResources()[0].GetName() != "probe" {
		t.Fatalf("a create is named from the projected response: %v", got[0].GetResources())
	}
}

func TestAgentWritesWhenTheRecorderClosedMidWriteIsAGapNotPending(t *testing.T) {
	root := t.TempDir()
	relative, err := PrepareIncidentSession(root, "", "inc")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, relative, "api-history")
	if err := apihistory.BeginGeneration(dir); err != nil {
		t.Fatal(err)
	}
	store, err := apihistory.Open(dir, "inc", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Add(-time.Minute)
	store.Record(apihistory.Event{RequestID: "deadwrite1", Phase: "start", Started: started, Verb: "patch", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app", Name: "api", Reserved: true}, nil)
	store.Close()
	got, pending, _, err := AgentWrites(root, "inc", time.Now().UTC())
	if err != nil || pending {
		t.Fatalf("no live recorder will ever finish this write: pending %v err %v", pending, err)
	}
	if len(got) != 1 || got[0].GetKind() != apihistory.GapKind || got[0].GetCompletedAt().AsTime().Before(started.Add(-time.Second)) {
		t.Fatalf("an unfinished write is a gap at its start: %v", got)
	}
}

func TestAgentWritesWithoutARecordingIsNoWrites(t *testing.T) {
	got, pending, _, err := AgentWrites(t.TempDir(), "inc", time.Now().UTC())
	if err != nil || pending || len(got) != 0 {
		t.Fatalf("%v %v %v", got, pending, err)
	}
}

func TestRecoveryReceiptsNeedNoLostWriteRecordsNotNoLostBodies(t *testing.T) {
	r := newRecording(t, "inc")
	api := &contracts.ObjectRef{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	r.do(call{verb: "patch", group: "apps", version: "v1", resource: "deployments", namespace: "app", name: "api", status: 200})
	// A watch stopped early and a 300 KB list lose bodies, never write records.
	r.do(call{verb: "get", version: "v1", resource: "pods", namespace: "app", status: 200, body: strings.Repeat(" ", apihistory.MaxProjectionBytes+1)})
	r.store.Close()
	since, until := r.now.Add(-time.Hour), time.Now().UTC()
	got, err := IncidentAPIRecoveryReceipts(r.root, "inc", []*contracts.ObjectRef{api}, since, until)
	if err != nil || len(got) != 1 {
		t.Fatalf("a lost read body made plan settlement unknown: %v %v", got, err)
	}
	restarted, err := apihistory.Open(r.dir, "inc", apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Close()
	if _, err := IncidentAPIRecoveryReceipts(r.root, "inc", []*contracts.ObjectRef{api}, since, time.Now().UTC()); err == nil {
		t.Fatal("a recorder restart may have lost a write: settlement must be unknown")
	}
}

func TestAnOpenPortForwardIsNotAWriteAndDoesNotHoldSettlement(t *testing.T) {
	r := newRecording(t, "inc")
	t.Cleanup(r.store.Close)
	r.do(call{verb: "post", version: "v1", resource: "pods", namespace: "app", name: "db-0", subresource: "portforward", open: true})
	got, pending, _, err := AgentWrites(r.root, "inc", time.Now().UTC())
	if err != nil || pending || len(got) != 0 {
		t.Fatalf("a tunnel is not a write: %v pending %v %v", got, pending, err)
	}
}

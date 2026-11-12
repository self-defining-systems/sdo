package memory

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// AgentWrites derives what the incident's agent changed in the cluster from the recording the proxy kept of
// every Kubernetes call it made. It is the one reader of those writes: settle detection, the closure guard,
// the outcome's repair records and reflection's change list read it, never the agent-writable command journal.
//
// A write is a paired, non-dry-run 2xx POST, PUT, PATCH or DELETE on any resource or subresource (except the
// review creates that persist nothing), or an established exec or attach session. Its receipt spans the request,
// or the session. A request that was forwarded but never finished is pending while the recorder is live, and a
// gap once it is not. A lost write record, however it was lost, is returned as a receipt of kind GapKind at the
// time of the loss: a gap counts as a write at its time, so settlement restarts there and a gap can delay
// recovery, never advance it. Lost bodies, oversized responses and interrupted streams are not gaps.
//
// Writes to an object the agent created in the incident carry the helper kind and bind to no cause. A
// recording that was never opened holds no writes: the proxy refuses a write it cannot record.
// The returned revision identifies the exact history that was read, for the closure guard.
func AgentWrites(repositoryMount, incidentID string, through time.Time) ([]*contracts.RepairActionReceipt, bool, string, error) {
	relative, err := PrepareIncidentSession(repositoryMount, "", incidentID)
	if err != nil {
		return nil, true, "", err
	}
	directory := filepath.Join(repositoryMount, relative, "api-history")
	if _, err := os.Stat(filepath.Join(directory, "manifest.json")); os.IsNotExist(err) {
		return nil, false, "", nil
	}
	history, err := apihistory.ReadThrough(context.Background(), directory, through)
	if err != nil {
		return nil, true, "", err
	}
	if history.Manifest.Incident != incidentID {
		return nil, true, "", fmt.Errorf("API history belongs to another incident")
	}
	receipts, pending := agentWriteReceipts(history, incidentID)
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%d/%d/%s/%d/%v", history.Manifest.Generation, history.Manifest.Epoch, history.Manifest.Events, history.Manifest.WriteGaps, history.Manifest.WriteGapAt.Format(time.RFC3339Nano), len(receipts), pending)))
	return receipts, pending, fmt.Sprintf("%x", hash), nil
}

type recordedRequest struct {
	start, end apihistory.Event
	hasEnd     bool
	duplicate  bool
}

func agentWriteReceipts(history apihistory.History, incident string) ([]*contracts.RepairActionReceipt, bool) {
	requests := map[string]*recordedRequest{}
	var order []string
	for _, event := range history.Events {
		if event.RequestID == "" || (event.Phase != "start" && event.Phase != "end") {
			continue
		}
		request := requests[event.RequestID]
		if request == nil {
			request = &recordedRequest{}
			requests[event.RequestID] = request
			order = append(order, event.RequestID)
		}
		if event.Phase == "start" {
			request.duplicate = request.duplicate || request.start.Phase != ""
			request.start = event
		} else {
			request.duplicate = request.duplicate || request.hasEnd
			request.end, request.hasEnd = event, true
		}
	}
	var result []*contracts.RepairActionReceipt
	pending := false
	gap := func(id string, at time.Time) {
		digest := sha256.Sum256([]byte(incident + "\x00" + history.Manifest.Generation + "\x00gap\x00" + id))
		result = append(result, &contracts.RepairActionReceipt{ActionId: fmt.Sprintf("%sgap-%x", apihistory.WriteIDPrefix, digest), Kind: apihistory.GapKind, Target: "api-history/gap", Summary: "a write record was lost", Details: "the recording lost a write record; it counts as a write at this time and recovery requires independent verification", StartedAt: timestamppb.New(at), CompletedAt: timestamppb.New(at), Success: true})
	}
	if history.Manifest.WriteGaps > 0 {
		at := history.Manifest.WriteGapAt
		if at.IsZero() {
			at = history.Manifest.Updated
		}
		gap(fmt.Sprintf("manifest/%d/%s", history.Manifest.WriteGaps, at.Format(time.RFC3339Nano)), at)
	}
	created := map[string]bool{}
	for _, id := range order {
		request := requests[id]
		if request.start.Phase == "" {
			if request.hasEnd && apihistory.WriteClass(request.end) {
				gap("orphan/"+id, request.end.Started)
			}
			continue
		}
		if !apihistory.WriteClass(request.start) {
			continue
		}
		switch {
		case request.duplicate:
			gap("duplicate/"+id, request.start.Started)
			continue
		case !request.hasEnd:
			// An open request is pending while its recorder is live; with no recorder left it is a write that
			// may have happened, at its start.
			if history.Live && history.Manifest.Open {
				pending = true
			} else {
				gap("unfinished/"+id, request.start.Started)
			}
			continue
		}
		start, end := request.start, request.end
		session := start.Subresource == "exec" || start.Subresource == "attach"
		switch {
		case end.DryRun || start.Epoch != end.Epoch || start.Sequence >= end.Sequence || !start.Started.Equal(end.Started) || start.Verb != end.Verb || start.Resource != end.Resource || start.Namespace != end.Namespace || start.Name != end.Name || start.Subresource != end.Subresource || end.Finished.Before(end.Started):
			gap("mismatched/"+id, start.Started)
			continue
		case session && end.Status != 101 && (end.Status < 200 || end.Status >= 300):
			continue
		case !session && (end.Status < 200 || end.Status >= 300):
			continue
		}
		completed := end.Finished
		if session && end.SessionEnd.After(completed) {
			completed = end.SessionEnd
		}
		receipt := writeReceipt(history, incident, start, end, completed, session)
		kind := strings.TrimPrefix(receipt.Kind, apihistory.WriteKindPrefix)
		if len(receipt.Resources) == 1 {
			key := objectKey(receipt.Resources[0].ApiVersion, receipt.Resources[0].Kind, receipt.Resources[0].Namespace, receipt.Resources[0].Name)
			if (start.Verb == "post" && start.Subresource == "" && !session) || created[key] {
				created[key] = true
				receipt.Kind = apihistory.HelperKindPrefix + kind
			}
		}
		result = append(result, receipt)
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := result[i].GetStartedAt().AsTime(), result[j].GetStartedAt().AsTime()
		if a.Equal(b) {
			return result[i].GetActionId() < result[j].GetActionId()
		}
		return a.Before(b)
	})
	return result, pending
}

func objectKey(apiVersion, kind, namespace, name string) string {
	return apiVersion + "\x00" + kind + "\x00" + namespace + "\x00" + name
}

func writeReceipt(history apihistory.History, incident string, start, end apihistory.Event, completed time.Time, session bool) *contracts.RepairActionReceipt {
	digest := sha256.Sum256([]byte(incident + "\x00" + history.Manifest.Generation + "\x00" + start.RequestID))
	apiVersion := start.Version
	if start.Group != "" {
		apiVersion = start.Group + "/" + start.Version
	}
	name, kind := start.Name, kindOfResource(start.Group, start.Version, start.Resource)
	if start.Verb == "post" && start.Subresource == "" {
		// A create learns its name, and for a kind the scheme does not know its kind, only from the
		// projected response.
		var object struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		}
		if end.BodyFormat == "projected_json" && json.Unmarshal(history.Body(end), &object) == nil && object.Metadata.Namespace == start.Namespace {
			name = object.Metadata.Name
			if kind == "" {
				kind = object.Kind
			}
		}
	}
	verb := map[string]string{"post": "create", "put": "put", "patch": "patch", "delete": "delete"}[start.Verb]
	summary := "agent Kubernetes API write"
	details := "recorded " + strings.TrimSuffix(start.Verb+" "+start.Subresource, " ") + " with API confirmation"
	if len(end.ChangedPaths) > 0 {
		details += "; changed " + strings.Join(end.ChangedPaths, ", ")
	}
	details += "; recovery requires independent verification"
	if session {
		verb = start.Subresource
		summary = "agent exec session"
		if start.Subresource == "attach" {
			summary = "agent attach session"
		}
		details = fmt.Sprintf("%s session; exit status and effect not observed; container %q; command %q", start.Subresource, start.Container, strings.Join(start.Command, " "))
	}
	target := start.Resource + "/" + name
	var resources []*contracts.ObjectRef
	if kind != "" && name != "" && !strings.HasPrefix(name, "opaque:") {
		resources = []*contracts.ObjectRef{{ApiVersion: apiVersion, Kind: kind, Namespace: start.Namespace, Name: name}}
		target = kind + "/" + name
	}
	if completed.Before(start.Started) {
		completed = start.Started
	}
	return &contracts.RepairActionReceipt{ActionId: fmt.Sprintf("%s%x", apihistory.WriteIDPrefix, digest), Kind: apihistory.WriteKindPrefix + verb, Target: target, Resources: resources, Summary: summary, Details: details, StartedAt: timestamppb.New(start.Started), CompletedAt: timestamppb.New(completed), Success: true}
}

var kindsByResource = sync.OnceValue(func() map[schema.GroupVersionResource]string {
	kinds := map[schema.GroupVersionResource]string{}
	for gvk := range scheme.Scheme.AllKnownTypes() {
		if gvk.Kind == "" || gvk.Version == "" || gvk.Version == "__internal" || strings.HasSuffix(gvk.Kind, "List") || strings.HasSuffix(gvk.Kind, "Options") {
			continue
		}
		plural, _ := meta.UnsafeGuessKindToResource(gvk)
		kinds[plural] = gvk.Kind
	}
	return kinds
})

// kindOfResource maps a recorded resource to its kind through the client-go scheme; empty for a resource it
// does not know (a custom resource), which is then named from a projected response or not at all.
func kindOfResource(group, version, resource string) string {
	return kindsByResource()[schema.GroupVersionResource{Group: group, Version: version, Resource: resource}]
}

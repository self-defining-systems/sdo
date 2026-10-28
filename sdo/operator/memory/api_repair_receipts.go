package memory

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// IncidentAPIRepairReceipts retains positive, paired API mutation observations
// even when shell commands bypass sdo act. It proves neither complete history
// nor recovery: independent finding clearance remains the publication gate.
// A hard-stopped writer may leave Open=true; durable complete request pairs
// remain facts, while dropped/stale generations never grant receipt authority.
func IncidentAPIRepairReceipts(root, incident string, resources []*contracts.ObjectRef, since, until time.Time) ([]*contracts.RepairActionReceipt, error) {
	sub, err := PrepareIncidentSession(root, "", incident)
	if err != nil {
		return nil, err
	}
	history, err := apihistory.Read(filepath.Join(root, sub, "api-history"))
	if err != nil {
		return nil, err
	}
	if history.Manifest.Incident != incident || history.Manifest.Dropped != 0 || uint64(len(history.Events)) != history.Manifest.Events || since.IsZero() || until.Before(since) {
		return nil, fmt.Errorf("API repair receipt evidence is incomplete, stale or outside the incident")
	}
	starts := map[string]apihistory.Event{}
	ends := map[string]apihistory.Event{}
	duplicates := map[string]bool{}
	for _, event := range history.Events {
		if event.RequestID == "" {
			continue
		}
		switch event.Phase {
		case "start":
			if _, ok := starts[event.RequestID]; ok {
				duplicates[event.RequestID] = true
			}
			starts[event.RequestID] = event
		case "end":
			if _, ok := ends[event.RequestID]; ok {
				duplicates[event.RequestID] = true
			}
			ends[event.RequestID] = event
		}
	}
	paired := func(event apihistory.Event) bool {
		start, ok := starts[event.RequestID]
		return ok && !duplicates[event.RequestID] && start.Sequence < event.Sequence && start.Epoch == event.Epoch && start.Started.Equal(event.Started) && start.Verb == event.Verb && start.Group == event.Group && start.Version == event.Version && start.Resource == event.Resource && start.Namespace == event.Namespace && start.Name == event.Name && start.Subresource == event.Subresource && start.DryRun == event.DryRun && !start.Partial && !event.Partial && !event.Started.Before(since) && !event.Finished.Before(event.Started) && !event.Finished.After(until)
	}
	var result []*contracts.RepairActionReceipt
	for _, event := range history.Events {
		if event.Phase != "end" || !paired(event) || event.DryRun || event.Resource == "secrets" || event.Status < 200 || event.Status >= 300 {
			continue
		}
		// Writes to the object itself, its scale or status, and evictions are
		// mutations. Exec/attach are not recorded here: the proxy sees the
		// session, never its command, so a read would look like a repair; sdo
		// act records those with their exit code.
		removal := event.Verb == "delete" || (event.Verb == "post" && event.Resource == "pods" && event.Subresource == "eviction")
		switch {
		case event.Subresource == "" && (event.Verb == "patch" || event.Verb == "put" || event.Verb == "post" || event.Verb == "delete"):
		case (event.Subresource == "scale" || event.Subresource == "status") && (event.Verb == "patch" || event.Verb == "put"):
		case removal && event.Subresource == "eviction":
		default:
			continue
		}
		for _, ref := range resources {
			if ref == nil || ref.GetKind() == "Secret" || ref.GetKind() == "" || ref.GetName() == "" || ref.GetApiVersion() == "" {
				continue
			}
			gv, err := schema.ParseGroupVersion(ref.GetApiVersion())
			if err != nil {
				continue
			}
			gvr, _ := meta.UnsafeGuessKindToResource(gv.WithKind(ref.GetKind()))
			if event.Group != gvr.Group || event.Version != gvr.Version || event.Resource != gvr.Resource || event.Namespace != ref.GetNamespace() {
				continue
			}
			// Named requests must target this exact object. Create obtains its name
			// only from the protected API response, never a declared shell scope.
			if event.Name != ref.GetName() && !(event.Verb == "post" && event.Name == "") {
				continue
			}
			confirmed := false
			completed := event.Finished
			if removal {
				for _, read := range history.Events {
					if read.Phase == "end" && read.Verb == "get" && read.Status == 404 && read.Subresource == "" && !read.DryRun && read.Group == event.Group && read.Version == event.Version && read.Resource == event.Resource && read.Namespace == event.Namespace && read.Name == event.Name && read.Started.After(event.Finished) && paired(read) {
						confirmed = true
						completed = read.Finished
						break
					}
				}
			} else if event.Verb != "post" {
				// A 2xx response to a named write is the API server's confirmation.
				// Response bodies of most kinds (webhooks, RBAC, Jobs, Scale) are
				// never projected, so they cannot be required.
				confirmed = true
			} else {
				var object struct {
					APIVersion string `json:"apiVersion"`
					Kind       string `json:"kind"`
					Metadata   struct {
						Name            string `json:"name"`
						Namespace       string `json:"namespace"`
						ResourceVersion string `json:"resourceVersion"`
					} `json:"metadata"`
				}
				confirmed = event.BodyFormat == "projected_json" && json.Unmarshal(history.Body(event), &object) == nil && object.APIVersion == ref.GetApiVersion() && object.Kind == ref.GetKind() && object.Metadata.Name == ref.GetName() && object.Metadata.Namespace == ref.GetNamespace() && object.Metadata.ResourceVersion != "" && !strings.HasPrefix(object.Metadata.ResourceVersion, "opaque:")
			}
			if !confirmed {
				continue
			}
			digest := sha256.Sum256([]byte(incident + "\x00" + history.Manifest.Generation + "\x00" + event.RequestID))
			result = append(result, &contracts.RepairActionReceipt{ActionId: fmt.Sprintf("sdo-api-%x", digest), Kind: "kubernetes-api", Target: ref.GetKind() + "/" + ref.GetName(), Resources: []*contracts.ObjectRef{{ApiVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()}}, Summary: "observed Kubernetes API mutation", Details: "controller-observed " + strings.TrimSuffix(event.Verb+" "+event.Subresource, " ") + " with API confirmation; recovery requires independent verification", StartedAt: timestamppb.New(event.Started), CompletedAt: timestamppb.New(completed), Success: true})
			break
		}
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i].GetStartedAt().AsTime(), result[j].GetStartedAt().AsTime()
		if a.Equal(b) {
			return result[i].GetActionId() < result[j].GetActionId()
		}
		return a.Before(b)
	})
	return result, nil
}

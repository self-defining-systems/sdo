package deployment

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

var _ Recreator = CommandBackend{}
var _ Pruner = CommandBackend{}
var _ ReplicasHandoff = CommandBackend{}

// ImmutableConflicts server-side dry-run applies documents with Apply's field
// manager and returns the objects the API server refuses for an immutable
// change. A dry run that fails for any other reason returns no conflicts:
// the apply that follows fails with the server's own answer.
func (b CommandBackend) ImmutableConflicts(ctx context.Context, documents []byte) ([]ImmutableConflict, error) {
	args := b.kubectlArgs("apply", "--server-side", "--force-conflicts", "--field-manager="+sdk.DeployerFieldManager, "--dry-run=server", "--filename=-")
	output, err := b.run(ctx, b.kubectl(), args, documents)
	if err == nil {
		return nil, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return ParseImmutableConflicts(string(output)), nil
}

// Get implements Recreator.
func (b CommandBackend) Get(ctx context.Context, ref sdk.ObjectRef) (LiveObject, bool, error) {
	args := b.kubectlArgs("get", ref.Kind, ref.Name, "--namespace", ref.Namespace, "--ignore-not-found", "--show-managed-fields", "--output=json")
	output, err := b.run(ctx, b.kubectl(), args, nil)
	if err != nil {
		return LiveObject{}, false, fmt.Errorf("get %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	if strings.TrimSpace(string(output)) == "" {
		return LiveObject{}, false, nil
	}
	var object map[string]any
	if err := json.Unmarshal(output, &object); err != nil {
		return LiveObject{}, false, fmt.Errorf("decode %s/%s: %w", ref.Kind, ref.Name, err)
	}
	return liveObject(object), true, nil
}

// AppliedJobs implements Pruner: the Jobs whose managed fields name the
// deployer's field manager as an applier and that carry its source-tree stamp.
func (b CommandBackend) AppliedJobs(ctx context.Context, namespace string) ([]LiveObject, error) {
	args := b.kubectlArgs("get", "jobs", "--namespace", namespace, "--show-managed-fields", "--output=json")
	output, err := b.run(ctx, b.kubectl(), args, nil)
	if err != nil {
		return nil, fmt.Errorf("list jobs in %s: %w: %s", namespace, err, strings.TrimSpace(string(output)))
	}
	var list map[string]any
	if err := json.Unmarshal(output, &list); err != nil {
		return nil, fmt.Errorf("decode jobs in %s: %w", namespace, err)
	}
	var out []LiveObject
	for _, raw := range objectSlice(list, "items") {
		item, _ := raw.(map[string]any)
		metadata := objectMap(item, "metadata")
		if objectString(objectMap(metadata, "annotations"), "sdo.dev/source-tree") == "" || !appliedByDeployer(metadata) {
			continue
		}
		out = append(out, liveObject(item))
	}
	return out, nil
}

func appliedByDeployer(metadata map[string]any) bool {
	for _, raw := range objectSlice(metadata, "managedFields") {
		entry, _ := raw.(map[string]any)
		if objectString(entry, "manager") == sdk.DeployerFieldManager && objectString(entry, "operation") == "Apply" {
			return true
		}
	}
	return false
}

// Dependents implements Recreator.
func (b CommandBackend) Dependents(ctx context.Context, ref sdk.ObjectRef, uid string) ([]LiveObject, error) {
	args := b.kubectlArgs("get", "replicasets,pods,controllerrevisions", "--namespace", ref.Namespace, "--output=json")
	output, err := b.run(ctx, b.kubectl(), args, nil)
	if err != nil {
		return nil, fmt.Errorf("list dependents of %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	var list map[string]any
	if err := json.Unmarshal(output, &list); err != nil {
		return nil, fmt.Errorf("decode dependents of %s/%s: %w", ref.Kind, ref.Name, err)
	}
	var out []LiveObject
	for _, raw := range objectSlice(list, "items") {
		item, _ := raw.(map[string]any)
		for _, rawOwner := range objectSlice(objectMap(item, "metadata"), "ownerReferences") {
			owner, _ := rawOwner.(map[string]any)
			if controller, _ := owner["controller"].(bool); controller && objectString(owner, "uid") == uid {
				out = append(out, liveObject(item))
			}
		}
	}
	return out, nil
}

// MarkOrphan implements Recreator: a merge patch of one label and two
// annotations.
func (b CommandBackend) MarkOrphan(ctx context.Context, ref sdk.ObjectRef, stamp OrphanStamp) error {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"labels":      map[string]string{LabelOrphanedFrom: stamp.From},
		"annotations": map[string]string{AnnotationOrphanedOwner: stamp.Owner, AnnotationOrphanedSelector: stamp.Selector},
	}})
	if err != nil {
		return err
	}
	args := b.kubectlArgs("patch", ref.Kind, ref.Name, "--namespace", ref.Namespace, "--type=merge", "--patch="+string(patch))
	if output, err := b.run(ctx, b.kubectl(), args, nil); err != nil {
		return fmt.Errorf("mark %s/%s orphaned: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	return nil
}

// Orphaned implements Recreator.
func (b CommandBackend) Orphaned(ctx context.Context, namespace string) ([]LiveObject, error) {
	args := b.kubectlArgs("get", "replicasets,pods,controllerrevisions", "--namespace", namespace, "--selector="+LabelOrphanedFrom, "--output=json")
	output, err := b.run(ctx, b.kubectl(), args, nil)
	if err != nil {
		return nil, fmt.Errorf("list orphaned dependents in %s: %w: %s", namespace, err, strings.TrimSpace(string(output)))
	}
	var list map[string]any
	if err := json.Unmarshal(output, &list); err != nil {
		return nil, fmt.Errorf("decode orphaned dependents in %s: %w", namespace, err)
	}
	var out []LiveObject
	for _, raw := range objectSlice(list, "items") {
		item, _ := raw.(map[string]any)
		if live := liveObject(item); live.Orphan != nil {
			out = append(out, live)
		}
	}
	return out, nil
}

// Delete implements Recreator: it waits until the object is gone.
func (b CommandBackend) Delete(ctx context.Context, ref sdk.ObjectRef, propagation Propagation) error {
	limit := b.ObserveLimit
	if limit <= 0 {
		limit = settings.DefaultRolloutTimeout
	}
	args := b.kubectlArgs("delete", ref.Kind, ref.Name, "--namespace", ref.Namespace, "--cascade="+strings.ToLower(string(propagation)),
		"--ignore-not-found", "--wait=true", "--timeout="+limit.String())
	if output, err := b.run(ctx, b.kubectl(), args, nil); err != nil {
		return fmt.Errorf("delete %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func liveObject(object map[string]any) LiveObject {
	metadata := objectMap(object, "metadata")
	live := LiveObject{
		Ref: sdk.ObjectRef{APIVersion: objectString(object, "apiVersion"), Kind: objectString(object, "kind"), Namespace: objectString(metadata, "namespace"), Name: objectString(metadata, "name")},
		UID: objectString(metadata, "uid"), Labels: map[string]string{},
	}
	for key, value := range objectMap(metadata, "labels") {
		if text, ok := value.(string); ok {
			live.Labels[key] = text
		}
	}
	for _, raw := range objectSlice(metadata, "ownerReferences") {
		owner, _ := raw.(map[string]any)
		if controller, _ := owner["controller"].(bool); controller {
			live.Controlled = true
		}
	}
	live.AppliedByDeployer = appliedByDeployer(metadata)
	if from := live.Labels[LabelOrphanedFrom]; from != "" {
		annotations := objectMap(metadata, "annotations")
		live.Orphan = &OrphanStamp{From: from, Owner: objectString(annotations, AnnotationOrphanedOwner), Selector: objectString(annotations, AnnotationOrphanedSelector)}
	}
	for _, raw := range objectSlice(objectMap(object, "status"), "conditions") {
		condition, _ := raw.(map[string]any)
		if kind := objectString(condition, "type"); (kind == "Complete" || kind == "Failed") && objectString(condition, "status") == "True" {
			live.Finished = true
		}
	}
	return live
}

// invalidObject matches the API server's invalid-object answer as kubectl
// prints it: `Job.batch "x" is invalid: ...` (or `The Job "x" is invalid:`).
var invalidObject = regexp.MustCompile(`([A-Z][A-Za-z0-9]*)(?:\.[a-z0-9][a-z0-9.-]*)? "([^"]+)" is invalid: `)

// invalidCause matches the start of one field cause in that answer.
var invalidCause = regexp.MustCompile(`(?:^|[\s\[*,])((?:spec|metadata|data|binaryData|stringData|type|immutable)(?:\.[A-Za-z0-9_\[\]-]+)*): (?:Invalid value|Forbidden|Unsupported value)`)

// immutableMarkers are the API server's words for a field that cannot change.
var immutableMarkers = []string{"is immutable", "updates to statefulset spec for fields other than", "may not change once set", "may not be changed"}

// ParseImmutableConflicts reads a server-side dry-run's output and returns the
// objects refused for an immutable change, with the fields the server named.
func ParseImmutableConflicts(output string) []ImmutableConflict {
	headers := invalidObject.FindAllStringSubmatchIndex(output, -1)
	var out []ImmutableConflict
	for i, header := range headers {
		end := len(output)
		if i+1 < len(headers) {
			end = headers[i+1][0]
		}
		body := output[header[1]:end]
		if line := strings.Index(body, "\nError from server"); line >= 0 {
			body = body[:line]
		}
		causes := invalidCause.FindAllStringSubmatchIndex(body, -1)
		var fields []string
		for j, cause := range causes {
			stop := len(body)
			if j+1 < len(causes) {
				stop = causes[j+1][0]
			}
			if immutableText(body[cause[0]:stop]) {
				fields = appendUnique(fields, body[cause[2]:cause[3]])
			}
		}
		if len(fields) == 0 {
			continue
		}
		conflict := ImmutableConflict{Ref: sdk.ObjectRef{Kind: output[header[2]:header[3]], Name: output[header[4]:header[5]]}, Fields: fields, Detail: strings.TrimSpace(clipText(output[header[0]:header[1]]+body, 400))}
		out = append(out, conflict)
	}
	return out
}

func immutableText(text string) bool {
	for _, marker := range immutableMarkers {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func clipText(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "..."
}

// HandOffReplicas implements ReplicasHandoff: when the deployer solely owns
// spec.replicas, the live count is applied under a neutral field manager.
func (b CommandBackend) HandOffReplicas(ctx context.Context, ref sdk.ObjectRef) error {
	args := b.kubectlArgs("get", ref.Kind, ref.Name, "--namespace", ref.Namespace, "--ignore-not-found", "--show-managed-fields", "--output=json")
	output, err := b.run(ctx, b.kubectl(), args, nil)
	if err != nil {
		return fmt.Errorf("get %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	if strings.TrimSpace(string(output)) == "" {
		return nil
	}
	var object map[string]any
	if err := json.Unmarshal(output, &object); err != nil {
		return fmt.Errorf("decode %s/%s: %w", ref.Kind, ref.Name, err)
	}
	live := &unstructured.Unstructured{Object: object}
	if !rendered.SolelyOwnsReplicas(live, sdk.DeployerFieldManager) {
		return nil
	}
	document, ok := rendered.ReplicasHandoff(live)
	if !ok {
		return nil
	}
	encoded, err := json.Marshal(document.Object)
	if err != nil {
		return err
	}
	args = b.kubectlArgs("apply", "--server-side", "--force-conflicts", "--field-manager="+rendered.ReplicasHandoffManager, "--filename=-")
	if output, err := b.run(ctx, b.kubectl(), args, encoded); err != nil {
		return fmt.Errorf("hand off spec.replicas of %s/%s: %w: %s", ref.Kind, ref.Name, err, strings.TrimSpace(string(output)))
	}
	return nil
}

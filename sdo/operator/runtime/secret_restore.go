package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PlanSecretRestoreClient is the private write boundary of a Secret restore:
// it sets the named keys of one Secret and never reads, returns or logs a
// value.
type PlanSecretRestoreClient interface {
	RestoreSecretData(ctx context.Context, ref sdk.ObjectRef, data map[string][]byte) error
}

// secretDataSource reads the literal data of a Secret in the accepted source.
// Its errors are typed (rendered.ErrSecretSourceNoLiteral, ErrSecretSourceNondeterministic)
// and carry no value.
type secretDataSource interface {
	AcceptedSecretData(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (map[string][]byte, error)
}

func generalSecretRestore(action program.Action) program.ReconcileFromSource {
	switch a := action.(type) {
	case program.ReconcileFromSource:
		return a
	case *program.ReconcileFromSource:
		return *a
	}
	return program.ReconcileFromSource{}
}

// RestoreSecretData writes data into the named keys of a Secret with one merge
// patch. It reads nothing and discards the response, which would carry the
// object. Other keys of the Secret are left as they are.
func (c TypedPlanClient) RestoreSecretData(ctx context.Context, ref sdk.ObjectRef, data map[string][]byte) error {
	if err := c.checkPlanNamespace(ref.Namespace); err != nil {
		return err
	}
	if c.Client == nil || ref.Kind != "Secret" || ref.APIVersion != "v1" {
		return errors.New("Secret restore requires a v1 Secret and the Kubernetes client")
	}
	encoded := make(map[string]string, len(data))
	for key, value := range data {
		encoded[key] = base64.StdEncoding.EncodeToString(value)
	}
	patch, err := json.Marshal(map[string]any{"data": encoded})
	if err != nil {
		return err
	}
	if _, err := c.Client.CoreV1().Secrets(ref.Namespace).Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("write Secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	return nil
}

// secretRestoreStep restores one Secret from the accepted source: the
// deployer's literal render of the accepted revision, written by the
// controller. No pre-image, patch, report or log holds a value; the report
// names the restored keys.
func (e PlanExecutor) secretRestoreStep(ctx context.Context, incident IncidentRequest, a program.ReconcileFromSource, dry bool) (PlanApplicationResult, error) {
	r := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{a}}, DryRun: dry}
	r.ActionReports = newPlanReports(r.Plan)
	switch {
	case incident.Namespace == "" || a.Ref.Namespace != incident.Namespace:
		return r, fmt.Errorf("Secret %s/%s is outside incident namespace %q", a.Ref.Namespace, a.Ref.Name, incident.Namespace)
	case incident.SourceCommit == "":
		return r, errors.New("accepted source is required to restore a Secret")
	case !dry && incident.RepairPolicy != "recorded-actions":
		return r, fmt.Errorf("repair policy %q denies direct plan mutations", incident.RepairPolicy)
	}
	source, ok := e.Source.(secretDataSource)
	if !ok {
		return r, errors.New("accepted Secret source unavailable")
	}
	client, ok := e.Client.(PlanSecretRestoreClient)
	if !ok {
		return r, errors.New("Secret restore transport unavailable")
	}
	data, err := source.AcceptedSecretData(ctx, incident.SourceCommit, incident.Namespace, a.Ref)
	if err != nil {
		return r, fmt.Errorf("restore Secret %s/%s from accepted source: %w", a.Ref.Namespace, a.Ref.Name, err)
	}
	if dry {
		return r, nil
	}
	err = client.RestoreSecretData(ctx, a.Ref, data)
	if err != nil {
		r.ActionReports[0].Status = "refused"
		return r, err
	}
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	r.Applied = true
	r.ActionsApplied = []program.Action{a}
	r.Patches = []PlanPatch{{Ref: a.Ref, Operation: "restore_secret", Irreversible: true}}
	r.ActionReports[0].Status = "applied"
	r.ActionReports[0].Observed = "restored keys: " + strings.Join(keys, ", ")
	return r, nil
}

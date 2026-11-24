package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// PlanSecretRestoreClient is the private write boundary of a Secret restore:
// it sets the named keys of one Secret and never reads, returns or logs a
// value.
type PlanSecretRestoreClient interface {
	RestoreSecretData(ctx context.Context, ref sdk.ObjectRef, version string, data map[string][]byte, dry bool) (bool, error)
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
// patch. It rechecks the comparison version and discards the response, which would carry the
// object. Other keys of the Secret are left as they are.
func (c TypedPlanClient) RestoreSecretData(ctx context.Context, ref sdk.ObjectRef, version string, data map[string][]byte, dry bool) (bool, error) {
	if err := c.checkPlanNamespace(ref.Namespace); err != nil {
		return false, err
	}
	if c.Client == nil || ref.Kind != "Secret" || ref.APIVersion != "v1" || version == "" {
		return false, errors.New("Secret restore requires a v1 Secret comparison")
	}
	secrets := c.Client.CoreV1().Secrets(ref.Namespace)
	live, err := secrets.Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return false, safeSecretAPIError(err)
	}
	if live.ResourceVersion != version {
		return false, errors.New("Secret changed after comparison")
	}
	encoded := make(map[string]string, len(data))
	for key, value := range data {
		prior, exists := live.Data[key]
		if !exists || !bytes.Equal(prior, value) {
			encoded[key] = base64.StdEncoding.EncodeToString(value)
		}
	}
	if dry || len(encoded) == 0 {
		return false, nil
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]string{"resourceVersion": version}, "data": encoded})
	if err != nil {
		return false, err
	}
	if _, err := secrets.Patch(ctx, ref.Name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: sdk.ResponderFieldManager}); err != nil {
		return false, safeSecretAPIError(err)
	}
	return true, nil
}

func safeSecretAPIError(err error) error {
	switch {
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return sdk.Classified("Secret operation refused", sdk.ErrRefused)
	case apierrors.IsConflict(err):
		return errors.New("Secret changed after comparison")
	default:
		return sdk.Classified("Secret operation failed", sdk.ErrUnavailable)
	}
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
	case incident.SourceCommit == "" || incident.SourceCommit != a.SecretCommit || len(a.Keys) == 0 || a.SecretResourceVersion == "":
		return r, errors.New("Secret restore requires compared keys and version at the incident's accepted commit")
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
		for _, refusal := range []error{rendered.ErrSecretSourceNoLiteral, rendered.ErrSecretSourceNondeterministic, rendered.ErrAcceptedManifestAbsent} {
			if errors.Is(err, refusal) {
				return r, refusal
			}
		}
		return r, fmt.Errorf("read accepted Secret %s/%s failed", a.Ref.Namespace, a.Ref.Name)
	}
	selected := make(map[string][]byte, len(a.Keys))
	for _, key := range a.Keys {
		value, ok := data[key]
		if !ok {
			return r, errors.New("compared Secret key missing from accepted source")
		}
		selected[key] = value
	}
	wrote, err := client.RestoreSecretData(ctx, a.Ref, a.SecretResourceVersion, selected, dry)
	if err != nil {
		r.ActionReports[0].Status = "refused"
		return r, err
	}
	if !wrote {
		return r, nil
	}
	keys := append([]string(nil), a.Keys...)
	sort.Strings(keys)
	r.Applied = true
	r.ActionsApplied = []program.Action{a}
	r.Patches = []PlanPatch{{Ref: a.Ref, Operation: "restore_secret", Irreversible: true}}
	r.ActionReports[0].Status = "applied"
	r.ActionReports[0].Observed = "restored keys: " + strings.Join(keys, ", ")
	return r, nil
}

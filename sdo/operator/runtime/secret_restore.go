package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Value-blind Secret restore and comparison. Declared values come only from
// rendered.Source.AcceptedSecretData and live values only from a direct
// Secret read below; both stay in Go memory. Receipts, findings, diffs and
// errors name keys (and, in a restore receipt, short value digests).

var errSecretRestoreWriteUnknown = errors.New("Secret restore write unconfirmed")

// secretDataSource resolves the literal data accepted source declares.
type secretDataSource interface {
	AcceptedSecretData(ctx context.Context, revision, namespace string, ref sdk.ObjectRef) (rendered.SecretData, error)
}

// PlanSecretRestoreClient is a private execution boundary: declared values go
// in, only keys and digests come out.
type PlanSecretRestoreClient interface {
	RestoreSecretFromSource(ctx context.Context, ref sdk.ObjectRef, declared rendered.SecretData, dry bool) (*contracts.SecretRestoreResult, error)
}

func (e PlanExecutor) secretRestoreStep(ctx context.Context, incident IncidentRequest, a program.RestoreSecretFromSource, dry bool) (PlanApplicationResult, error) {
	r := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{a}}, DryRun: dry}
	r.ActionReports = newPlanReports(r.Plan)
	if err := a.Validate(); err != nil {
		return r, err
	}
	if incident.Namespace == "" || a.Ref.Namespace != incident.Namespace {
		return r, fmt.Errorf("RestoreSecretFromSource requires a Secret in the incident namespace")
	}
	source, ok := e.Source.(secretDataSource)
	if !ok || incident.SourceCommit == "" {
		return r, fmt.Errorf("accepted source is required to restore a Secret")
	}
	client, ok := e.Client.(PlanSecretRestoreClient)
	if !ok {
		return r, fmt.Errorf("Secret restore transport unavailable")
	}
	declared, err := source.AcceptedSecretData(ctx, incident.SourceCommit, incident.Namespace, a.Ref)
	if err != nil {
		return r, err
	}
	report, err := client.RestoreSecretFromSource(ctx, a.Ref, declared, dry)
	if !dry && report != nil && (err == nil || errors.Is(err, errSecretRestoreWriteUnknown)) && (len(report.Restored)+len(report.Removed) > 0 || report.Created) {
		r.Applied = true
		r.ActionsApplied = []program.Action{a}
		// No Secret-bearing image is kept, so compensation cannot undo it.
		r.Patches = []PlanPatch{{Ref: a.Ref, Operation: "restore_secret_from_source", Authorization: planAuthorizationMode(a), Irreversible: true, SourceCommit: incident.SourceCommit}}
		r.ActionReports[0].Status = "unknown"
	}
	r.ActionReports[0].SecretRestore = report
	if err != nil {
		return r, err
	}
	if !dry {
		r.ActionReports[0].Status = "applied"
	}
	return r, nil
}

// RestoreSecretFromSource writes the declared data with an optimistic
// concurrency precondition, discards the response and reads back the written
// bytes. Labels, annotations and type of an existing Secret are kept.
func (c TypedPlanClient) RestoreSecretFromSource(ctx context.Context, ref sdk.ObjectRef, declared rendered.SecretData, dry bool) (*contracts.SecretRestoreResult, error) {
	if err := c.checkPlanNamespace(ref.Namespace); err != nil {
		return nil, err
	}
	if c.Client == nil {
		return nil, fmt.Errorf("Kubernetes plan client is required")
	}
	secrets := c.Client.CoreV1().Secrets(ref.Namespace)
	live, err := secrets.Get(ctx, ref.Name, metav1.GetOptions{})
	absent := apierrors.IsNotFound(err)
	if err != nil && !absent {
		return nil, fmt.Errorf("read Secret %s/%s: %w", ref.Namespace, ref.Name, secretReadError(err))
	}
	report := &contracts.SecretRestoreResult{Ref: planRefProto(ref), Keys: declared.Keys(), Digests: declared.Digests(), Created: absent}
	if absent {
		report.Restored = declared.Keys()
	} else {
		diff := declared.Compare(live.Data)
		report.Restored = append(append([]string(nil), diff.Differs...), diff.Missing...)
		sort.Strings(report.Restored)
		report.Removed = diff.Extra
	}
	if dry || len(report.Restored)+len(report.Removed) == 0 && !absent {
		return report, nil
	}
	if absent {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, Labels: declared.Labels}, Type: corev1.SecretType(declared.Type), Data: declared.Values()}
		_, err = secrets.Create(ctx, secret, metav1.CreateOptions{})
	} else {
		next := live.DeepCopy()
		next.Data, next.StringData = declared.Values(), nil
		_, err = secrets.Update(ctx, next, metav1.UpdateOptions{})
	}
	if err != nil {
		if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) || apierrors.IsForbidden(err) || apierrors.IsInvalid(err) {
			return nil, fmt.Errorf("restore Secret %s/%s refused: %s", ref.Namespace, ref.Name, apierrors.ReasonForError(err))
		}
		return report, errSecretRestoreWriteUnknown
	}
	observed, err := secrets.Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil || declared.Compare(observed.Data).Changed() {
		return report, fmt.Errorf("%w: readback mismatch or unavailable", errSecretRestoreWriteUnknown)
	}
	return report, nil
}

// secretReadError keeps only the API reason; a status message never carries
// Secret data, but the reason is all a caller needs.
func secretReadError(err error) error {
	if reason := apierrors.ReasonForError(err); reason != metav1.StatusReasonUnknown {
		return errors.New(string(reason))
	}
	return errors.New("API read failed")
}

// secretSourceDiff compares a live Secret's data with the literal data accepted
// source declares, inside Go. An absent live Secret is missing every key.
func secretSourceDiff(ctx context.Context, client kubernetes.Interface, source any, revision, namespace string, ref sdk.ObjectRef) (rendered.SecretKeyDiff, error) {
	reader, ok := source.(secretDataSource)
	if !ok || revision == "" {
		return rendered.SecretKeyDiff{}, errors.New("accepted source is unavailable")
	}
	if client == nil {
		return rendered.SecretKeyDiff{}, errors.New("Secret comparison needs the Kubernetes client")
	}
	declared, err := reader.AcceptedSecretData(ctx, revision, namespace, ref)
	if err != nil {
		return rendered.SecretKeyDiff{}, err
	}
	live, err := client.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return rendered.SecretKeyDiff{Missing: declared.Keys()}, nil
	}
	if err != nil {
		return rendered.SecretKeyDiff{}, fmt.Errorf("read Secret %s/%s: %w", ref.Namespace, ref.Name, secretReadError(err))
	}
	return declared.Compare(live.Data), nil
}

// secretDiffText is the key-only evidence of a Secret comparison.
func secretDiffText(ref sdk.ObjectRef, diff rendered.SecretKeyDiff) string {
	text := fmt.Sprintf("Secret %s/%s %s (values never shown)", ref.Namespace, ref.Name, diff)
	if diff.Changed() {
		text += "; RestoreSecretFromSource restores the declared data"
	}
	return text
}

// secretDriftEvent turns a Secret comparison into unexplained drift with one
// field per key, naming only the key and how it differs.
func secretDriftEvent(ref sdk.ObjectRef, diff rendered.SecretKeyDiff, at sdk.DriftEvent) *sdk.DriftEvent {
	if !diff.Changed() {
		return nil
	}
	event := at
	event.Ref = ref
	event.Source = "Secret data differs from accepted manifest; restore with RestoreSecretFromSource"
	event.Classification = sdk.DriftUnexplained
	for _, group := range []struct {
		keys  []string
		state string
	}{{diff.Differs, "value differs"}, {diff.Missing, "missing"}, {diff.Extra, "not declared"}} {
		for _, key := range group.keys {
			if len(event.Fields) < maxDriftFields {
				event.Fields = append(event.Fields, sdk.DriftField{Path: "data." + key, New: group.state})
			}
		}
	}
	return &event
}

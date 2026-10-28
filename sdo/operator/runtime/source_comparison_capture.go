package runtime

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// sourceComparisonCaptureBudget bounds one boundary's source comparisons.
const sourceComparisonCaptureBudget = 20 * time.Second

// sourceInventoryComparisons records, at one capture boundary, the accepted
// source comparison of every object a program may compare: each namespace
// object that is not controller-derived, and each object accepted source
// declares (a missing one compares as missing). Replay answers drift programs
// from these replies; a capture is projected (ConfigMap values redacted) and
// cannot recompute them. The context bounds the whole set.
func sourceInventoryComparisons(ctx context.Context, snapshot sdk.DetectionContext, view program.ReadView) []sdktest.PrimitiveCall {
	if snapshot == nil || view == nil {
		return nil
	}
	namespace := snapshot.Namespace()
	seen := map[sdk.ObjectRef]bool{}
	var refs []sdk.ObjectRef
	add := func(ref sdk.ObjectRef) {
		if !sourceComparable(ref) || seen[ref] {
			return
		}
		seen[ref] = true
		refs = append(refs, ref)
	}
	for _, o := range sdk.TypedObjects(snapshot) {
		if len(o.GetOwnerReferences()) > 0 || derivedObjectKind(o.GetKind()) {
			continue
		}
		add(sdk.ObjectIdentity(o))
	}
	if live, ok := routingLiveView(view); ok {
		if lister, ok := live.Source.(interface {
			AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error)
		}); ok {
			commit := live.SourceCommit
			if live.SourceRevision != nil {
				commit = live.SourceRevision()
			}
			objects, err := lister.AcceptedManifests(ctx, commit, namespace)
			if err != nil {
				slog.Warn("source comparison evidence gap", "namespace", namespace, "error", err)
			}
			for _, o := range objects {
				if o == nil {
					continue
				}
				ref := inventoryRef(o)
				if inventoryClusterKind(ref.Kind) {
					ref.Namespace = ""
				} else if ref.Namespace == "" {
					ref.Namespace = namespace
				}
				add(ref)
			}
		}
	}
	var calls []sdktest.PrimitiveCall
	for _, ref := range refs {
		if ctx.Err() != nil {
			slog.Warn("source comparison evidence gap", "namespace", namespace, "recorded", len(calls), "objects", len(refs), "error", ctx.Err())
			break
		}
		diff, err := view.DiffAgainstSource(ref)
		call, err := sourceComparisonCall(ref, diff, err)
		if err != nil {
			slog.Warn("source comparison evidence gap", "resource", ref.Kind+"/"+ref.Name, "error", err)
			continue
		}
		calls = append(calls, call)
	}
	return calls
}

// derivedObjectKind names objects controllers derive from declared ones;
// accepted source that declares one still has it compared.
func derivedObjectKind(kind string) bool {
	switch strings.ToLower(kind) {
	case "endpoints", "endpointslice", "controllerrevision", "lease", "replicaset":
		return true
	}
	return false
}

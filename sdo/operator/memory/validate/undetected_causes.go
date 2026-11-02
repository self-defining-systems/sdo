package validate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// configurationDigest intentionally excludes API bookkeeping/status. A restart
// counter, resourceVersion or readiness change is not a learned configuration
// repair. Secret objects never qualify, and only hashes cross the outcome edge.
func configurationDigest(snapshot sdktest.Snapshot, ref *contracts.ObjectRef) (string, string) {
	if strings.EqualFold(ref.GetKind(), "Secret") {
		return "", ""
	}
	identity := sdk.ObjectRef{APIVersion: ref.GetApiVersion(), Kind: ref.GetKind(), Namespace: ref.GetNamespace(), Name: ref.GetName()}
	if identity.APIVersion == "" {
		for _, object := range append(append([]unstructured.Unstructured{}, snapshot.Objects...), sdk.TypedObjects(snapshot)...) {
			observed := sdk.ObjectIdentity(object)
			if observed.Kind == identity.Kind && observed.Namespace == identity.Namespace && observed.Name == identity.Name {
				if identity.APIVersion != "" && identity.APIVersion != observed.APIVersion {
					return "", ""
				}
				identity.APIVersion = observed.APIVersion
			}
		}
	}
	object, err := snapshot.Object(identity)
	if err != nil || object == nil || object.GetUID() == "" {
		return "", ""
	}
	content := object.DeepCopy().Object
	delete(content, "status")
	metadata := map[string]any{"name": object.GetName(), "namespace": object.GetNamespace()}
	for _, key := range []string{"labels", "annotations", "finalizers", "ownerReferences"} {
		if value, found, _ := unstructured.NestedFieldCopy(content, "metadata", key); found {
			metadata[key] = value
		}
	}
	if annotations, ok := metadata["annotations"].(map[string]any); ok {
		delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
		if len(annotations) == 0 {
			delete(metadata, "annotations")
		}
	}
	content["metadata"] = metadata
	raw, err := json.Marshal(content)
	if err != nil {
		return "", ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), string(object.GetUID())
}

// CaptureUndetectedCauses is controller-only derivation over protected capture
// boundaries. It does not infer a root cause or claim application recovery.
// An original successful receipt and observed same-UID configuration change
// permit reflection to propose a detector/repair, subject to full corpus replay.
func CaptureUndetectedCauses(outcome *contracts.OutcomeRecord, capture string) []*contracts.UndetectedCause {
	if capture == "" {
		return nil
	}
	var fixed sdktest.Snapshot
	raw, err := os.ReadFile(filepath.Join(capture, "fixed.json"))
	if err != nil || json.Unmarshal(raw, &fixed) != nil || fixed.ObservedAt().IsZero() {
		return nil
	}
	var opens []sdktest.Snapshot
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		var s sdktest.Snapshot
		raw, err := os.ReadFile(filepath.Join(capture, phase+".json"))
		if err == nil && json.Unmarshal(raw, &s) == nil && !s.ObservedAt().IsZero() {
			opens = append(opens, s)
		}
	}
	seen := map[[3]string]bool{}
	var result []*contracts.UndetectedCause
	// Select the last successful receipt for each resource. Its fixed observation
	// must follow that receipt; an earlier successful intermediate is insufficient.
	for i := len(outcome.GetRepairActions()) - 1; i >= 0; i-- {
		action := outcome.GetRepairActions()[i]
		if !action.GetSuccess() || action.GetActionId() == "" || action.GetFailureReason() != "" || action.GetStartedAt() == nil || !action.GetStartedAt().IsValid() || action.GetCompletedAt() == nil || !action.GetCompletedAt().IsValid() || action.GetCompletedAt().AsTime().Before(action.GetStartedAt().AsTime()) {
			continue
		}
		for _, ref := range action.GetResources() {
			key := [3]string{ref.GetKind(), ref.GetNamespace(), ref.GetName()}
			if seen[key] {
				continue
			}
			seen[key] = true
			detected := slices.ContainsFunc(outcome.GetFindings(), func(f *contracts.Finding) bool {
				return equalCauseResource(f.GetPrimaryResource(), ref) || slices.ContainsFunc(f.GetRelatedResources(), func(other *contracts.ObjectRef) bool { return equalCauseResource(other, ref) })
			})
			for _, cause := range outcome.GetConfirmedRootCauses() {
				detected = detected || slices.ContainsFunc(cause.GetResources(), func(other *contracts.ObjectRef) bool { return equalCauseResource(other, ref) })
			}
			if detected || fixed.ObservedAt().Before(action.GetCompletedAt().AsTime()) {
				continue
			}
			after, uid := configurationDigest(fixed, ref)
			if after == "" {
				continue
			}
			var selected *sdktest.Snapshot
			for j := range opens {
				open := &opens[j]
				if !open.ObservedAt().After(action.GetStartedAt().AsTime()) && (selected == nil || open.ObservedAt().After(selected.ObservedAt())) {
					selected = open
				}
			}
			if selected == nil {
				continue
			}
			before, beforeUID := configurationDigest(*selected, ref)
			if before == "" || before == after || beforeUID != uid {
				continue
			}
			result = append(result, &contracts.UndetectedCause{Resource: ref, RepairActionId: action.GetActionId(), OpenDigest: before, FixedDigest: after, OpenObservedAt: timestamppb.New(selected.ObservedAt()), FixedObservedAt: timestamppb.New(fixed.ObservedAt())})
		}
	}
	slices.Reverse(result)
	return result
}

func undetectedCauseValid(outcome *contracts.OutcomeRecord, cause *contracts.UndetectedCause) bool {
	if strings.EqualFold(cause.GetResource().GetKind(), "Secret") || cause.GetResource().GetKind() == "" || cause.GetResource().GetName() == "" || cause.GetOpenDigest() == cause.GetFixedDigest() {
		return false
	}
	for _, digest := range []string{cause.GetOpenDigest(), cause.GetFixedDigest()} {
		raw, err := hex.DecodeString(digest)
		if err != nil || len(raw) != sha256.Size {
			return false
		}
	}
	if cause.GetOpenObservedAt() == nil || !cause.GetOpenObservedAt().IsValid() || cause.GetFixedObservedAt() == nil || !cause.GetFixedObservedAt().IsValid() {
		return false
	}
	for _, action := range outcome.GetRepairActions() {
		if action.GetActionId() == cause.GetRepairActionId() && action.GetSuccess() && action.GetFailureReason() == "" && action.GetStartedAt() != nil && action.GetStartedAt().IsValid() && action.GetCompletedAt() != nil && action.GetCompletedAt().IsValid() && !action.GetCompletedAt().AsTime().Before(action.GetStartedAt().AsTime()) && !cause.GetOpenObservedAt().AsTime().After(action.GetStartedAt().AsTime()) && !cause.GetFixedObservedAt().AsTime().Before(action.GetCompletedAt().AsTime()) && slices.ContainsFunc(action.GetResources(), func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, cause.GetResource()) }) {
			return true
		}
	}
	return false
}

// UndetectedCaseMatches binds authored replay inputs to the controller's measured
// configuration transition. Labels or a fixed-only fixture cannot grant authority.
func UndetectedCaseMatches(outcome *contracts.OutcomeRecord, c sdktest.CorpusCase, work *contracts.ReflectionCauseWork) bool {
	if work.GetLearningBasis() != "observed_transition" {
		return true
	}
	index := int(work.GetCauseIndex()) - len(outcome.GetConfirmedRootCauses())
	if index < 0 || index >= len(outcome.GetUndetectedCauses()) {
		return false
	}
	cause := outcome.UndetectedCauses[index]
	before, _ := configurationDigest(c.Open, cause.GetResource())
	after, _ := configurationDigest(c.Fixed, cause.GetResource())
	return before == cause.GetOpenDigest() && after == cause.GetFixedDigest()
}

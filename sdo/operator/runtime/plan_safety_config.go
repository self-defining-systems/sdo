package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"strings"
	"time"
)

// ReviewedPlanSafetyDocument is administrator-owned launch configuration.
// Its hash is pinned separately by the reviewer, never computed from a plan.
type ReviewedPlanSafetyDocument struct {
	Version         int                    `json:"version"`
	Authority       string                 `json:"authority"`
	ReviewID        string                 `json:"review_id"`
	SourceCommit    string                 `json:"source_commit"`
	ExpiresAt       time.Time              `json:"expires_at"`
	Objects         []ReviewedPlanObject   `json:"objects"`
	ServiceAccounts []ReviewedPlanIdentity `json:"service_accounts,omitempty"`
}
type ReviewedPlanObject struct {
	Ref           sdk.ObjectRef `json:"ref"`
	UID           string        `json:"uid"`
	Digests       []string      `json:"digests"`
	CreateAllowed bool          `json:"create_allowed,omitempty"`
	DeleteAllowed bool          `json:"delete_allowed,omitempty"`
}
type ReviewedPlanIdentity struct {
	Ref sdk.ObjectRef `json:"ref"`
	UID string        `json:"uid"`
}

func loadReviewedPlanSafety(raw, expected string, now time.Time) (*PlanSafetyPolicy, error) {
	if raw == "" && expected == "" {
		return nil, nil
	}
	sum := sha256.Sum256([]byte(raw))
	if len(raw) > 1024*1024 || expected != hex.EncodeToString(sum[:]) {
		return nil, fmt.Errorf("plan safety: reviewed policy hash mismatch or size limit")
	}
	var doc ReviewedPlanSafetyDocument
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("plan safety: invalid reviewed policy document")
	}
	if decoder.Decode(new(any)) != io.EOF || doc.Version != 1 || doc.Authority != "reviewed-fixture" || strings.TrimSpace(doc.ReviewID) == "" || doc.SourceCommit == "" || !now.Before(doc.ExpiresAt) || len(doc.Objects) == 0 {
		return nil, fmt.Errorf("plan safety: current reviewed-fixture authority required; plan/dry-run output is not authority")
	}
	p := &PlanSafetyPolicy{SourceCommit: doc.SourceCommit, ExpiresAt: doc.ExpiresAt, Objects: map[sdk.ObjectRef]PlanObjectSafety{}, ServiceAccounts: map[sdk.ObjectRef]string{}}
	for _, o := range doc.Objects {
		if err := validatePlanRef(o.Ref); err != nil {
			return nil, err
		}
		if _, exists := p.Objects[o.Ref]; exists || len(o.Digests) == 0 || (o.UID == "" && !o.CreateAllowed) || (o.DeleteAllowed && o.UID == "") {
			return nil, fmt.Errorf("plan safety: incomplete or duplicate object authorization")
		}
		digests := map[string]bool{}
		for _, d := range o.Digests {
			b, err := hex.DecodeString(d)
			if err != nil || len(b) != sha256.Size {
				return nil, fmt.Errorf("plan safety: invalid approved digest")
			}
			digests[d] = true
		}
		p.Objects[o.Ref] = PlanObjectSafety{UID: o.UID, Digests: digests, CreateAllowed: o.CreateAllowed, DeleteAllowed: o.DeleteAllowed}
	}
	for _, a := range doc.ServiceAccounts {
		if a.Ref.APIVersion != "v1" || a.Ref.Kind != "ServiceAccount" || a.Ref.Namespace == "" || a.Ref.Name == "" || a.UID == "" || p.ServiceAccounts[a.Ref] != "" {
			return nil, fmt.Errorf("plan safety: invalid reviewed identity")
		}
		p.ServiceAccounts[a.Ref] = a.UID
	}
	return p, nil
}

// ReviewedPlanObjectDigest hashes an independently reviewed nonsecret fixture.
// Never use proposed plan images or opaque current reads to create authority.
func ReviewedPlanObjectDigest(object *unstructured.Unstructured) (string, error) {
	if object == nil {
		return "", fmt.Errorf("plan safety: reviewed object required")
	}
	if err := planSecretPredicate(object); err != nil {
		return "", err
	}
	return planSafetyDigest(object), nil
}

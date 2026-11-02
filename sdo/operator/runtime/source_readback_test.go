package runtime

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

type sourceReadbackClient struct {
	PlanObjectClient
	writes      int
	unavailable bool
	mode        string
}

func (c *sourceReadbackClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.writes > 0 && c.unavailable && (c.mode == "read-error" || c.mode == "identity-change") {
		if c.mode == "read-error" {
			return nil, errors.New("synthetic readback outage")
		}
		object, err := c.PlanObjectClient.Get(ctx, ref)
		if err == nil {
			object.SetUID("replacement")
		}
		return object, err
	}
	return c.PlanObjectClient.Get(ctx, ref)
}

func (c *sourceReadbackClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	if err := c.PlanObjectClient.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	c.writes++
	return nil
}

// The other readback boundaries obey the same observation-only contract.
type sourceReadbackSource struct {
	PlanSource
	client *sourceReadbackClient
}

func (s sourceReadbackSource) AcceptedManifest(ctx context.Context, revision string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if s.client.writes > 0 && s.client.unavailable && s.client.mode == "source-error" {
		return nil, errors.New("synthetic source outage")
	}
	return s.PlanSource.AcceptedManifest(ctx, revision, ref)
}

type sourceReadbackRollouts struct {
	PlanRollouts
	client *sourceReadbackClient
}

func (r sourceReadbackRollouts) SourceRolloutResiduals(context.Context, *unstructured.Unstructured) ([]string, error) {
	if r.client.unavailable && r.client.mode == "rollout-read-error" {
		return nil, errors.New("synthetic rollout read outage")
	}
	return nil, nil
}

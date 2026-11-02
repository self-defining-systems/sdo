package runtime

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

type convergingSourceRollout struct {
	observations int
	persistent   bool
}

func (r *convergingSourceRollout) Wait(context.Context, []sdk.ObjectRef) error {
	r.observations++
	return nil
}

func (r *convergingSourceRollout) SourceRolloutResiduals(context.Context, *unstructured.Unstructured) ([]string, error) {
	r.observations++
	if r.persistent || r.observations < 4 {
		return []string{"rollout.pods[test/old]"}, nil
	}
	return nil, nil
}

type convergenceTestSource struct{ planFakeSource }

func (s convergenceTestSource) AcceptedManifest(ctx context.Context, commit string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, err := s.planFakeSource.AcceptedManifest(ctx, commit, ref)
	object.SetName(ref.Name)
	return object, err
}

type convergenceExecClient struct{ *lifecycleTestClient }

func (convergenceExecClient) Exec(context.Context, sdk.ObjectRef, string, []string) (int, error) {
	return 0, nil
}

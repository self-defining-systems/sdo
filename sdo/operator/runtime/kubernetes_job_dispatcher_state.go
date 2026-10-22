package runtime

import (
	"context"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// restoreToolState uses the same CAS ConfigMap state edge as the controller.
// A separate incident-scoped state document avoids racing observation-state
// saves while a responder Job uses tools. Only budgets and failure outcomes
// are restored before serving tools.
func (d KubernetesJobDispatcher) restoreToolState(ctx context.Context, request IncidentRequest, loop *ResponderLoop) error {
	if d.Client == nil {
		return nil
	}
	store := lifecycleio.NewConfigMapStateStore(d.Client, d.Namespace, IncidentJobName(request.IncidentID)+"-tools-state")
	state, revision, err := store.Load(ctx)
	if err != nil {
		return err
	}
	if len(state.GetResponderToolStateJson()) > 0 {
		if err := loop.restoreToolSnapshot(state.GetResponderToolStateJson()); err != nil {
			return err
		}
	}
	loop.saveToolState = func(ctx context.Context, payload []byte) error {
		next, err := store.Save(ctx, &contracts.ControllerState{Version: RuntimeStateVersion, ResponderToolStateJson: payload}, revision)
		if err != nil {
			return fmt.Errorf("save incident attempt facts: %w", err)
		}
		revision = next
		return nil
	}
	return nil
}

package runtime

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// restoreToolState uses the same CAS ConfigMap state edge as the controller.
// A separate incident-scoped state document avoids racing observation-state
// saves while a responder Job uses tools. Selection facts and provider identity
// survive source adoption; prepared plans and pre-images are never restored.
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
		document := &contracts.ControllerState{Version: RuntimeStateVersion}
		if state != nil {
			document = proto.Clone(state).(*contracts.ControllerState)
		}
		document.ResponderToolStateJson = payload
		next, err := store.Save(ctx, document, revision)
		if err != nil {
			return fmt.Errorf("save incident attempt facts: %w", err)
		}
		revision = next
		return nil
	}
	return nil
}

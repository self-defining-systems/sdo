package runtime

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// restoreToolState uses the same CAS ConfigMap state edge as the controller.
// A separate incident-scoped state document avoids racing observation-state
// saves while a responder Job uses tools. Counters, session presentation and menu
// reports survive a controller restart; prepared plans and pre-images are never restored.
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
		journal, err := unpackToolState(state.GetResponderToolStateJson())
		if err != nil {
			return err
		}
		if err := loop.restoreToolSnapshot(journal); err != nil {
			return err
		}
	}
	loop.operationMu.Lock()
	defer loop.operationMu.Unlock()
	loop.mu.Lock()
	defer loop.mu.Unlock()
	loop.saveToolState = func(ctx context.Context, payload []byte) error {
		document := &contracts.ControllerState{Version: RuntimeStateVersion}
		if state != nil {
			document = proto.Clone(state).(*contracts.ControllerState)
		}
		packed, err := packToolState(payload)
		if err != nil {
			return err
		}
		document.ResponderToolStateJson = packed
		next, err := store.Save(ctx, document, revision)
		if err != nil {
			return fmt.Errorf("save incident attempt facts: %w", err)
		}
		revision = next
		return nil
	}
	return nil
}

// packToolState gzips the attempt journal. Every plan application keeps its pre-images and patches, so a
// responder that repairs many resources grows it past the ConfigMap budget as plain JSON (base64 inside the
// state document); compressed it is a small fraction of that and the incident's tool calls keep persisting.
func packToolState(payload []byte) ([]byte, error) {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(payload); err != nil {
		return nil, fmt.Errorf("compress responder attempt state: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("compress responder attempt state: %w", err)
	}
	return buffer.Bytes(), nil
}

// unpackToolState reverses packToolState.
func unpackToolState(packed []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(packed))
	if err != nil {
		return nil, fmt.Errorf("open responder attempt state: %w", err)
	}
	defer func() { _ = reader.Close() }()
	payload, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("decompress responder attempt state: %w", err)
	}
	return payload, nil
}

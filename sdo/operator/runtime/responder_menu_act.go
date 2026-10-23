package runtime

import (
	"context"
	"crypto/rand"
)

// A process-local lease holds the existing mutation gateway while the worker's
// recorded act command runs. Missing completion never grants another write.
// Replacement workers remain frozen until their driver proves quiescence;
// receiving this token from an old worker is not such proof.
func (l *ResponderLoop) startMenuAct(ctx context.Context, response *ToolResponse) {
	if l.selection == nil || l.externalMutationToken != "" {
		response.Error = "mutation owner unavailable"
		return
	}
	select {
	case <-ctx.Done():
		response.Error = "mutation owner unavailable"
		return
	case <-l.mutationOwner.gate:
	default:
		response.Error = "mutation already in progress"
		return
	}
	if err := l.menuFence(ctx); err != nil {
		l.mutationOwner.gate <- struct{}{}
		response.Error = "mutation owner prohibits writes"
		return
	}
	if err := l.selection.chargeLegacy(ctx); err != nil {
		l.mutationOwner.gate <- struct{}{}
		response.Error = "incident mutation allowance exhausted"
		return
	}
	l.batches = int(l.selection.snapshot().UsedBatches + l.selection.snapshot().ReservedBatches)
	l.externalMutationToken = "act-" + rand.Text()
	response.MutationToken = l.externalMutationToken
	response.Summary = "recorded mutation owner acquired"
}
func (l *ResponderLoop) finishMenuAct(ctx context.Context, token string, response *ToolResponse) {
	if token == "" || token != l.externalMutationToken {
		response.Error = "unknown mutation owner"
		return
	}
	l.externalMutationToken = ""
	l.mutationOwner.gate <- struct{}{}
	if err := l.menuFence(ctx); err != nil {
		response.Error = "mutation owner expired or superseded"
		return
	}
	response.Summary = "recorded mutation owner released"
}

package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory/layout"
)

// AppendSDKGaps appends controller-recorded reflection annotations. It accepts
// no replacement incident evidence. The broker invokes it in its private clone
// and serializes publication; Put retains its immutable incident contract.
func (s *JSONStore) AppendSDKGaps(ctx context.Context, id string, gaps []*contracts.SdkGap) (Outcome, error) {
	return s.appendReflectionAnnotations(ctx, id, gaps, nil)
}

// AppendReflectionRejections records broker validation decisions without
// permitting replacement of classification, receipts or verification evidence.
func (s *JSONStore) AppendReflectionRejections(ctx context.Context, id string, rejections []*contracts.ProposalRejection) (Outcome, error) {
	return s.appendReflectionAnnotations(ctx, id, nil, rejections)
}

func (s *JSONStore) appendReflectionAnnotations(ctx context.Context, id string, gaps []*contracts.SdkGap, rejections []*contracts.ProposalRejection) (Outcome, error) {
	previous, err := s.Get(ctx, id)
	if err != nil {
		return Outcome{}, err
	}
	for _, gap := range gaps {
		if gap == nil {
			return Outcome{}, fmt.Errorf("nil SDK gap")
		}
	}
	for _, rejection := range rejections {
		if rejection == nil {
			return Outcome{}, fmt.Errorf("nil reflection rejection")
		}
	}
	// Agent prose is compared and stored in its logical form, so a retried
	// annotation is recognized and no later reflection reads a physical path.
	incoming := proto.Clone(previous.Record).(*contracts.OutcomeRecord)
	incoming.SdkGaps, incoming.ReflectionRejections = gaps, rejections
	if incoming, err = LogicalOutcome(incoming); err != nil {
		return Outcome{}, err
	}
	gaps, rejections = incoming.SdkGaps, incoming.ReflectionRejections
	next := proto.Clone(previous.Record).(*contracts.OutcomeRecord)
	for _, gap := range gaps {
		exists := false
		for _, old := range next.SdkGaps {
			exists = exists || proto.Equal(old, gap)
		}
		if !exists {
			next.SdkGaps = append(next.SdkGaps, proto.Clone(gap).(*contracts.SdkGap))
		}
	}
	for _, rejection := range rejections {
		exists := false
		for _, old := range next.ReflectionRejections {
			// A guard and a recovered result can observe the same rejection at
			// different times. Preserve the first durable timestamp.
			exists = exists || (old.GetAttempt() == rejection.Attempt && proto.Equal(&contracts.ProposalRejection{Diagnostics: old.Diagnostics}, &contracts.ProposalRejection{Diagnostics: rejection.Diagnostics}))
		}
		if !exists {
			next.ReflectionRejections = append(next.ReflectionRejections, proto.Clone(rejection).(*contracts.ProposalRejection))
		}
	}
	sort.SliceStable(next.SdkGaps, func(i, j int) bool {
		a, _ := wire.MarshalFull(next.SdkGaps[i])
		b, _ := wire.MarshalFull(next.SdkGaps[j])
		return string(a) < string(b)
	})
	encoded, err := wire.MarshalFull(next)
	if err != nil {
		return Outcome{}, err
	}
	// Preserve unknown historical fields and all immutable incident evidence.
	var document, updated map[string]json.RawMessage
	if err = json.Unmarshal(previous.document, &document); err != nil {
		return Outcome{}, err
	}
	if err = json.Unmarshal(encoded, &updated); err != nil {
		return Outcome{}, err
	}
	document["sdk_gaps"] = updated["sdk_gaps"]
	document["reflection_rejections"] = updated["reflection_rejections"]
	raw, err := json.Marshal(document)
	if err != nil {
		return Outcome{}, err
	}
	outcome, err := parse(raw)
	if err != nil {
		return Outcome{}, err
	}
	if proto.Equal(previous.Record, next) {
		return previous, nil
	}
	if err = ctx.Err(); err != nil {
		return Outcome{}, err
	}
	directory := filepath.Join(s.root, layout.OutcomesDir)
	if err = safeDirectory(directory); err != nil {
		return Outcome{}, err
	}
	file, err := os.CreateTemp(directory, ".sdk-gaps-*")
	if err != nil {
		return Outcome{}, err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(outcome.document); err != nil {
		file.Close()
		return Outcome{}, err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return Outcome{}, err
	}
	if err = file.Close(); err != nil {
		return Outcome{}, err
	}
	if err = os.Chmod(file.Name(), 0644); err != nil {
		return Outcome{}, err
	}
	if err = os.Rename(file.Name(), filepath.Join(s.root, layout.OutcomePath(id))); err != nil {
		return Outcome{}, err
	}
	return outcome, nil
}

// originalOutcomeRetry accepts replay of immutable evidence after annotation,
// retaining every appended gap and rejecting a replacement/addition via Put.
func originalOutcomeRetry(previous, requested *contracts.OutcomeRecord) bool {
	if previous == nil || requested == nil || (len(previous.GetSdkGaps()) == 0 && len(previous.GetReflectionRejections()) == 0) {
		return false
	}
	old := proto.Clone(previous).(*contracts.OutcomeRecord)
	next := proto.Clone(requested).(*contracts.OutcomeRecord)
	old.SdkGaps = nil
	next.SdkGaps = nil
	old.ReflectionRejections = nil
	next.ReflectionRejections = nil
	if !proto.Equal(old, next) {
		return false
	}
	for _, gap := range requested.SdkGaps {
		found := false
		for _, stored := range previous.SdkGaps {
			found = found || proto.Equal(gap, stored)
		}
		if !found {
			return false
		}
	}
	for _, rejection := range requested.ReflectionRejections {
		found := false
		for _, stored := range previous.ReflectionRejections {
			found = found || proto.Equal(rejection, stored)
		}
		if !found {
			return false
		}
	}
	return true
}

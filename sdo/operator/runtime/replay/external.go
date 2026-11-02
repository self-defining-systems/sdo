package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/types"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func operationKey(o program.ExternalOperation) string { raw, _ := json.Marshal(o); return string(raw) }
func (c *cluster) seedExternal(calls []sdktest.PrimitiveCall) error {
	c.operationState = map[string]int32{}
	c.effectUsed = map[int]bool{}
	c.externalCalls = append([]sdktest.PrimitiveCall(nil), calls...)
	for _, call := range calls {
		if call.Error != "" {
			continue
		}
		if call.Method == "Observe" {
			var args []program.ExternalOperation
			var code int32
			if json.Unmarshal(call.Arguments, &args) != nil || len(args) != 1 || args[0].Validate() != nil || json.Unmarshal(call.Result, &code) != nil {
				return fmt.Errorf("invalid external observation recording")
			}
			key := operationKey(args[0])
			if prior, ok := c.operationState[key]; ok && prior != code {
				return fmt.Errorf("conflicting open external observations")
			}
			c.operationState[key] = code
		}
	}
	for _, call := range calls {
		if call.Method != "ExternalEffect" || call.Error != "" {
			continue
		}
		var effect program.ExternalEffect
		var args []program.ExternalOperation
		if json.Unmarshal(call.Result, &effect) != nil || effect.Observe.Validate() != nil || effect.Ensure.Validate() != nil || json.Unmarshal(call.Arguments, &args) != nil || len(args) != 2 {
			return fmt.Errorf("invalid external effect recording")
		}
		if operationKey(args[0]) != operationKey(effect.Observe) || operationKey(args[1]) != operationKey(effect.Ensure) {
			return fmt.Errorf("external effect identity differs from its recording")
		}
		c.externalEffects = append(c.externalEffects, effect)
		key := operationKey(effect.Observe)
		if _, ok := c.operationState[key]; !ok {
			c.operationState[key] = effect.Before
		}
	}
	return nil
}
func (c *cluster) RunOperation(ctx context.Context, o program.ExternalOperation) (int32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := o.Validate(); err != nil {
		return 0, err
	}
	if _, err := c.Get(ctx, o.Pod()); err != nil {
		return 0, err
	}
	key := operationKey(o)
	if code, ok := c.operationState[key]; ok {
		return code, nil
	}

	return 0, fmt.Errorf("external operation has no recorded observation; ensures require the full pair")
}
func (c *cluster) EnsureOperation(ctx context.Context, observe, ensure program.ExternalOperation) (int32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := observe.Validate(); err != nil {
		return 0, err
	}
	if err := ensure.Validate(); err != nil {
		return 0, err
	}
	for i, effect := range c.externalEffects {
		if c.effectUsed[i] || operationKey(effect.Ensure) != operationKey(ensure) || operationKey(effect.Observe) != operationKey(observe) {
			continue
		}
		if before, ok := c.operationState[operationKey(observe)]; !ok || before != effect.Before {
			continue
		}
		c.effectUsed[i] = true
		c.operationState[operationKey(observe)] = effect.After
		return effect.ExitCode, nil
	}
	return 0, fmt.Errorf("external observe/ensure pair has no matching recorded transition")
}

func (v *readView) Observe(o program.ExternalOperation) (int32, error) {
	if v.external != nil {
		return v.external.RunOperation(v.ctx, o)
	}
	// Seed from measured open observations and effects, never fixed recordings.
	c := &cluster{}
	if err := c.seedExternal(v.Calls); err != nil {
		return 0, err
	}
	if code, ok := c.operationState[operationKey(o)]; ok {
		return code, nil
	}
	return v.ReplayReadView.Observe(o)
}
func (c *cluster) WaitJob(ctx context.Context, job program.RunJob) error {
	args, _ := json.Marshal([]any{job})
	for i, call := range c.externalCalls {
		if call.Method != "JobCompletion" || c.jobCallsUsed[i] || !sameRecordedJSON(call.Arguments, args) {
			continue
		}
		if call.Error != "" {
			return fmt.Errorf("recorded job completion failed")
		}
		var status map[string]any
		if err := json.Unmarshal(call.Result, &status); err != nil {
			return fmt.Errorf("job completion status unavailable")
		}
		raw, _ := json.Marshal(map[string]any{"status": status})
		if err := c.Patch(ctx, job.Object.Ref, types.MergePatchType, raw); err != nil {
			return err
		}
		object, err := c.Get(ctx, job.Object.Ref)
		if err != nil {
			return err
		}
		actual, err := program.FieldValue(object.Object, job.Completion.Path)
		want, _ := json.Marshal(job.Completion.Equals)
		value, _ := json.Marshal(actual)
		if err != nil || string(want) != string(value) {
			return fmt.Errorf("recorded job did not complete")
		}
		if c.jobCallsUsed == nil {
			c.jobCallsUsed = map[int]bool{}
		}
		c.jobCallsUsed[i] = true
		if c.simStatus == nil {
			c.simStatus = map[sdk.ObjectRef]map[string]any{}
		}
		c.simStatus[job.Object.Ref] = status
		return nil
	}
	return fmt.Errorf("job has no exact recorded completion")
}
func sameRecordedJSON(a, b []byte) bool {
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	x, _ := json.Marshal(left)
	y, _ := json.Marshal(right)
	return string(x) == string(y)
}

var _ operator.PlanOperationClient = (*cluster)(nil)
var _ operator.PlanJobClient = (*cluster)(nil)

package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// appendEvidenceGaps checks presence, not primitive success or causal relevance.
// Open recordings belong to the controller's protected pre-mutation boundaries;
// fixed calls carry the explicit fixed phase. Never infer timing from file mtimes.
func appendEvidenceGaps(work []*contracts.ReflectionCauseWork, capture string) uint32 {
	observed := map[string][]sdk.ObjectRef{}
	files, _ := filepath.Glob(filepath.Join(capture, "open@*-recordings.json"))
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		var calls []sdktest.PrimitiveCall
		if json.Unmarshal(raw, &calls) != nil {
			continue
		}
		for _, call := range calls {
			phase := call.Phase
			if phase == "" {
				phase = "open"
			}
			if phase != "open" && phase != "fixed" {
				continue
			}
			var arguments []json.RawMessage
			if json.Unmarshal(call.Arguments, &arguments) != nil || len(arguments) == 0 {
				continue
			}
			var ref sdk.ObjectRef
			if json.Unmarshal(arguments[0], &ref) != nil || ref.Kind == "" || ref.Name == "" {
				continue
			}
			observed[phase] = append(observed[phase], ref)
		}
	}
	distinct := map[string]bool{}
	for _, cause := range work {
		seen := map[string]bool{}
		for _, resource := range cause.Resources {
			for _, phase := range []string{"open", "fixed"} {
				found := false
				for _, ref := range observed[phase] {
					found = found || sameResource(ref, objectRefState(resource))
				}
				if found {
					continue
				}
				key, _ := json.Marshal([]string{resource.GetKind(), resource.GetNamespace(), resource.GetName(), phase})
				if seen[string(key)] {
					continue
				}
				seen[string(key)] = true
				cause.Items = append(cause.Items, &contracts.ReflectionWorkItem{
					Disposition: "question", Reason: "evidence_gap", EvidencePhase: phase,
					Resources: []*contracts.ObjectRef{proto.Clone(resource).(*contracts.ObjectRef)},
					Question:  "Go found no recorded observation of this resource at the named phase. Assess whether this gap blocks learning.",
				})
				distinct[string(key)] = true
			}
		}
	}
	return uint32(len(distinct))
}

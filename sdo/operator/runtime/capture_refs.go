package runtime

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func findingCaptureRefs(findings []sdk.Finding) []sdk.ObjectRef {
	var refs []sdk.ObjectRef
	for _, finding := range findings {
		refs = append(refs, finding.PrimaryResource)
		refs = append(refs, finding.RelatedResources...)
	}
	return refs
}

func planCaptureRefs(plan program.Plan) []sdk.ObjectRef {
	var refs []sdk.ObjectRef
	for _, action := range plan.Actions {
		if ref, err := planActionRef(action); err == nil {
			refs = append(refs, ref)
		}
	}
	return refs
}

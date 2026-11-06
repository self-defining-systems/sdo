package runtime

import (
	"errors"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"sdo.dev/operator/sdk"
)

// A Job whose template changed is refused as immutable by the API server; the
// deployer recreates it, so adoption must not report it as a failure each poll.
func TestAdoptionLeavesAnImmutableJobTemplateToTheDeployer(t *testing.T) {
	invalid := apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "seed", field.ErrorList{
		field.Invalid(field.NewPath("spec", "template"), "{}", "field is immutable"),
	})
	job := sdk.ObjectRef{Kind: "Job", Name: "seed"}
	if !jobTemplateImmutable(job, invalid) {
		t.Fatal("an immutable Job template is the deployer's to recreate")
	}
	if jobTemplateImmutable(sdk.ObjectRef{Kind: "Service", Name: "web"}, invalid) {
		t.Fatal("only Jobs are recreated")
	}
	other := apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "seed", field.ErrorList{field.Required(field.NewPath("spec", "template"), "required")})
	if jobTemplateImmutable(job, other) || jobTemplateImmutable(job, errors.New("immutable")) {
		t.Fatal("other refusals stay failures")
	}
}

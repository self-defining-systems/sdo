package broker

import (
	"context"
	"testing"
)

type validationJobFunc func(context.Context, ValidationJobRequest) (ValidationJobReceipt, error)

func (f validationJobFunc) Validate(ctx context.Context, request ValidationJobRequest) (ValidationJobReceipt, error) {
	return f(ctx, request)
}

func TestJobValidatorRequiresTreeBoundIsolationReceipt(t *testing.T) {
	root := repository(t)
	requestSeen := false
	validator := JobValidator(validationJobFunc(func(_ context.Context, request ValidationJobRequest) (ValidationJobReceipt, error) {
		requestSeen = true
		if !request.ReadOnly || !request.NoPublishingCredentials || !request.EgressIsolated || request.CandidateTree == "" {
			t.Fatalf("unsafe request: %+v", request)
		}
		return ValidationJobReceipt{CandidateTree: request.CandidateTree, Accepted: true, DetectorCompiled: true, SourceValidated: true, ReadOnlyMountObserved: true, NoPublishingCredentialsObserved: true}, nil
	}))
	if err := validator(context.Background(), root, []string{"app.txt"}); err == nil {
		t.Fatal("accepted receipt without egress canary")
	}
	if !requestSeen {
		t.Fatal("job was not invoked")
	}
	if err := JobValidator(nil)(context.Background(), root, nil); err == nil {
		t.Fatal("nil job accepted")
	}
}

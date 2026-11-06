package engine_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chartutil"

	"sdo.dev/operator/render/helm/internal/engine"
	"sdo.dev/operator/render/helm/internal/policy"
)

func fanOutChart() *chart.Chart {
	tpl := ""
	for i := 0; i < 40; i++ {
		tpl += fmt.Sprintf(`{{ define "h%d" }}{{ include "h%d" . }}{{ include "h%d" . }}{{ end }}`, i, i+1, i+1)
	}
	tpl += `{{ define "h40" }}x{{ end }}`
	return &chart.Chart{
		Metadata:  &chart.Metadata{Name: "c", Version: "0.1.0", APIVersion: "v2"},
		Templates: []*chart.File{{Name: "templates/_h.tpl", Data: []byte(tpl)}, {Name: "templates/x.yaml", Data: []byte(`{{ include "h0" . }}`)}},
	}
}

// The deadline is checked on every function call, and a typed cause set with
// WithTimeoutCause comes back unchanged.
func TestDeadlineStopsAFanOutWithItsTypedCause(t *testing.T) {
	cause := &policy.Error{Rule: policy.RuleRenderBudget, Detail: "too slow"}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 200*time.Millisecond, cause)
	defer cancel()
	_, err := engine.Engine{Context: ctx}.Render(fanOutChart(), chartutil.Values{})
	if !errors.Is(err, cause) && !policy.Violates(err, policy.RuleRenderBudget) {
		t.Fatalf("got %v", err)
	}
}

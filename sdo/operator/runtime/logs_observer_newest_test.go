package runtime

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
)

// The access-log samples of a busy container must come from the newest lines
// of the window, and a long structured line must keep its path.
func TestRequestSamplesComeFromTheNewestLinesAndKeepTheirPath(t *testing.T) {
	now := time.Unix(10000, 0).UTC()
	var all []string
	for i := 0; i < 6000; i++ {
		at := now.Add(-time.Minute + time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano)
		path := "/old/" + fmt.Sprint(i)
		if i >= 5990 {
			path = "/api/v4/projects/7/packages/generic/pkg/1.0/file.bin"
		}
		// The path follows 400 bytes of other fields, past the excerpt bound.
		all = append(all, fmt.Sprintf(`%s {"time":"%s","pad":"%s","method":"GET","path":"%s","status":404}`, at, at, strings.Repeat("x", 400), path))
	}
	q := requestmetrics.NewDetector(requestmetrics.Config{Entries: []requestmetrics.Entry{{Service: "api", Metric: "m"}}}).(interface{ LogQueries() []logs.Query }).LogQueries()
	o := &LogsObserver{queries: q, patterns: map[string]*regexp.Regexp{}, clock: func() time.Time { return now },
		states: map[string]*logState{}, read: kubeLogs(all)}
	for _, p := range q[0].Patterns {
		o.patterns[p.RE2] = regexp.MustCompile(p.RE2)
	}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api-0", Namespace: "app", UID: "u1"},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	base := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}
	ctx := inputs.New(base).WithLogs(logResults(o.collect(context.Background(), base)))
	res, ok := logs.ResultFrom(ctx, requestmetrics.LogQueryID)
	if !ok || res.Error != "" || len(res.Windows) != 1 || len(res.Windows[0].Cycles) == 0 {
		t.Fatalf("result: %+v", res)
	}
	found := false
	for _, m := range res.Windows[0].Cycles[0].Matches {
		for _, excerpt := range m.Excerpts {
			sample, ok := requestmetrics.ParseFailingLine(excerpt)
			if !ok {
				t.Fatalf("excerpt lost its request fields: %q", excerpt)
			}
			if sample.Path != "/api/v4/projects/7/packages/generic/pkg/1.0/file.bin" {
				t.Fatalf("excerpt is not from the newest lines: %q", excerpt)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no excerpts")
	}
}

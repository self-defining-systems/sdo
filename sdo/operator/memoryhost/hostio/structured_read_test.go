package hostio

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type storeStdout string

func (s storeStdout) Stream(_ context.Context, _ sdk.ObjectRef, _ string, _ []string, _ string, out io.Writer) error {
	_, err := io.WriteString(out, string(s))
	return err
}

type rowMitigator struct{ query program.Query }

func (rowMitigator) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "restore-setting"} }

func (m rowMitigator) Mitigate(_ context.Context, view program.ReadView, _ program.Bindings) (program.Plan, error) {
	observation, err := view.Read(m.query)
	if err != nil {
		return program.Plan{}, err
	}
	row := observation.Rows[0]
	if id, ok := row.Int("id"); !ok || id != math.MaxInt64 {
		return program.Plan{}, nil
	}
	return program.Plan{Actions: []program.Action{program.RepairData{
		Target: m.query.Store.Target, Scope: m.query.Store.Scope,
		Statement: "SET enabled = true", ExpectMin: 1, ExpectMax: 1,
	}}}, nil
}

func TestStoreScalarsSurviveLiveProgramReadsAndCorpusReplay(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "app", UID: "db-uid",
			OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db", Controller: new(true)}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "db", Image: "postgres:16"}}},
	}
	m, err := ApplicationMapFromYAML("components:\n  - name: db\n    workload: StatefulSet/db\n    holds_data: true\n    data: {engine: postgres, role: authoritative}\n")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{pod}}
	host := NewLiveReadView(t.Context(), snapshot, ReadBackends{
		Client: fake.NewSimpleClientset(&pod), ApplicationMap: m,
		Exec: storeStdout("sdo-count 1\n" +
			`{"id":9223372036854775807,"floor":-9223372036854775808,"zero":0,"ratio":1.25,"whole":7.0,"large":1e21,"label":"001","enabled":false,"empty":null}` +
			"\n#sdo-end 1\n"),
	})
	q := program.Query{Store: &program.StoreQuery{
		Target: program.StoreTarget{Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db"},
		Scope:  program.DataScope{Collection: "items", Selector: "id = 9223372036854775807", Limit: 1},
		Fields: []string{"id", "floor", "zero", "ratio", "whole", "large", "label", "enabled", "empty", "missing"},
	}}
	wire := &programread.View{Exchange: func(call programread.Message) (programread.Message, error) {
		return programread.Read(host, call), nil
	}}
	recorded := &sdktest.RecordingReadView{ReadView: wire}
	for _, stage := range []struct {
		name string
		read func(program.Query) (program.Observation, error)
	}{
		{"live", host.Read},
		{"wire", wire.Read},
		{"recorded", recorded.Read},
		{"corpus", func(q program.Query) (program.Observation, error) {
			raw, err := json.Marshal(recorded.Calls)
			if err != nil {
				return program.Observation{}, err
			}
			replay := snapshot
			if err := json.Unmarshal(raw, &replay.Readings); err != nil {
				return program.Observation{}, err
			}
			return replay.Read(q)
		}},
	} {
		t.Run(stage.name, func(t *testing.T) {
			observation, err := stage.read(q)
			if err != nil || observation.Count != 1 || len(observation.Rows) != 1 {
				t.Fatalf("read = %+v, %v", observation, err)
			}
			row := observation.Rows[0]
			for key, want := range map[string]int64{"id": math.MaxInt64, "floor": math.MinInt64, "zero": 0, "whole": 7} {
				if got, ok := row.Int(key); !ok || got != want {
					t.Errorf("Int(%q) = %d, %t, want %d (stored as %T)", key, got, ok, want, row[key])
				}
			}
			for key, want := range map[string]float64{"ratio": 1.25, "large": 1e21, "whole": 7} {
				if got, ok := row.Float(key); !ok || got != want {
					t.Errorf("Float(%q) = %g, %t, want %g", key, got, ok, want)
				}
			}
			if _, ok := row.Int("ratio"); ok {
				t.Error("fraction read as an integer")
			}
			if got, ok := row.String("label"); !ok || got != "001" {
				t.Errorf("String(label) = %q, %t", got, ok)
			}
			if got, ok := row.Bool("enabled"); !ok || got {
				t.Errorf("Bool(enabled) = %t, %t", got, ok)
			}
			if !row.IsNull("empty") || row.IsNull("missing") {
				t.Errorf("row did not distinguish explicit null from missing: %v", row)
			}
		})
	}

	// Run the generated memory check's captured-plan entry point too. A
	// rounded ID leaves this program with no executable action.
	capture := t.TempDir()
	for name, value := range map[string]any{
		"open@detect.json": snapshot, "open@detect-recordings.json": recorded.Calls,
	} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(capture, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if unverified, err := sdktest.CheckPlansOnIncident(rowMitigator{query: q}, capture); err != nil || len(unverified) != 0 {
		t.Fatalf("memory check plan has gaps: %v, %v", unverified, err)
	}
}

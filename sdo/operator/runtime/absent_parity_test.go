package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// answers is what one read path said about each ConfigMap: the diff and the
// class of the error, if any.
type answers map[string]struct {
	Diff  program.Diff
	Class string
}

// parityProbe records DiffAgainstSource for every name, whatever each says.
type parityProbe struct {
	names []string
	got   answers
}

func (*parityProbe) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "parity"} }
func (p *parityProbe) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	p.got = answers{}
	for _, name := range p.names {
		diff, err := v.DiffAgainstSource(cmRef(name))
		p.got[name] = struct {
			Diff  program.Diff
			Class string
		}{diff, errClass(err)}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

// The same world must give the same answer live, through the memory host's
// frozen reads, and when replayed from the capture: an absent object is
// absent on every path, for every row of the table.
func TestAbsentObjectParity(t *testing.T) {
	// cm: declared, deleted. nowhere: neither declared nor live. chg: declared,
	// present, changed. extra: present, undeclared.
	world := absentWorld(configMapKind)
	world.ConfigMapList = append(world.ConfigMapList,
		corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "chg", Namespace: "app"}, Data: map[string]string{"init.sh": "echo changed"}},
		corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "extra", Namespace: "app"}, Data: map[string]string{"k": "v"}})
	names := []string{"cm", "nowhere", "chg", "extra"}
	source := declaringSource{"cm": true, "chg": true}

	live := &parityProbe{names: names}
	recorder := &sdktest.RecordingReadView{ReadView: absentView(world, nil, source)}
	if _, err := live.Diagnose(context.Background(), recorder, program.Bindings{}); err != nil {
		t.Fatal(err)
	}
	if got := live.got["cm"].Diff; !got.MissingButInSource() {
		t.Fatalf("live: declared deleted = %+v", got)
	}
	if got := live.got["nowhere"].Diff; !got.SourceAbsent || !got.LiveAbsent || got.Changed {
		t.Fatalf("live: undeclared absent = %+v", got)
	}

	host := &parityProbe{names: names}
	remote := hostRunning(t, host)
	if _, err := remote.Diagnose(context.Background(), NewCycleReadView(absentView(world, nil, source)), program.Bindings{}); err != nil {
		t.Fatal(err)
	}

	captured, err := sdktest.CaptureSnapshot(world, "app")
	if err != nil {
		t.Fatal(err)
	}
	replay := &parityProbe{names: names}
	if _, err := replay.Diagnose(context.Background(), &sdktest.ReplayReadView{Snapshot: captured, Calls: recorder.SnapshotCalls()}, program.Bindings{}); err != nil {
		t.Fatal(err)
	}

	for _, path := range []struct {
		name string
		got  answers
	}{{"host", host.got}, {"replay of the recording", replay.got}} {
		if !reflect.DeepEqual(path.got, live.got) {
			t.Errorf("%s differs from live:\n got  %+v\n live %+v", path.name, path.got, live.got)
		}
	}
	// Object agrees between live and the captured replay.
	view := absentView(world, nil, source)
	for _, name := range []string{"cm", "nowhere"} {
		_, liveErr := view.Object(cmRef(name))
		_, replayErr := captured.Object(cmRef(name))
		if !errors.Is(liveErr, sdk.ErrObjectAbsent) || !errors.Is(replayErr, sdk.ErrObjectAbsent) {
			t.Errorf("Object(%s): live %v, replay %v; want both absent", name, liveErr, replayErr)
		}
	}
}

// errClass names an error for comparison: absent, or unavailable for any other error.
func errClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, sdk.ErrObjectAbsent):
		return "absent"
	}
	return "error"
}

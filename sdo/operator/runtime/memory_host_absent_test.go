package runtime

import (
	"context"
	"testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// AcceptedManifestRefs lists what accepted source declares, as the real source does.
func (s declaringSource) AcceptedManifestRefs(context.Context, string, string) ([]sdk.ObjectRef, error) {
	var refs []sdk.ObjectRef
	for name, declared := range s {
		if declared {
			refs = append(refs, cmRef(name))
		}
	}
	return refs, nil
}

// absenceProbe asks about a declared-but-deleted ConfigMap and an undeclared,
// absent one, and records what the read said so a test can compare paths.
type absenceProbe struct {
	declaredMissing, undeclaredAbsent *program.Diff
	missingErr, undeclaredErr         error
}

func (*absenceProbe) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "recreate"} }
func (p *absenceProbe) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	missing, err := v.DiffAgainstSource(cmRef("cm"))
	p.missingErr = err
	if err != nil {
		return program.Verdict{}, err
	}
	undeclared, err := v.DiffAgainstSource(cmRef("nowhere"))
	p.undeclaredErr = err
	if err != nil {
		return program.Verdict{}, err
	}
	p.declaredMissing, p.undeclaredAbsent = &missing, &undeclared
	if missing.MissingButInSource() && undeclared.SourceAbsent && undeclared.LiveAbsent && !undeclared.Changed {
		return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Fact: "ConfigMap deleted"}}}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func hostRunning(t *testing.T, p program.Diagnoser) *remoteProgram {
	t.Helper()
	server, err := memoryhost.NewServer("accepted", "binary", nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.WithPrograms(registry).WithReadFactory(func(ctx context.Context, snapshot sdktest.Snapshot) (program.ReadView, error) {
		return hostio.NewLiveReadView(ctx, snapshot, hostio.ReadBackends{}), nil
	})
	if err := server.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
		t.Fatal(err)
	}
	return &remoteProgram{spec: p.Spec(), remote: &RemoteMemory{rpc: acceptedReadsHost{server}, memoryCommit: "accepted"}}
}

// The memory host answers a Diagnose from reads the controller froze. A deleted
// declared object is neither in the snapshot nor bound, so the controller must
// freeze its diff too; an object nobody declares and the complete list lacks is
// answered absent on both sides, not "unrecorded".
func TestHostDiagnoseSeesDeclaredMissingObject(t *testing.T) {
	probe := &absenceProbe{}
	remote := hostRunning(t, probe)
	controller := absentView(absentWorld(configMapKind), nil, declaringSource{"cm": true})
	verdict, err := remote.Diagnose(context.Background(), NewCycleReadView(controller), program.Bindings{})
	if err != nil || verdict.Result != program.Match {
		t.Fatalf("host diagnose = %+v, %v (declared-missing %v, undeclared %v)", verdict, err, probe.missingErr, probe.undeclaredErr)
	}
}

// A host cannot vouch for a kind the controller did not list completely.
func TestHostDoesNotAssertAbsenceForAnUncompleteKind(t *testing.T) {
	probe := &absenceProbe{}
	remote := hostRunning(t, probe)
	controller := absentView(absentWorld(), nil, declaringSource{"cm": true})
	if verdict, err := remote.Diagnose(context.Background(), NewCycleReadView(controller), program.Bindings{}); err == nil && verdict.Result == program.Match {
		t.Fatalf("an unlisted kind matched through the host: %+v", verdict)
	}
}

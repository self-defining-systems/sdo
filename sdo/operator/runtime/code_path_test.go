package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCodePathDefaultGolden(t *testing.T) {
	path, err := NewCodePathRegistry().Resolve(CodePathConfig{})
	if err != nil {
		t.Fatal(err)
	}
	want := CodePathSelection{Name: "go", DetectorSource: "go", Coverage: "diagnose", ResponseStages: []string{"deterministic", "agent"}, MemoryValidation: "go", Activation: "rollout", AgentGuidance: "go"}
	if !reflect.DeepEqual(path.Selection, want) {
		t.Fatalf("default plugins changed: got %#v want %#v", path.Selection, want)
	}
	findings := []sdk.Finding{{DetectorID: "test", Status: sdk.FindingActive, Fingerprint: "a"}}
	if !reflect.DeepEqual(path.Coverage.Coverage(findings, nil, nil, nil), buildPlaybookCoverage(findings, nil, nil, nil)) {
		t.Fatal("Go coverage wrapper changed behavior")
	}
}

func TestCodePathCompositionAndOverrides(t *testing.T) {
	registry := NewCodePathRegistry()
	err := registry.RegisterCodePath("custom-agent", CodePathSelection{DetectorSource: "go", Coverage: "none", ResponseStages: []string{"agent"}, MemoryValidation: "go", Activation: "pull", AgentGuidance: "shell"})
	if err != nil {
		t.Fatal(err)
	}
	path, err := registry.Resolve(CodePathConfig{Name: "custom-agent"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(path.Selection.ResponseStages, []string{"agent"}) {
		t.Fatalf("shell selection: %#v", path.Selection)
	}
	if got := path.Coverage.Coverage([]sdk.Finding{{Status: sdk.FindingActive}}, nil, nil, nil); len(got.Matches) != 0 {
		t.Fatal("shell coverage must be empty")
	}
	custom := CodePathConfig{Name: "go", Overrides: CodePathOverrides{Coverage: "none"}}
	_, err = registry.Resolve(custom)
	var invalid *CodePathConfigError
	if !errors.As(err, &invalid) || invalid.Seam != "response_stages" {
		t.Fatalf("expected named combination error, got %v", err)
	}
	custom.Overrides.ResponseStages = []string{"agent"}
	if _, err = registry.Resolve(custom); err != nil {
		t.Fatal(err)
	}
	for _, config := range []CodePathConfig{{Name: "missing"}, {Overrides: CodePathOverrides{Activation: "missing"}}, {Overrides: CodePathOverrides{ResponseStages: []string{"unknown"}}}} {
		if _, err = registry.Resolve(config); !errors.As(err, &invalid) {
			t.Fatalf("expected named error for %#v: %v", config, err)
		}
	}
	// Registration is explicit; independently supplied plugins can coexist.
	if err := registry.RegisterCoverage("custom", noCodePathCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterCoverage("custom", noCodePathCoverage{}); err == nil {
		t.Fatal("duplicate registration accepted")
	}
	if _, err := registry.Resolve(CodePathConfig{Name: "custom-agent", Overrides: CodePathOverrides{Coverage: "custom"}}); err != nil {
		t.Fatal(err)
	}

}

func TestCodePathGoDelegatesExistingEdges(t *testing.T) {
	validationCalled, activationCalled := false, false
	path, err := NewCodePathRegistryWithDependencies(CodePathDependencies{
		ValidateGo: func(_ context.Context, root string, paths []string) error {
			validationCalled = true
			if root != "root" || !reflect.DeepEqual(paths, []string{"a"}) {
				t.Fatal("validation arguments changed")
			}
			return nil
		},
		RebuildController: func(_ context.Context, commit string) error {
			activationCalled = true
			if commit != "commit" {
				t.Fatal("commit changed")
			}
			return nil
		},
	}).Resolve(CodePathConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := path.Memory.Validate(context.Background(), "root", []string{"a"}); err != nil || !validationCalled {
		t.Fatalf("validation not delegated: %v", err)
	}
	if err := path.Activation.Activate(context.Background(), "commit"); err != nil || !activationCalled {
		t.Fatalf("rollout not delegated: %v", err)
	}
	detectors := []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}
	got, err := path.Detectors.Detectors(context.Background(), DetectorSourceRequest{RepositoryRoot: "root", Namespace: "app", CompiledDetectors: detectors})
	if err != nil || len(got) != 1 || got[0] != detectors[0] {
		t.Fatalf("compiled detector path changed: %v %v", got, err)
	}
}

func TestCodePathShellSelection(t *testing.T) {
	path, err := NewCodePathRegistry().Resolve(CodePathConfig{Name: "shell"})
	if err != nil {
		t.Fatal(err)
	}
	want := CodePathSelection{Name: "shell", DetectorSource: "shell", Coverage: "none", ResponseStages: []string{"agent"}, MemoryValidation: "hygiene", Activation: "pull", AgentGuidance: "shell"}
	if !reflect.DeepEqual(path.Selection, want) {
		t.Fatalf("shell selection: %#v", path.Selection)
	}
}

func TestCodePathGoStagesMatchPipelineDefault(t *testing.T) {
	pipelineDefault, err := responseStageNames(nil)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := NewCodePathRegistry().Resolve(CodePathConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pipelineDefault, selected.Selection.ResponseStages) {
		t.Fatalf("Go stages %v differ from PIPE default %v", selected.Selection.ResponseStages, pipelineDefault)
	}
}

func TestCodePathDispatchRetainsSelectedCoverageWithLiveLoop(t *testing.T) {
	for _, name := range []string{"go", "shell"} {
		t.Run(name, func(t *testing.T) {
			selected, err := NewCodePathRegistry().Resolve(CodePathConfig{Name: name})
			if err != nil {
				t.Fatal(err)
			}
			request := goldenRequest(t)
			request.Findings[0].Fingerprint = "unexplained"
			dispatcher := KubernetesJobDispatcher{
				Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t),
				CoverageProvider: selected.Coverage,
				LoopFactory: func(request IncidentRequest) *ResponderLoop {
					return NewResponderLoop(request, nil, staticProvider{snapshot: sdktest.Snapshot{}}, nil, PlanExecutor{}, 5)
				},
			}
			prepared, err := dispatcher.PrepareDispatch(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if name == "shell" {
				want = 0
			}
			if prepared.PlaybookCoverage == nil || len(prepared.PlaybookCoverage.UnexplainedFindingFingerprints) != want {
				t.Fatalf("%s dispatch lost selected coverage: %+v", name, prepared.PlaybookCoverage)
			}
		})
	}
}

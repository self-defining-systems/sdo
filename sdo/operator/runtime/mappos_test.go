package runtime

import (
	"reflect"
	"testing"
)

func testGraph(t *testing.T) mapGraph {
	t.Helper()
	g, err := newMapGraph(mapTestDoc)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestMapGraphTestPathUnionsTheProbedOperationPaths(t *testing.T) {
	g := testGraph(t)
	if got := g.testPath("create"); !reflect.DeepEqual(got, []string{"api", "db", "front"}) {
		t.Fatalf("create path = %v", got)
	}
	if got := g.testPath("unmapped"); len(got) != 0 {
		t.Fatalf("a scenario the map does not probe has no path, got %v", got)
	}
}

func TestMapGraphReachFollowsDependencyDirectionAndLabelsPossibleEdges(t *testing.T) {
	g := testGraph(t)
	up := g.reach("front", true, 2)
	if len(up) != 3 || up[0].Name != "api" || up[0].Depth != 1 || up[1].Via != "api" {
		t.Fatalf("front depends on api, then db and cache through it: %+v", up)
	}
	down := g.reach("api", false, 1)
	if len(down) != 2 || down[0].Name != "front" || down[0].Possible || down[1].Name != "mailer" || !down[1].Possible {
		t.Fatalf("api is called by front, and possibly by mailer: %+v", down)
	}
}

func TestMapGraphPositionClasses(t *testing.T) {
	g := testGraph(t)
	failing := map[string][]string{"create": g.testPath("create")}
	passing := map[string][]string{"list": g.testPath("list")}
	for component, want := range map[string]MapPosition{
		"db":      {Class: PositionFailingOnly, Failing: []string{"create"}, FailingHop: 0},
		"api":     {Class: PositionShared, Failing: []string{"create"}, Passing: []string{"list"}, FailingHop: 0},
		"jobs":    {Class: PositionOffPath, FailingHop: -1},
		"cache":   {Class: PositionDependency, Passing: []string{"list"}, FailingHop: 1},
		"mailer":  {Class: PositionOffPath, FailingHop: -1},
		"missing": {Class: PositionUnplaced, FailingHop: -1},
	} {
		got := g.position(component, failing, passing)
		if got.Class != want.Class || got.FailingHop != want.FailingHop || !reflect.DeepEqual(got.Failing, want.Failing) || !reflect.DeepEqual(got.Passing, want.Passing) {
			t.Fatalf("%s: got %+v, want %+v", component, got, want)
		}
	}
}

func TestMapGraphDependencyIsUpstreamOfAFailingPath(t *testing.T) {
	g := testGraph(t)
	// Only front fails: api is what front calls, db is what api calls.
	failing := map[string][]string{"home": {"front"}}
	if got := g.position("api", failing, nil); got.Class != PositionDependency || got.FailingHop != 1 {
		t.Fatalf("front depends on api: %+v", got)
	}
	if got := g.position("db", failing, nil); got.Class != PositionDependency || got.FailingHop != 2 {
		t.Fatalf("front reaches db through api: %+v", got)
	}
	if got := g.position("jobs", failing, nil); got.Class != PositionOffPath {
		t.Fatalf("jobs is a caller of db, not a dependency of front: %+v", got)
	}
}

package traffic

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// relationDoer serves a synthetic two-sided relation: GET /followers?of=X
// lists who follows X, GET /following?of=Y lists whom Y follows. Every
// request is recorded, so a test can show the probe only read.
type relationDoer struct {
	followers map[string][]string
	following map[string][]string
	methods   *[]string
}

func (d relationDoer) Do(request *http.Request) (*http.Response, error) {
	*d.methods = append(*d.methods, request.Method)
	of := request.URL.Query().Get("of")
	var ids []string
	switch request.URL.Path {
	case "/followers":
		ids = d.followers[of]
	case "/following":
		ids = d.following[of]
	}
	body := `{"ids":["` + strings.Join(ids, `","`) + `"]}`
	if len(ids) == 0 {
		body = `{"ids":[]}`
	}
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// relationScenario reads a seeded entity's followers, samples one, and
// checks the other side of the relation: that follower follows the entity.
func relationScenario() Scenario {
	return Scenario{
		ID: "sampled-relation", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead, Marker: "sdo-probe",
		Steps: []Step{
			{Name: "followers", Endpoint: GET("/followers", Params{"of": Const("7")}).
				ReadsExistingRecords("ids", "", `^[0-9]+$`).SaveJSON("ids", "followers")},
			{Name: "inverse", Endpoint: GET("/following", Params{"of": FromPrevious("follower")}).
				SampleSaved("followers", "", "follower").Includes("ids", "", "7")},
		},
	}
}

func iterateRelation(t *testing.T, doer relationDoer, iterations int) (failed int, outcomes []Outcome) {
	t.Helper()
	engine := setupEngine(t, relationScenario(), doer)
	for i := range iterations {
		_, sample := engine.Iterate(context.Background(), uint64(i))
		outcomes = append(outcomes, sample.Outcome)
		if sample.Outcome == OutcomeError {
			failed++
		}
	}
	return failed, outcomes
}

// A ghost member added to a real entity's relation answers 200 and is well
// formed, so a shape check passes; checking the other side of the relation on
// a rotating sample of real members fails, and the probe only reads.
func TestSampledInverseRelationCatchesAGhostMemberReadOnly(t *testing.T) {
	var methods []string
	healthy := relationDoer{
		followers: map[string][]string{"7": {"1", "2", "3"}},
		following: map[string][]string{"1": {"7"}, "2": {"7", "9"}, "3": {"7"}},
		methods:   &methods,
	}
	if failed, outcomes := iterateRelation(t, healthy, 20); failed != 0 {
		t.Fatalf("a healthy relation failed: %v", outcomes)
	}
	corrupt := healthy
	corrupt.followers = map[string][]string{"7": {"2", "3", "999999"}}
	if failed, _ := iterateRelation(t, corrupt, 20); failed == 0 {
		t.Fatal("no iteration sampled the ghost member")
	}
	if slices.ContainsFunc(methods, func(method string) bool { return method != http.MethodGet }) {
		t.Fatalf("the probe wrote: %v", methods)
	}
}

// A fresh install has no records: the sample is invalid, never failing.
func TestSampleOfAnEmptyListIsInvalid(t *testing.T) {
	var methods []string
	_, outcomes := iterateRelation(t, relationDoer{methods: &methods}, 1)
	if outcomes[0] != OutcomeInvalid {
		t.Fatalf("outcome %s", outcomes[0])
	}
}

func TestSampleSavedPicksAFieldOfEveryMemberOverIterations(t *testing.T) {
	endpoint := GET("/items/{id}", nil).SampleSaved("items", "ref.id", "id")
	seen := map[string]bool{}
	engine := setupEngine(t, relationScenario(), relationDoer{methods: new([]string)})
	for i := range 40 {
		request, err := endpoint.Build(context.Background(), engine.Rand(uint64(i)), State{"items": `[{"ref":{"id":"a"}},{"ref":{"id":"b"}},{"ref":{"id":"c"}}]`})
		if err != nil {
			t.Fatal(err)
		}
		seen[request.Path] = true
	}
	if len(seen) != 3 {
		t.Fatalf("sampled %v", seen)
	}
	if _, err := endpoint.Build(context.Background(), engine.Rand(0), State{"items": "not json"}); err == nil {
		t.Fatal("a value that is not a JSON array was sampled")
	}
}

func TestIncludesRequiresOneMatchingMember(t *testing.T) {
	endpoint := GET("/following", nil).Includes("ids", "id", "{entity}")
	state := State{"entity": "7"}
	for _, tc := range []struct {
		name string
		body string
		ok   bool
	}{
		{"member present", `{"ids":[{"id":3},{"id":7}]}`, true},
		{"member absent", `{"ids":[{"id":3}]}`, false},
		{"empty", `{"ids":[]}`, false},
		{"not an array", `{"ids":{"id":7}}`, false},
	} {
		if got := checkBody(endpoint, tc.body, state); got.OK != tc.ok {
			t.Errorf("%s: ok=%v, want %v (%s)", tc.name, got.OK, tc.ok, got.Reason)
		}
	}
}

// A member sampled from a list a write saved is the probe's own data.
func TestExistingRecordsStepsFollowAKeySampledFromAWrittenList(t *testing.T) {
	scenario := Scenario{
		ID: "own-sample", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectWriteWithCleanup, Marker: "sdo-probe",
		Setup: []Step{{Name: "create", Endpoint: POST("/items", Params{"name": Const("sdo-probe")}).SaveJSON("items", "created")}},
		Steps: []Step{{Name: "open", Endpoint: GET("/items/{pick}", nil).SampleSaved("created", "id", "pick").ReadsExistingRecords("items", "name", `^.+$`)}},
	}
	if facts := scenario.ExistingRecordsSteps(); len(facts) != 1 || !slices.Equal(facts[0].OwnKeys, []string{"created"}) {
		t.Fatalf("%+v", facts)
	}
}

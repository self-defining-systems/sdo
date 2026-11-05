package traffic

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func checkBody(endpoint *HTTPEndpoint, body string, state State) Verdict {
	return endpoint.Check(Response{Status: http.StatusOK, Body: []byte(body)}, state)
}

func TestJSONEqualsComparesTheExactValueOfAField(t *testing.T) {
	endpoint := GET("/items/1", nil).JSONEquals("item.note", "{note}")
	state := State{"note": "sdo-probe-42"}
	for _, tc := range []struct {
		body string
		ok   bool
	}{
		{`{"item":{"note":"sdo-probe-42"}}`, true},
		{`{"item":{"note":"sdo-probe-41"}}`, false},
		{`{"item":{"note":"sdo-probe-42 and more"}}`, false},
		{`{"item":{}}`, false},
		{`not json`, false},
	} {
		if got := checkBody(endpoint, tc.body, state); got.OK != tc.ok {
			t.Errorf("%s: ok=%v, want %v (%s)", tc.body, got.OK, tc.ok, got.Reason)
		}
	}
	if got := checkBody(endpoint, `{"item":{"note":""}}`, State{}); got.OK || !strings.Contains(got.Reason, "note") {
		t.Fatalf("a placeholder no step saved passed: %+v", got)
	}
	if got := checkBody(GET("/", nil).JSONEquals("items.1", "b"), `{"items":["a","b"]}`, State{}); !got.OK {
		t.Fatalf("an array index in the path: %s", got.Reason)
	}
}

func TestExactSetRejectsMissingExtraAndDuplicateMembers(t *testing.T) {
	endpoint := GET("/followers", nil).ExactSet("followers", "id", "{a}", "{b}")
	state := State{"a": "7", "b": "9"}
	for _, tc := range []struct {
		name string
		body string
		ok   bool
	}{
		{"same set, other order", `{"followers":[{"id":9},{"id":"7"}]}`, true},
		{"a member is missing", `{"followers":[{"id":7}]}`, false},
		{"an extra member", `{"followers":[{"id":7},{"id":9},{"id":8}]}`, false},
		{"a duplicate member", `{"followers":[{"id":7},{"id":9},{"id":9}]}`, false},
		{"a member without the field", `{"followers":[{"id":7},{"name":"x"}]}`, false},
		{"not an array", `{"followers":{"id":7}}`, false},
		{"empty", `{"followers":[]}`, false},
	} {
		if got := checkBody(endpoint, tc.body, state); got.OK != tc.ok {
			t.Errorf("%s: ok=%v, want %v (%s)", tc.name, got.OK, tc.ok, got.Reason)
		}
	}
	if got := checkBody(GET("/", nil).ExactSet("", ""), `[]`, State{}); !got.OK {
		t.Fatalf("an expected empty root array: %s", got.Reason)
	}
	if got := checkBody(GET("/", nil).ExactSet("", "", "x"), `["x"]`, State{}); !got.OK {
		t.Fatalf("a root array of strings: %s", got.Reason)
	}
}

func TestEachRequiresEveryMemberToBeWellFormed(t *testing.T) {
	endpoint := GET("/timeline", nil).Each("posts", "post_id", `^[0-9]+$`)
	for _, tc := range []struct {
		name string
		body string
		ok   bool
	}{
		{"every member matches", `{"posts":[{"post_id":12},{"post_id":"13"}]}`, true},
		{"one member is malformed", `{"posts":[{"post_id":12},{"post_id":"corrupt"}]}`, false},
		{"one member lacks the field", `{"posts":[{"post_id":12},{}]}`, false},
		{"no members", `{"posts":[]}`, false},
		{"missing array", `{}`, false},
	} {
		if got := checkBody(endpoint, tc.body, State{}); got.OK != tc.ok {
			t.Errorf("%s: ok=%v, want %v (%s)", tc.name, got.OK, tc.ok, got.Reason)
		}
	}
}

func TestStatusOnlyScenarios(t *testing.T) {
	if GET("/", nil).ChecksContent() || GET("/", nil).Status(200).OrExisting(409, "exists").ChecksContent() {
		t.Fatal("a status-only endpoint claims to check content")
	}
	for name, endpoint := range map[string]*HTTPEndpoint{
		"Contains":   GET("/", nil).Contains("x"),
		"SaveJSON":   GET("/", nil).SaveJSON("id", "id"),
		"JSONEquals": GET("/", nil).JSONEquals("id", "1"),
		"ExactSet":   GET("/", nil).ExactSet("ids", ""),
		"Each":       GET("/", nil).Each("ids", "", "."),
	} {
		if !endpoint.ChecksContent() {
			t.Errorf("%s does not count as a content check", name)
		}
	}
	scenario := Scenario{ID: "s", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead,
		Steps: []Step{{Name: "a", Endpoint: GET("/a", nil)}, {Name: "b", Endpoint: GET("/b", nil).Status(200)}}}
	if !scenario.StatusOnly() {
		t.Fatal("a scenario whose steps check only status is not StatusOnly")
	}
	scenario.Setup = []Step{{Name: "setup", Endpoint: POST("/x", nil).Contains("created")}}
	if !scenario.StatusOnly() {
		t.Fatal("a setup step's check counted as a check of the scenario's steps")
	}
	scenario.Steps[1].Endpoint = GET("/b", nil).Contains("ok")
	if scenario.StatusOnly() {
		t.Fatal("a scenario with a content check is StatusOnly")
	}
}

// The strict checks detect a healthy-status response with the wrong body.
func TestStrictChecksDetectWrongBody(t *testing.T) {
	for name, endpoint := range map[string]*HTTPEndpoint{
		"JSONEquals": GET("/", nil).JSONEquals("id", "1"),
		"ExactSet":   GET("/", nil).ExactSet("ids", "", "1"),
		"Each":       GET("/", nil).Each("ids", "", "."),
	} {
		scenario := Scenario{ID: "strict", Target: Target{Service: "web", Port: 80}, SideEffect: SideEffectRead,
			Steps: []Step{{Name: "read", Endpoint: endpoint}}, Detects: []FaultClass{FaultWrongBody}}
		if failures := CheckCatalog(context.Background(), Catalog{scenario}); len(failures) > 0 {
			t.Errorf("%s: %v", name, failures)
		}
	}
}

// A test identity logs in through the application: the session cookie a
// login step receives is saved and sent by later steps in a header.
func TestSessionCookieFlowsFromLoginToLaterSteps(t *testing.T) {
	login := POST("/login", Params{"user": Const("sdo-probe-user")}).Status(http.StatusFound).SaveCookie("session", "session")
	header := http.Header{}
	header.Add("Set-Cookie", "other=1; Path=/")
	header.Add("Set-Cookie", "session=abc123; Path=/; HttpOnly")
	state := State{}
	if got := login.Check(Response{Status: http.StatusFound, Header: header}, state); !got.OK || state["session"] != "abc123" {
		t.Fatalf("login: %+v, state %v", got, state)
	}
	if got := login.Check(Response{Status: http.StatusFound, Header: http.Header{}}, State{}); got.OK || !strings.Contains(got.Reason, "session") {
		t.Fatalf("a login without the cookie passed: %+v", got)
	}
	if !login.ChecksContent() {
		t.Fatal("saving a cookie does not count as a content check")
	}
	read := GET("/me", nil).WithHeader("Cookie", "session={session}")
	request, err := read.Build(context.Background(), nil, state)
	if err != nil || request.Headers["Cookie"] != "session=abc123" {
		t.Fatalf("header = %q, err %v", request.Headers["Cookie"], err)
	}
	if _, err := read.Build(context.Background(), nil, State{}); err == nil {
		t.Fatal("a header placeholder no step saved was sent")
	}
}

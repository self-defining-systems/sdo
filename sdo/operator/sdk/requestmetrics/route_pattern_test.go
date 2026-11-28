package requestmetrics

import "testing"

// Route labels are path templates; SDO's own concrete request paths match the
// route they were counted under, and only that one.
func TestRoutePatternsMatchConcretePaths(t *testing.T) {
	cases := []struct {
		route, path string
		match       bool
	}{
		{"/api/:version/projects/:id/repository/files/:file_path", "/api/v4/projects/7/repository/files/sdoprobe-1.txt", true},
		{"/api/:version/projects/:id/repository/files/:file_path", "/api/v4/projects/7/repository/files/dir%2Fsdoprobe-1.txt", true},
		{"/api/:version/projects/:id/repository/files/:file_path", "/api/v4/projects/7/repository/files", false},
		{"/api/:version/projects/:id/packages/generic/:package_name/*package_version/(*path/):file_name", "/api/v4/projects/7/packages/generic/p/1.0.0/a.txt", true},
		{"/api/:version/projects/:id/packages/generic/:package_name/*package_version/(*path/):file_name", "/api/v4/projects/7/packages/generic/p/1.0.0/sub/a.txt", true},
		{"/api/:version/projects/:id/issues/:issue_iid", "/api/v4/projects/7/issues/3", true},
		{"/api/:version/projects/:id/issues/:issue_iid", "/api/v4/projects/7/issues", false},
		{"/orders/{id}", "/orders/42", true},
		{"/static/{path...}", "/static/a/b.css", true},
		{"/users/<int:id>", "/users/3", true},
		{"/health", "/health", true},
		{"/health", "/healthz", false},
		{"projects#show", "/projects/1", false},
	}
	for _, c := range cases {
		re, ok := routePattern(c.route)
		if got := ok && re.MatchString(c.path); got != c.match {
			t.Errorf("route %q path %q: match %v, want %v", c.route, c.path, got, c.match)
		}
	}
}

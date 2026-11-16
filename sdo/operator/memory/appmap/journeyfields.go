package appmap

import "strings"

// The per-journey checklist of the onboarding check needs two optional map
// fields that the current schema lacks: Operation.Triggers (the background job
// classes an operation sets off) and Component.Observe (where a component's
// telemetry lives). They are the only schema added for it. When the map
// layout is redesigned (operation steps, a routes block), moving them is a
// move: the check reads them only through these definitions.

// Observe is a component's observability pointers: the selector or job of its
// metrics, the workload container or label its logs come from, and the
// service name its spans carry (empty when the runtime cannot trace).
type Observe struct {
	Metrics      string `yaml:"metrics,omitempty"`
	Logs         string `yaml:"logs,omitempty"`
	TraceService string `yaml:"trace_service,omitempty"`
}

// Routes is the application's route list and the routes deliberately left
// without a journey. List.Command runs in List.Component's container and
// prints one "METHOD route" per line.
type Routes struct {
	List     *RouteList       `yaml:"list,omitempty"`
	Excluded []RouteExclusion `yaml:"excluded,omitempty"`
}

// RouteList says how to print the application's routes.
type RouteList struct {
	Component string   `yaml:"component"`
	Container string   `yaml:"container,omitempty"`
	Command   []string `yaml:"command"`
}

// RouteExclusion is a listed route no journey covers, and why. Route is
// "METHOD route", or a route alone for every method.
type RouteExclusion struct {
	Route string `yaml:"route"`
	Why   string `yaml:"why"`
}

// RouteKey is the operation's route as the route list prints it: its method
// and RouteValue, the one rule the check and the grader share.
func (o Operation) RouteKey() string {
	return strings.ToUpper(strings.TrimSpace(o.Entry.Method + " " + o.RouteValue()))
}

// Assigned reports whether a listed "METHOD route" line is a journey's route
// or excluded.
func (m Map) Assigned(line string) bool {
	line = strings.ToUpper(strings.Join(strings.Fields(line), " "))
	route := line
	if i := strings.Index(line, " "); i > 0 {
		route = line[i+1:]
	}
	for _, op := range m.Operations {
		if op.RouteKey() == line {
			return true
		}
	}
	if m.Routes != nil {
		for _, x := range m.Routes.Excluded {
			e := strings.ToUpper(strings.Join(strings.Fields(x.Route), " "))
			if e == line || e == route {
				return true
			}
		}
	}
	return false
}

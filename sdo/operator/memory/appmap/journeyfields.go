package appmap

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"

	"sdo.dev/operator/memory/layout"
)

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

// ExcludedRouteValues is the route part of each excluded route ("METHOD route"
// or a route alone): the route label values a request counter carries for
// routes users never call (health, metrics, static files).
func (m Map) ExcludedRouteValues() []string {
	if m.Routes == nil {
		return nil
	}
	var values []string
	for _, x := range m.Routes.Excluded {
		fields := strings.Fields(x.Route)
		if len(fields) > 0 {
			values = append(values, fields[len(fields)-1])
		}
	}
	return values
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

// GivenJourney is a user journey a human lists in goal.md's front matter, in
// user terms: no routes or components.
type GivenJourney struct {
	ID   string `yaml:"id"`
	What string `yaml:"what"`
}

// ParseGoalJourneys reads the optional journeys list of goal.md's front
// matter. An operation serves one with its journey field.
func ParseGoalJourneys(goal []byte) ([]GivenJourney, error) {
	doc, err := layout.ParseDocument(goal)
	if err != nil {
		return nil, err
	}
	raw, ok := doc.Meta["journeys"]
	if !ok {
		return nil, nil
	}
	data, err := yaml.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var out []GivenJourney
	if err := yaml.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("goal journeys: %w", err)
	}
	seen := map[string]bool{}
	for _, g := range out {
		switch {
		case strings.TrimSpace(g.ID) == "" || strings.TrimSpace(g.What) == "":
			return nil, fmt.Errorf("goal journeys: every journey needs an id and what")
		case seen[g.ID]:
			return nil, fmt.Errorf("goal journeys: duplicate id %q", g.ID)
		}
		seen[g.ID] = true
	}
	return out, nil
}

// maxGivenJourneys bounds a journeys input: a longer one is a document, not a list of journeys.
const maxGivenJourneys = 100

// JourneysFromText derives the given journeys from the free text the people
// running the application write ("users sign in, browse projects, push code").
// The text is the source of truth and reaches the agents as written; this only
// gives each listed journey the stable id an operation's journey field names,
// so the given-journeys check can match them without an agent invention.
//
// A journey is one non-empty line or bullet, whatever punctuation it holds
// (semicolons, commas, colons and brackets stay inside it); a leading bullet
// or number is dropped. Only an input that is a single line (no newline) is
// split: at semicolons when it has any, else at commas. An
// id is the item's first words as a slug; a repeat gets a numeric suffix.
func JourneysFromText(text string) ([]GivenJourney, error) {
	var items []string
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	var nonEmpty []string
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			nonEmpty = append(nonEmpty, line)
		}
	}
	for _, line := range nonEmpty {
		parts := []string{line}
		if len(nonEmpty) == 1 {
			separators := ","
			if strings.Contains(line, ";") {
				separators = ";"
			}
			parts = splitAny(line, separators)
		}
		for _, part := range parts {
			if item := cleanJourneyItem(part); item != "" {
				items = append(items, item)
			}
		}
	}
	if len(items) > maxGivenJourneys {
		return nil, fmt.Errorf("journeys: %d items; list at most %d user journeys", len(items), maxGivenJourneys)
	}
	var out []GivenJourney
	used := map[string]bool{}
	for _, item := range items {
		id := journeySlug(item)
		for n := 2; used[id]; n++ {
			id = fmt.Sprintf("%s-%d", journeySlug(item), n)
		}
		used[id] = true
		out = append(out, GivenJourney{ID: id, What: item})
	}
	return out, nil
}

func splitAny(s, separators string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return strings.ContainsRune(separators, r) })
}

var journeyBullet = regexp.MustCompile(`^(?:[-*+•]|\d+[.)])\s+`)

// cleanJourneyItem trims one item: bullet, a leading "and", and trailing
// ellipsis or period. What is left may be empty.
func cleanJourneyItem(s string) string {
	s = strings.TrimSpace(s)
	s = journeyBullet.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, ". …")
	s = strings.TrimSpace(s)
	if len(s) > 4 && strings.EqualFold(s[:4], "and ") {
		s = strings.TrimSpace(s[4:])
	}
	if strings.IndexFunc(s, func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }) < 0 {
		return ""
	}
	return s
}

var journeyNonWord = regexp.MustCompile(`[^a-z0-9]+`)

// journeySlug is the first six words of an item as a lower-case id.
func journeySlug(item string) string {
	words := strings.Fields(journeyNonWord.ReplaceAllString(strings.ToLower(item), " "))
	if len(words) > 6 {
		words = words[:6]
	}
	if len(words) == 0 {
		return "journey"
	}
	return strings.Join(words, "-")
}

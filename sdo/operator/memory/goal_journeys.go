package memory

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// journeysHeading introduces the user journeys in the goal's body, where every
// agent that reads the objective sees them.
const journeysHeading = "User journeys:"

// JourneysRequiredError is the refusal to onboard an application whose people
// gave no user journeys while onboarding.journeys_required is on.
type JourneysRequiredError struct{}

func (*JourneysRequiredError) Error() string {
	return "onboarding.journeys_required is on and no user journeys were given. The people running the application " +
		"say which journeys SDO must support, as free text such as \"users sign in, browse projects, push code, " +
		"download packages\" (one journey per line or comma-separated). Give them with --journeys TEXT or " +
		"--journeys-file FILE, or as a journeys: list in the goal file's front matter (--goal-file). To onboard " +
		"without them and let the agent discover the journeys itself, set onboarding.journeys_required: false in a " +
		"--config file or SDO_ONBOARDING_JOURNEYS_REQUIRED=false"
}

// ComposeObjective returns the objective carrying the free-text journeys: a
// goal document whose front matter lists the journeys derived from the text
// (the ids operations name) and whose body is the objective followed by the
// text exactly as given. Blank text returns the objective unchanged. It is an
// error to give journeys in both the objective's front matter and the text.
func ComposeObjective(objective, journeysText string) (string, error) {
	if strings.TrimSpace(journeysText) == "" {
		return objective, nil
	}
	body := strings.TrimSpace(objective)
	if strings.HasPrefix(objective, "---\n") {
		existing, err := ObjectiveJourneys(objective)
		if err != nil {
			return "", err
		}
		if len(existing) > 0 {
			return "", fmt.Errorf("the goal already lists journeys in its front matter: give journeys once, in the goal or as text")
		}
		doc, err := layout.ParseDocument([]byte(objective))
		if err != nil {
			return "", fmt.Errorf("goal front matter: %w", err)
		}
		body = strings.TrimSpace(doc.Body)
	}
	list, err := appmap.JourneysFromText(journeysText)
	if err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", fmt.Errorf("journeys: the text %q lists no journey; write one per line, or comma-separated", strings.TrimSpace(journeysText))
	}
	front, err := yaml.Marshal(map[string]any{"schema_version": 1, "journeys": list})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("---\n%s---\n%s\n\n%s\n%s\n", front, body, journeysHeading, strings.TrimSpace(journeysText)), nil
}

// ObjectiveJourneys reads the journeys an objective carries in its front
// matter; a plain objective carries none.
func ObjectiveJourneys(objective string) ([]appmap.GivenJourney, error) {
	if !strings.HasPrefix(objective, "---\n") {
		return nil, nil
	}
	return appmap.ParseGoalJourneys([]byte(objective))
}

// RequireJourneys refuses an objective that carries no journeys.
func RequireJourneys(objective string) error {
	list, err := ObjectiveJourneys(objective)
	if err != nil {
		return err
	}
	if len(list) == 0 {
		return &JourneysRequiredError{}
	}
	return nil
}

// renderJourneys lists structured journeys in the goal's body, unless the body
// already carries them (a composed objective).
func renderJourneys(body string, list []appmap.GivenJourney) string {
	if len(list) == 0 || strings.Contains(body, journeysHeading) {
		return body
	}
	var b strings.Builder
	b.WriteString(strings.TrimSpace(body))
	b.WriteString("\n\n" + journeysHeading + "\n")
	for _, g := range list {
		fmt.Fprintf(&b, "- %s: %s\n", g.ID, g.What)
	}
	return b.String()
}

// ObjectiveText is the objective as the agents read it: the body of a goal
// document with its journeys listed, or a plain objective unchanged.
func ObjectiveText(objective string) (string, error) {
	if !strings.HasPrefix(objective, "---\n") {
		return objective, nil
	}
	doc, err := layout.ParseDocument([]byte(objective))
	if err != nil {
		return "", fmt.Errorf("goal front matter: %w", err)
	}
	list, err := appmap.ParseGoalJourneys([]byte(objective))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(renderJourneys(doc.Body, list)), nil
}

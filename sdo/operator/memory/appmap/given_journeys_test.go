package appmap

import "testing"

func TestGivenJourneysAreReadFromTheGoalFrontMatter(t *testing.T) {
	goal := "---\nschema_version: 1\nowner: human\njourneys:\n  - {id: sign-up, what: a visitor creates an account and signs in}\n  - {id: checkout, what: a customer pays for a cart}\n---\nKeep it healthy.\n"
	got, err := ParseGoalJourneys([]byte(goal))
	if err != nil || len(got) != 2 || got[0].ID != "sign-up" || got[1].What != "a customer pays for a cart" {
		t.Fatalf("got %+v, %v", got, err)
	}
	none, err := ParseGoalJourneys([]byte("---\nschema_version: 1\nowner: human\n---\nbody\n"))
	if err != nil || len(none) != 0 {
		t.Fatalf("a goal without journeys has none: %+v, %v", none, err)
	}
	if _, err := ParseGoalJourneys([]byte("---\nschema_version: 1\njourneys:\n  - {id: a}\n---\nbody\n")); err == nil {
		t.Fatal("a journey without what is an error")
	}
	if _, err := ParseGoalJourneys([]byte("---\nschema_version: 1\njourneys:\n  - {id: a, what: x}\n  - {id: a, what: y}\n---\nbody\n")); err == nil {
		t.Fatal("duplicate ids are an error")
	}
}

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

func TestJourneysFromTextDerivesOneJourneyPerItem(t *testing.T) {
	cases := map[string][]string{
		"users sign in, browse projects, push code, and download packages ...": {"users sign in", "browse projects", "push code", "download packages"},
		"- users sign in\n* browse projects, filter by tag\n3. push code\n\n":  {"users sign in", "browse projects, filter by tag", "push code"},
		"sign in; browse\npush code":                                           {"sign in; browse", "push code"},
		"sign in; browse; push code":                                           {"sign in", "browse", "push code"},
		"":                                                                     nil,
		"  \n\n":                                                               nil,
	}
	for text, want := range cases {
		got, err := JourneysFromText(text)
		if err != nil || len(got) != len(want) {
			t.Fatalf("%q: got %+v, %v", text, got, err)
		}
		for i, g := range got {
			if g.What != want[i] || g.ID == "" {
				t.Errorf("%q item %d = %+v, want what %q", text, i, g, want[i])
			}
		}
	}
	got, _ := JourneysFromText("Sign in\nsign-in\nsign in!")
	if len(got) != 3 || got[0].ID != "sign-in" || got[1].ID != "sign-in-2" || got[2].ID != "sign-in-3" {
		t.Fatalf("repeated slugs must stay unique: %+v", got)
	}
}

func TestJourneysFromTextKeepsOneJourneyPerLineWhateverItContains(t *testing.T) {
	text := "Users reach the shop through the front-end (port 80); they browse the catalogue, filter by tag: socks or shoes\n" +
		"- A user signs in (POST /login with username; password) and sees their orders\n" +
		"* A user places an order: adds an item, pays, and reads the order back (status 201; body has the id)\n"
	got, err := JourneysFromText(text)
	if err != nil || len(got) != 3 {
		t.Fatalf("one journey per line: got %+v, %v", got, err)
	}
	want := []string{
		"Users reach the shop through the front-end (port 80); they browse the catalogue, filter by tag: socks or shoes",
		"A user signs in (POST /login with username; password) and sees their orders",
		"A user places an order: adds an item, pays, and reads the order back (status 201; body has the id)",
	}
	for i, g := range got {
		if g.What != want[i] {
			t.Errorf("journey %d = %q, want %q", i, g.What, want[i])
		}
	}
}

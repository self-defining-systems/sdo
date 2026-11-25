package evidencesink

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"
)

// synthetic stands for every credential kind SDO mounts. No value is real.
var synthetic = map[string]string{
	"refresh":  "synthetic-refresh-" + strings.Repeat("r", 20),
	"client":   "synthetic-client-" + strings.Repeat("c", 12),
	"access":   "synthetic/access+" + strings.Repeat("a", 20) + "==",
	"key1":     "synthetickeyline" + strings.Repeat("k", 48),
	"key2":     "synthetickeyline" + strings.Repeat("m", 48),
	"password": `synthetic"pass<word>\` + strings.Repeat("p", 12),
	"bare":     "synthetic-bare-" + strings.Repeat("b", 20),
	"account":  "synthetic-account-" + strings.Repeat("t", 20),
	"rotated":  "synthetic-rotated-" + strings.Repeat("n", 20),
}

func writeText(t *testing.T, name, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func syntheticLogin(t *testing.T) string {
	t.Helper()
	data, err := json.MarshalIndent(map[string]any{
		"type": "authorized_user", "refresh_token": synthetic["refresh"], "client_secret": synthetic["client"],
		"token_uri": "https://oauth.example.test/token", "client_id": "public-client-identifier",
		"private_key":   "-----BEGIN PRIVATE KEY-----\n" + synthetic["key1"] + "\n" + synthetic["key2"] + "\n-----END PRIVATE KEY-----\n",
		"claudeAiOauth": map[string]any{"accessToken": synthetic["access"], "subscriptionType": "subscription-kind"},
		"accounts":      []any{map[string]any{"password": synthetic["password"]}},
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func assertNoSynthetic(t *testing.T, where, text string) {
	t.Helper()
	for kind, value := range synthetic {
		for _, line := range []string{value, jsonText(value, true), jsonText(jsonText(value, false), false)} {
			if strings.Contains(text, line) {
				t.Errorf("%s holds the synthetic %s value", where, kind)
			}
		}
	}
}

// An agent that prints its environment and reads the mounted login leaves no credential value in the saved
// transcript, whichever way its output is quoted.
func TestMirrorNeverSavesACredentialValue(t *testing.T) {
	base := t.TempDir()
	runtime, mounted, accounts, sinkRoot := filepath.Join(base, "runtime"), filepath.Join(base, "credentials"), filepath.Join(base, "accounts"), filepath.Join(base, "sink")
	login := syntheticLogin(t)
	writeText(t, filepath.Join(mounted, "..data", ".credentials.json"), login)
	if err := os.Symlink("..data/.credentials.json", filepath.Join(mounted, ".credentials.json")); err != nil {
		t.Fatal(err)
	}
	writeText(t, filepath.Join(mounted, "PROVIDER_API_KEY"), synthetic["bare"]+"\n")
	writeText(t, filepath.Join(accounts, "a01.token"), synthetic["account"])
	session := "sessions/0123456789abcdef0123456789abcdef"
	writeText(t, filepath.Join(runtime, session, "claude", ".credentials.json"), `{"claudeAiOauth":{"accessToken":"`+synthetic["access"]+`"}}`)

	printed := "LOGIN_FILE=" + login + "\nKEY=" + synthetic["bare"] + "\nTOKEN=" + synthetic["account"] + "\nkept public-client-identifier subscription-kind"
	once, _ := json.Marshal(map[string]string{"tool_result": printed})
	twice, _ := json.Marshal(map[string]string{"line": string(once)})
	transcript := session + "/claude/projects/p/s.jsonl"
	writeText(t, filepath.Join(runtime, transcript), string(once)+"\n"+string(twice)+"\n"+printed)
	if err := os.Mkdir(sinkRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	sink, err := OpenDir(sinkRoot, "run", now(), false)
	if err != nil {
		t.Fatal(err)
	}
	mirror := &Mirror{RuntimeRoot: runtime, CredentialRoots: []string{mounted}, OptionalCredentialRoots: []string{accounts, filepath.Join(base, "absent")}, Sink: sink, Now: now}
	if err := mirror.Pass(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(sink.Root(), "incidents/0123456789abcdef0123456789abcdef/claude/projects/p/s.jsonl")
	saved, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSynthetic(t, "transcript", string(saved))
	for _, kept := range []string{"public-client-identifier", "subscription-kind", "https://oauth.example.test/token", Redacted} {
		if !strings.Contains(string(saved), kept) {
			t.Errorf("transcript lost %q", kept)
		}
	}

	// A login refreshed after the copy is redacted from the files already saved.
	writeText(t, filepath.Join(runtime, transcript), "printed "+synthetic["rotated"])
	if err := mirror.Pass(); err != nil {
		t.Fatal(err)
	}
	writeText(t, filepath.Join(runtime, session, "claude", ".credentials.json"), `{"claudeAiOauth":{"accessToken":"`+synthetic["rotated"]+`"}}`)
	if err := mirror.Pass(); err != nil {
		t.Fatal(err)
	}
	if saved, err = os.ReadFile(target); err != nil {
		t.Fatal(err)
	}
	assertNoSynthetic(t, "transcript after refresh", string(saved))

	// Saved evidence is private to its writer.
	err = filepath.WalkDir(sink.Root(), func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		want := os.FileMode(0o600)
		if entry.IsDir() {
			want = 0o700
		}
		if info.Mode().Perm() != want {
			t.Errorf("%s mode %o, want %o", name, info.Mode().Perm(), want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A credential that cannot be read withholds only the transcripts it may be in: a missing mount all of them, one
// home's broken login that session's. Other evidence is saved, the gap is recorded, and it closes by itself.
func TestMirrorWithholdsOnlyUncoveredTranscripts(t *testing.T) {
	base := t.TempDir()
	runtime, mounted := filepath.Join(base, "runtime"), filepath.Join(base, "credentials")
	one, two := "0123456789abcdef0123456789abcdef", "fedcba9876543210fedcba9876543210"
	for _, id := range []string{one, two} {
		writeText(t, filepath.Join(runtime, "sessions", id, "claude/projects/p/s.jsonl"), `{"type":"message"} `+synthetic["access"])
	}
	writeText(t, filepath.Join(runtime, "usage/u.jsonl"), "{}\n")
	login := filepath.Join(runtime, "sessions", one, "claude/.credentials.json")
	full := `{"claudeAiOauth":{"accessToken":"` + synthetic["access"] + `"}}`
	writeText(t, login, full[:len(full)-8]) // half written
	sinkRoot := filepath.Join(base, "sink")
	if err := os.Mkdir(sinkRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	now := func() time.Time { return time.Unix(0, 0) }
	sink, err := OpenDir(sinkRoot, "run", now(), false)
	if err != nil {
		t.Fatal(err)
	}
	mirror := &Mirror{RuntimeRoot: runtime, CredentialRoots: []string{mounted}, Sink: sink, Now: now}
	check := func(stage string, want map[string]bool) {
		t.Helper()
		if err := mirror.Pass(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(sink.Root(), "usage/u.jsonl")); err != nil {
			t.Fatalf("%s: other evidence lost: %v", stage, err)
		}
		gaps, _ := os.ReadFile(filepath.Join(sink.Root(), GapFile))
		for id, saved := range want {
			_, err := os.Stat(filepath.Join(sink.Root(), "incidents", id, "claude/projects/p/s.jsonl"))
			if (err == nil) != saved || strings.Contains(string(gaps), id) == saved {
				t.Errorf("%s: %s saved=%v want %v, gaps %s", stage, id, err == nil, saved, gaps)
			}
		}
	}
	check("no mount", map[string]bool{one: false, two: false})
	if err := os.Mkdir(mounted, 0o700); err != nil {
		t.Fatal(err)
	}
	check("half-written login", map[string]bool{one: false, two: true})
	writeText(t, login, full)
	check("complete login", map[string]bool{one: true, two: true})
	saved, err := os.ReadFile(filepath.Join(sink.Root(), "incidents", one, "claude/projects/p/s.jsonl"))
	if err != nil || string(saved) != `{"type":"message"} `+Redacted {
		t.Fatalf("saved transcript %q, %v", saved, err)
	}
}

// Client settings kept in the Secret and short values are ordinary text.
func TestCredentialsLeaveOrdinaryTextAlone(t *testing.T) {
	mounted := t.TempDir()
	writeText(t, filepath.Join(mounted, "ANTHROPIC_VERTEX_PROJECT_ID"), "synthetic-project-id")
	writeText(t, filepath.Join(mounted, "auth.json"), `{"token":"message"}`)
	credentials := &Credentials{}
	credentials.Load([]string{mounted}, nil, t.TempDir())
	if credentials.Len() != 0 {
		t.Fatalf("%d ordinary values would be redacted", credentials.Len())
	}
}

// Values are found across read boundaries and when the source ends inside one.
func TestCredentialReaderBoundaries(t *testing.T) {
	credentials := &Credentials{}
	credentials.addDocument([]byte(synthetic["bare"]))
	credentials.index()
	filler := strings.Repeat("x", redactChunk-5)
	text := filler + synthetic["bare"] + filler + synthetic["bare"][:12]
	for _, source := range []io.Reader{strings.NewReader(text), iotest.HalfReader(strings.NewReader(text))} {
		got, err := io.ReadAll(credentials.Reader(source))
		if err != nil {
			t.Fatal(err)
		}
		if want := filler + Redacted + filler + Redacted; string(got) != want {
			t.Fatalf("got %d bytes, want %d", len(got), len(want))
		}
	}
}

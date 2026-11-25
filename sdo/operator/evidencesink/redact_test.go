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
	mirror := &Mirror{RuntimeRoot: runtime, CredentialRoots: []string{mounted, accounts, filepath.Join(base, "absent")}, Sink: sink, Now: now}
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

// Evidence is not copied when a configured credential cannot be read.
func TestMirrorCopiesNothingWithoutItsCredentials(t *testing.T) {
	base := t.TempDir()
	runtime, notDirectory := filepath.Join(base, "runtime"), filepath.Join(base, "credentials")
	writeText(t, filepath.Join(runtime, "usage", "u.jsonl"), "{}\n")
	writeText(t, notDirectory, "x")
	sink, err := OpenDir(base, "run", time.Unix(0, 0), false)
	if err != nil {
		t.Fatal(err)
	}
	mirror := &Mirror{RuntimeRoot: runtime, CredentialRoots: []string{notDirectory}, Sink: sink, Now: func() time.Time { return time.Unix(0, 0) }}
	if err := mirror.Pass(); err == nil {
		t.Fatal("unreadable credentials must fail the pass")
	}
	if _, err := os.Stat(filepath.Join(sink.Root(), "usage")); !os.IsNotExist(err) {
		t.Fatalf("evidence copied without the credential set: %v", err)
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

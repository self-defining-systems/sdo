package upstream_test

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/upstream"
	"sdo.dev/operator/upstream/gitpoll"
)

func TestParseConfigAndPollInterval(t *testing.T) {
	c := upstream.NewCatalog()
	if err := gitpoll.Register(c); err != nil {
		t.Fatal(err)
	}
	config, err := c.ParseConfig("git", []byte(`{"repository":"git://synthetic.invalid/a.git","ref":"main","poll_interval":"90s"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Decode("git", config); err != nil {
		t.Fatalf("parsed config does not decode: %v", err)
	}
	if got, err := c.PollInterval("git", config); err != nil || got != 90*time.Second {
		t.Fatalf("PollInterval = %v, %v", got, err)
	}
	unset, err := c.ParseConfig("git", []byte(`{"repository":"r","ref":"main"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.PollInterval("git", unset); err != nil || got != 0 {
		t.Fatalf("unset PollInterval = %v, %v; want 0 (follower default)", got, err)
	}
	if _, err := c.ParseConfig("git", []byte(`{"repository":"r","nope":1}`)); err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("unknown field accepted: %v", err)
	}
	if _, err := c.ParseConfig("svn", []byte(`{}`)); err == nil {
		t.Fatal("unknown watcher accepted")
	}
}

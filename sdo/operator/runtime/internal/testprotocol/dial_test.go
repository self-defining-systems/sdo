package testprotocol

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestPeerDeadlineUsesInjectedClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		at := time.Now().Add(-2 * time.Second)
		conn, err := DialWithClock(context.Background(), "redis", "healthy", func() time.Time { return at })
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("PING\r\n")); err == nil {
			t.Fatal("peer accepted input after injected deadline")
		}
	})
}

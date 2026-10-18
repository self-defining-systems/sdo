package operatorcli

import (
	"context"
	"errors"
	"testing"
)

func TestNetworkCanaryRequiresExpectedConnectivity(t *testing.T) {
	connected := func(context.Context, string) error { return nil }
	blocked := func(context.Context, string) error { return errors.New("blocked") }
	if err := ProbeNetworkCanary(context.Background(), "kubernetes:443", false, connected); err != nil {
		t.Fatalf("allowed canary rejected connectivity: %v", err)
	}
	if err := ProbeNetworkCanary(context.Background(), "kubernetes:443", true, connected); err == nil {
		t.Fatal("default-deny canary accepted connectivity")
	}
	if err := ProbeNetworkCanary(context.Background(), "kubernetes:443", true, blocked); err != nil {
		t.Fatalf("default-deny canary rejected blocked connection: %v", err)
	}
	if err := ProbeNetworkCanary(context.Background(), "kubernetes:443", false, blocked); err == nil {
		t.Fatal("allow canary accepted blocked connection")
	}
}

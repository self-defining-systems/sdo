package lifecycle

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/install"
)

type captureInstall struct {
	called bool
	config install.Config
	seeder install.RepositorySeeder
}

type capturePreparedInstall struct {
	captureInstall
	prepared bool
	root     string
}

func (c *capturePreparedInstall) InstallPrepared(_ context.Context, config install.Config, root string) (install.Result, error) {
	c.prepared = true
	c.config = config
	c.root = root
	return install.Result{}, nil
}

func TestNativeInstallAdapterUsesPreparedCheckoutInsideOperate(t *testing.T) {
	strategy := &capturePreparedInstall{}
	acceptedCommit := strings.Repeat("a", 40)
	adapter := NativeInstallAdapter{Strategy: strategy, Prepared: true, AcceptedRepository: "/workspace/application", Config: install.Config{Namespace: "demo", Application: "demo"}}
	if err := adapter.Install(context.Background(), acceptedCommit, "production"); err != nil {
		t.Fatal(err)
	}
	if !strategy.prepared || strategy.called || strategy.root != "/workspace/application" {
		t.Fatalf("prepared install did not reuse accepted PVC checkout: %+v", strategy)
	}
	if strategy.config.AcceptedCommit != acceptedCommit || strategy.config.Namespace != "production" {
		t.Fatalf("prepared install lost accepted identity: %+v", strategy.config)
	}
}

func (c *captureInstall) Install(_ context.Context, config install.Config, seeder install.RepositorySeeder) (install.Result, error) {
	c.called = true
	c.config = config
	c.seeder = seeder
	return install.Result{}, nil
}

func TestNativeInstallAdapterBindsAcceptedCommitToRepositorySeed(t *testing.T) {
	strategy := &captureInstall{}
	acceptedCommit := strings.Repeat("a", 40)
	adapter := NativeInstallAdapter{Strategy: strategy, AcceptedRepository: "/accepted/repository", Config: install.Config{Namespace: "old-namespace", Application: "demo"}}
	if err := adapter.Install(context.Background(), acceptedCommit, "production"); err != nil {
		t.Fatal(err)
	}
	if !strategy.called || strategy.seeder == nil {
		t.Fatal("native installer did not receive a repository seeder")
	}
	if strategy.config.AcceptedCommit != acceptedCommit || strategy.config.Namespace != "production" {
		t.Fatalf("installer lost accepted state: %#v", strategy.config)
	}
}

func TestNativeInstallAdapterRejectsMissingAcceptedCommit(t *testing.T) {
	strategy := &captureInstall{}
	adapter := NativeInstallAdapter{Strategy: strategy, AcceptedRepository: "/accepted/repository", Config: install.Config{Application: "demo"}}
	if err := adapter.Install(context.Background(), "", "production"); err == nil {
		t.Fatal("missing accepted commit was installed")
	}
	if strategy.called {
		t.Fatal("installer was called without an accepted commit")
	}
}

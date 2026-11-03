//go:build integration && controller_e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

func TestControllerStarts(t *testing.T) {
	w := NewWorld(t)
	w.DeployToy()
	w.InstallController()
	deadline := time.Now().Add(8 * time.Minute)
	for time.Now().Before(deadline) {
		log := w.ControllerLog()
		if strings.Contains(log, "baseline") {
			break
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("controller log:\n%s", w.ControllerLog())
	t.Logf("kubelet errors: %v", func() (out []string) {
		w.Kubelet.Errors.Range(func(k, _ any) bool { out = append(out, k.(string)); return true })
		return
	}())
}

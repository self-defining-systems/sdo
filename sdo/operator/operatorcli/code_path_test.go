package operatorcli
import (
 "context"
 "os"
 "path/filepath"
 "testing"
 contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)
func TestShellHealthRefreshRequiresShellDetector(t *testing.T) {
 root:=t.TempDir()
 cfg:=&contracts.ControllerConfig{AppRoot:root,CodePath:&contracts.CodePathConfig{Name:"shell"}}
 need,err:=shellHealthRefreshNeeded(context.Background(),cfg)
 if err!=nil || !need { t.Fatalf("missing shell health must refresh: %v %v",need,err) }
 path:=filepath.Join(root,".sdo/diagnostics/detectors/health/ready/check.sh")
 if err=os.MkdirAll(filepath.Dir(path),0755);err!=nil {t.Fatal(err)}
 if err=os.WriteFile(path,[]byte("exit 0\n"),0755);err!=nil {t.Fatal(err)}
 need,err=shellHealthRefreshNeeded(context.Background(),cfg)
 if err!=nil || need {t.Fatalf("existing shell health: %v %v",need,err)}
}
func TestResponseStageFlagUsesOnlyCodePathOverride(t *testing.T) {
 var cfg *contracts.CodePathConfig
 addResponseStageOverride(&cfg,stageOverride("agent"))
 if cfg.GetName()!="go" || len(cfg.GetOverrides().GetResponseStages())!=1 || cfg.GetOverrides().GetResponseStages()[0]!="agent" {t.Fatal(cfg)}
}

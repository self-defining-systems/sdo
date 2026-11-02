package builder

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
)

// MemoryHostArtifact is a validated executable. The caller owns Workspace until
// it has atomically published Path into its immutable artifact cache.
type MemoryHostArtifact struct {
	Path   string
	Digest string
}

// BuildMemoryHost is the single candidate-validation/accepted-runtime build
// seam. It uses the same staged diagnostics, playbooks and lib as Check. It
// returns an executable used by the opt-in controller launch path.
func (workspace *Workspace) prepareMemoryHost() error {
	if workspace == nil || workspace.Directory == "" {
		return fmt.Errorf("workspace is required")
	}
	dir := filepath.Join(workspace.Directory, "cmd", "memory-host")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	main := fmt.Sprintf(`package main
import (
 "context"
 "flag"
 "log"
 "os"
 "os/signal"
 "syscall"
 "sdo.dev/operator/memoryhost"
 "sdo.dev/operator/runtime/prober"
 "sdo.dev/operator/memoryhost/hostio"
 "sdo.dev/operator/core"
 "sdo.dev/operator/sdk/program"
 "sdo.dev/operator/sdk/sdktest"
 %q
)
func main(){
 socket:=flag.String("socket","","Unix socket")
 commit:=flag.String("commit","","accepted memory commit")
 namespace:=flag.String("namespace","","application namespace")
 synthetic:=flag.Bool("synthetic-traffic",true,"run accepted traffic")
 capBytes:=flag.Uint64("memory-bytes",512<<20,"memory cap")
 flag.Parse()
 if err:=hostio.SetMemoryLimit(*capBytes);err!=nil{log.Fatal(err)}
 path,err:=os.Executable();if err!=nil{log.Fatal(err)}
 digest,err:=hostio.BinaryDigest(path);if err!=nil{log.Fatal(err)}
 server,err:=memoryhost.NewServer(*commit,digest,generated.All());if err!=nil{log.Fatal(err)}
 programs,err:=generated.Programs();if err!=nil{log.Fatal(err)}
 server.WithPrograms(programs)
 catalog,documents:=generated.TrafficCatalog(),generated.TrafficWorkloads
 if !*synthetic {catalog,documents=nil,nil}
 workloads,err:=prober.ParseWorkloads(documents);if err!=nil{log.Fatal(err)}
 engine,err:=prober.New(prober.Config{Namespace:*namespace,Catalog:catalog,Workloads:workloads});if err!=nil{log.Fatal(err)}
 trafficHost:=prober.NewHost(engine);defer trafficHost.Close()
 server.WithTraffic(trafficHost)
 reader,err:=core.NewKubernetesClient();if err!=nil{log.Fatal(err)}
 server.WithReadFactory(func(ctx context.Context,snapshot sdktest.Snapshot)(program.ReadView,error){return hostio.NewLiveReadView(ctx,snapshot,reader),nil})
 ctx,cancel:=signal.NotifyContext(context.Background(),syscall.SIGTERM,syscall.SIGINT);defer cancel()
 if err=hostio.Serve(ctx,*socket,*capBytes,server);err!=nil{log.Fatal(err)}
}
`, workspace.ModulePath+"/generated")
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(main), 0644); err != nil {
		return err
	}
	return nil
}

// BuildMemoryHost builds the same entry point selected by Check.
func (workspace *Workspace) BuildMemoryHost(ctx context.Context) (MemoryHostArtifact, error) {
	if err := workspace.prepareMemoryHost(); err != nil {
		return MemoryHostArtifact{}, err
	}
	path := filepath.Join(workspace.Directory, "sdo-memory-host")
	stages := [][]string{{"mod", "tidy"}, {"test", "./..."}, {"build", "-trimpath", "-buildvcs=false", "-o", path, "./cmd/memory-host"}}
	if workspace.compileOnly {
		// An adopted build is unvalidated by design: compile, never execute tests.
		stages = [][]string{{"mod", "tidy"}, lean("build", "-trimpath", "-buildvcs=false", "-o", path, "./cmd/memory-host")}
	}
	for _, args := range stages {
		if err := workspace.runGo(ctx, args...); err != nil {
			return MemoryHostArtifact{}, err
		}
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		return MemoryHostArtifact{}, err
	}
	return MemoryHostArtifact{Path: path, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(bytes))}, nil
}

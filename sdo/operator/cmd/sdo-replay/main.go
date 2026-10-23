// sdo-replay compiles accepted programs through the production builder and
// executes them against offline client-go fakes. It never opens a kubeconfig.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/builder"
)

func main() { os.Exit(run()) }
func run() int {
	memory := flag.String("sdo", "", "application .sdo directory")
	operator := flag.String("operator", "", "SDO operator module directory")
	fixtures := flag.String("fixtures", "", "CorpusCase directory")
	source := flag.String("source", "", "shared accepted manifests")
	matrix := flag.Bool("matrix", true, "run every program and detector against every case")
	repeat := flag.Int("repeat", 1, "repeat cases after compilation")
	flag.Parse()
	if *memory == "" || *operator == "" || *fixtures == "" {
		fmt.Fprintln(os.Stderr, "usage: sdo-replay -sdo /app/.sdo -operator /sdo/sdo/operator -fixtures /fixtures [-source /accepted.yaml] [-matrix=true] [-repeat 50]")
		return 2
	}
	root, err := filepath.Abs(*memory)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if filepath.Base(root) != ".sdo" {
		fmt.Fprintln(os.Stderr, "-sdo must name a .sdo directory")
		return 2
	}
	workspace, binary, err := builder.BuildReplay(context.Background(), builder.Config{ApplicationRoot: filepath.Dir(root), DiagnosticsRoot: filepath.Join(root, "diagnostics"), OperatorRoot: *operator})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer workspace.Close()
	args := []string{"-fixtures", *fixtures, fmt.Sprintf("-matrix=%t", *matrix), fmt.Sprintf("-repeat=%d", *repeat)}
	if *source != "" {
		args = append(args, "-source", *source)
	}
	command := exec.Command(binary, args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err = command.Run(); err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			return exit.ExitCode()
		}
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	return 0
}

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/internal/purity/testsplit"
	"strings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	processRoot := flag.String("process-root", "", "report subprocess-dependent declarations in this test package")
	source := flag.String("source", "", "source Go test file")
	target := flag.String("target", "", "new integration test file")
	selected := flag.String("decls", "", "comma-separated declaration names; omit to list")
	write := flag.Bool("write", false, "write both files after validating the split")
	flag.Parse()
	if *processRoot != "" {
		paths, err := filepath.Glob(filepath.Join(*processRoot, "*_test.go"))
		if err != nil {
			return err
		}
		sources := map[string][]byte{}
		for _, name := range paths {
			data, err := os.ReadFile(name)
			if err != nil {
				return err
			}
			sources[name] = data
		}
		report, err := testsplit.ProcessClosure(sources)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	data, err := os.ReadFile(*source)
	if err != nil {
		return err
	}
	if *selected == "" {
		names, err := testsplit.List(data)
		if err != nil {
			return err
		}
		fmt.Println(strings.Join(names, "\n"))
		return nil
	}
	unit, integration, err := testsplit.Split(data, strings.Split(*selected, ","))
	if err != nil {
		return err
	}
	if !*write {
		fmt.Printf("validated split: unit %d bytes, integration %d bytes\n", len(unit), len(integration))
		return nil
	}
	if *target == "" || *target == *source {
		return fmt.Errorf("provide a distinct -target")
	}
	// Exclusive creation refuses an existing destination before touching source.
	output, err := os.OpenFile(*target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err = output.Write(integration); err != nil {
		output.Close()
		os.Remove(*target)
		return err
	}
	if err = output.Close(); err != nil {
		return err
	}
	if len(unit) == 0 {
		return os.Remove(*source)
	}
	return os.WriteFile(*source, unit, 0644)
}

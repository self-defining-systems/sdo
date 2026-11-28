// Command bootstrap writes SDO's Go-owned onboarding start files into a checkout, as the
// lifecycle does before an onboarding session: the operational-memory schema and the
// architecture (manifest inventory and seeded application map) derived from rendered objects.
//
//	bootstrap CHECKOUT APPLICATION SOURCE_COMMIT RENDERED.yaml
//
// The goal file is the caller's. Used by the onboarding fast gate for its synthetic application.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sdo.dev/operator/memory"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bootstrap:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 4 {
		return errors.New("usage: bootstrap CHECKOUT APPLICATION SOURCE_COMMIT RENDERED.yaml")
	}
	checkout, application, commit, rendered := args[0], args[1], args[2], args[3]
	data, err := os.ReadFile(rendered)
	if err != nil {
		return err
	}
	objects, err := decode(data)
	if err != nil {
		return fmt.Errorf("decode %s: %w", rendered, err)
	}
	if err := memory.WriteSchema(checkout); err != nil {
		return err
	}
	return memory.WriteArchitecture(checkout, application, commit, memory.ArchitectureInventory(memory.SourceFactsOf(objects, "raw")))
}

func decode(data []byte) ([]*unstructured.Unstructured, error) {
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var objects []*unstructured.Unstructured
	for {
		object := map[string]any{}
		if err := decoder.Decode(&object); errors.Is(err, io.EOF) {
			return objects, nil
		} else if err != nil {
			return nil, err
		}
		if len(object) > 0 && object["kind"] != "Secret" {
			objects = append(objects, &unstructured.Unstructured{Object: object})
		}
	}
}

// Command tool-report-schema regenerates the responder command-file schema.
package main

import (
	"encoding/json"
	"os"
	"sdo.dev/operator/runtime"
)

func main() {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(runtime.CommandReportSchema()); err != nil {
		panic(err)
	}
}

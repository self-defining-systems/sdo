package program

import (
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

var authoredAction = regexp.MustCompile(`\bsdo\s+(?:--json\s+)?(?:mitigate|plan)\s+([A-Z][A-Za-z0-9]*)\b`)
var authoredParameter = regexp.MustCompile(`--param(?:=|\s+)["']?([A-Za-z_][A-Za-z0-9_]*)=`)

// checkAuthoredActionParameters checks documented built-in selections against
// the same protobuf descriptor the executor uses for menuRawPlan. Go action
// literals are independently checked by the compiler. This is name validation,
// not execution or evidence: values and live preconditions remain executor-owned.
func checkAuthoredActionParameters(body string) error {
	body = strings.ReplaceAll(body, "\\\n", " ")
	actions := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields()
	for _, line := range strings.Split(body, "\n") {
		selections := authoredAction.FindAllStringSubmatchIndex(line, -1)
		for i, selection := range selections {
			name := line[selection[2]:selection[3]]
			var descriptor protoreflect.MessageDescriptor
			for j := 0; j < actions.Len(); j++ {
				if string(actions.Get(j).Message().Name()) == name {
					descriptor = actions.Get(j).Message()
					break
				}
			}
			if descriptor == nil {
				return fmt.Errorf("unknown built-in action %q; use the published executor contract", name)
			}
			end := len(line)
			if i+1 < len(selections) {
				end = selections[i+1][0]
			}
			parameters := line[selection[1]:end]
			for _, parameter := range authoredParameter.FindAllStringSubmatch(parameters, -1) {
				field := descriptor.Fields().ByName(protoreflect.Name(parameter[1]))
				if field == nil {
					field = descriptor.Fields().ByJSONName(parameter[1])
				}
				if field == nil {
					return fmt.Errorf("action %s: unknown parameter %q; use the published executor contract", name, parameter[1])
				}
			}
		}
	}
	return nil
}

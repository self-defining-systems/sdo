package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type menuEntry struct {
	compileError string
	removal      *UnexplainedRemoval
	spec         program.ProgramSpec
	action       protoreflect.FieldDescriptor
	programID    string
	bindings     program.Bindings
	plan         *program.Plan
}

// A session catalog retains every advertised ID. Listings are filtered views;
// preparing a batch must never replace the entries used by another selection.
type menuCatalog struct {
	entries map[string]menuEntry
}

func (c *menuCatalog) advertise(entries map[string]menuEntry) {
	if c.entries == nil {
		c.entries = make(map[string]menuEntry, len(entries))
	}
	for id, entry := range entries {
		c.entries[id] = entry
	}
}

func (c *menuCatalog) lookup(id string) (menuEntry, bool) {
	entry, ok := c.entries[id]
	return entry, ok
}

func (c *menuCatalog) empty() bool { return len(c.entries) == 0 }

func (c *menuCatalog) refreshProgram(id string, spec program.ProgramSpec) {
	for key, entry := range c.entries {
		if entry.programID == id && entry.action == nil && entry.plan == nil {
			entry.spec = spec
			entry.compileError = ""
			c.entries[key] = entry
		}
	}
}

func menuDigest(value any) string {
	data, _ := json.Marshal(value)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
func menuActionID(attempt string, round uint32, index int) string {
	return fmt.Sprintf("%s-r%d-a%d", attempt, round, index)
}
func menuActionReceipt(id string, ref sdk.ObjectRef) *contracts.MitigationActionReceipt {
	return &contracts.MitigationActionReceipt{ActionId: id, Resource: planRefProto(ref), Effect: contracts.MitigationOption_EFFECT_SDK_PLAN_ACTION, State: contracts.MitigationActionReceipt_STATE_UNKNOWN}
}
func menuBindings(bindings program.Bindings) []*contracts.Binding {
	return programRunContract(ProgramRun{Bindings: bindings}).Bindings
}

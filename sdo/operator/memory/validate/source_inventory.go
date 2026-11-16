package validate

import (
	"context"
	"strings"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/internal/memoryio"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

// derivedInventory is the manifest inventory of the checkout at r.Root: the
// committed deployment source rendered and projected by the same code that
// writes the arch.md section. When no renderer catalog is wired, the checkout
// has no plan, or the source does not render, it reads the section in doc.
func derivedInventory(r Request, doc string) appmap.Inventory {
	fromDoc := appmap.ParseInventory(doc)
	if r.Renderers == nil || r.Root == "" {
		return fromDoc
	}
	plan, err := planfile.Read(r.Root, r.PlanCatalogs)
	if err != nil || plan.GetSource() == nil {
		return fromDoc
	}
	ctx := context.Background()
	head, err := memoryio.Git(ctx, r.Root, false, nil, "rev-parse", "HEAD")
	if err != nil {
		return fromDoc
	}
	namespace := r.ApplicationNamespace
	if namespace == "" {
		namespace = "default"
	}
	source := rendered.Source{Repository: r.Root, Deployment: plan.GetSource(), Catalog: r.Renderers}
	inventory, err := source.AcceptedInventoryAllNamespaces(ctx, strings.TrimSpace(string(head)), namespace)
	if err != nil {
		return fromDoc
	}
	facts := memory.SourceFactsOf(inventory.Objects, plan.GetSource().GetRendererId())
	derived := appmap.ParseInventory(memory.ArchitectureInventory(facts))
	if len(fromDoc.Possible) > 0 {
		derived.Possible = fromDoc.Possible
	}
	return derived
}

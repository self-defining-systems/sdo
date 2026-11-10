package program

// DataEffect classifies what an action may do to the records of a store. Go
// assigns it from what it can see of the action; an author's word never lowers
// it, so anything opaque is EffectUnknown.
type DataEffect string

const (
	// EffectRead changes nothing.
	EffectRead DataEffect = "read"
	// EffectDerivedInvalidate drops entries of a component that only holds
	// copies of another component's data (a cache that refills).
	EffectDerivedInvalidate DataEffect = "derived-invalidate"
	// EffectStoreSetting changes a store's runtime setting or catalog item
	// (an account, a role, a flag), not its records.
	EffectStoreSetting DataEffect = "store-setting"
	// EffectDataWrite changes records.
	EffectDataWrite DataEffect = "data-write"
	// EffectUnknown is the default for anything opaque: it may do any of the above.
	EffectUnknown DataEffect = "unknown"
)

// Touch is one application-map component an external action reaches, and how.
type Touch struct {
	Component string     `json:"component"`
	Effect    DataEffect `json:"effect"`
}

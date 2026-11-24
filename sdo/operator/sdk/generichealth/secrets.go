package generichealth

// Legacy Secret reader rule IDs remain available to consumers of old evidence.
// Field ownership timestamps alone cannot verify that Secret contents changed.
const (
	RuleSecretReaderSkew           = "secret-reader-skew"
	RuleSecretReadersPredateChange = "secret-readers-predate-change"
)

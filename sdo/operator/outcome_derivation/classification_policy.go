package outcome_derivation

import (
	_ "embed"
	"encoding/json"
)

//go:embed classifications.json
var classificationPolicyJSON []byte

type classificationRule struct {
	ReceiptAccepted bool `json:"receipt_accepted"`
	Learnable       bool `json:"learnable"`
}

var classificationPolicy = func() map[string]classificationRule {
	var policy map[string]classificationRule
	if err := json.Unmarshal(classificationPolicyJSON, &policy); err != nil {
		panic(err)
	}
	return policy
}()

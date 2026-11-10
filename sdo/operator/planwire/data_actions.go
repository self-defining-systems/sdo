package planwire

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
)

func storeItemProto(i *storestate.Item) *contracts.StoreItem {
	if i == nil {
		return nil
	}
	return &contracts.StoreItem{Kind: string(i.Kind), Scope: i.Scope, Name: i.Name, Value: i.Value}
}

func storeItemState(i *contracts.StoreItem) *storestate.Item {
	if i == nil {
		return nil
	}
	return &storestate.Item{Kind: storestate.ItemKind(i.GetKind()), Scope: i.GetScope(), Name: i.GetName(), Value: i.GetValue()}
}

func storeTargetProto(t program.StoreTarget) *contracts.StoreTarget {
	return &contracts.StoreTarget{Component: t.Component, Engine: t.Engine, Pod: refProto(t.Pod), Container: t.Container, Port: int32(t.Port)}
}

func storeTargetState(t *contracts.StoreTarget) program.StoreTarget {
	return program.StoreTarget{Component: t.GetComponent(), Engine: t.GetEngine(), Pod: objectRefState(t.GetPod()), Container: t.GetContainer(), Port: int(t.GetPort())}
}

func dataScopeProto(s program.DataScope) *contracts.DataScope {
	return &contracts.DataScope{Database: s.Database, Collection: s.Collection, Selector: s.Selector, Limit: int32(s.Limit)}
}

func dataScopeState(s *contracts.DataScope) program.DataScope {
	return program.DataScope{Database: s.GetDatabase(), Collection: s.GetCollection(), Selector: s.GetSelector(), Limit: int(s.GetLimit())}
}

func memberPredicateProto(p *program.MemberPredicate) *contracts.MemberPredicate {
	if p == nil {
		return nil
	}
	return &contracts.MemberPredicate{NotDigits: p.NotDigits, MissingKey: p.MissingKey, In: p.In, NotIn: p.NotIn}
}

func memberPredicateState(p *contracts.MemberPredicate) *program.MemberPredicate {
	if p == nil {
		return nil
	}
	return &program.MemberPredicate{NotDigits: p.GetNotDigits(), MissingKey: p.GetMissingKey(), In: p.GetIn(), NotIn: p.GetNotIn()}
}

func optionalScopeProto(s *program.DataScope) *contracts.DataScope {
	if s == nil {
		return nil
	}
	return dataScopeProto(*s)
}

func optionalScopeState(s *contracts.DataScope) *program.DataScope {
	if s == nil {
		return nil
	}
	scope := dataScopeState(s)
	return &scope
}

// dataActionProto converts a store action; ok is false for any other action.
func dataActionProto(action program.Action) (*contracts.Action, bool) {
	switch a := program.DataAction(action).(type) {
	case program.StoreBackup:
		return &contracts.Action{Kind: &contracts.Action_StoreBackup{StoreBackup: &contracts.StoreBackup{Id: a.ID, Target: storeTargetProto(a.Target), Scope: dataScopeProto(a.Scope), Reason: a.Reason}}}, true
	case program.InvalidateDerived:
		return &contracts.Action{Kind: &contracts.Action_InvalidateDerived{InvalidateDerived: &contracts.InvalidateDerived{Target: storeTargetProto(a.Target), Scope: dataScopeProto(a.Scope), Reason: a.Reason}}}, true
	case program.RepairData:
		return &contracts.Action{Kind: &contracts.Action_RepairData{RepairData: &contracts.RepairData{Target: storeTargetProto(a.Target), Scope: dataScopeProto(a.Scope), Statement: a.Statement, MemberPredicate: memberPredicateProto(a.MemberPredicate), CopyFrom: optionalScopeProto(a.CopyFrom), ExpectMin: int32(a.ExpectMin), ExpectMax: int32(a.ExpectMax), Backup: a.Backup, Reason: a.Reason}}}, true
	case program.RestoreBackup:
		return &contracts.Action{Kind: &contracts.Action_RestoreBackup{RestoreBackup: &contracts.RestoreBackup{Target: storeTargetProto(a.Target), Backup: a.Backup, Reason: a.Reason}}}, true
	case program.RestoreStoreItem:
		change := &contracts.StoreItemChange{Before: storeItemProto(a.Change.Before), After: storeItemProto(a.Change.After)}
		return &contracts.Action{Kind: &contracts.Action_RestoreStoreItem{RestoreStoreItem: &contracts.RestoreStoreItem{Target: storeTargetProto(a.Target), Change: change, EndSessions: a.EndSessions, Reason: a.Reason}}}, true
	case program.CheckStoreCredential:
		c := a.Credential
		inputs := &contracts.StoreCredentialInputs{User: c.User, Password: c.Password, Database: c.Database, Uri: c.URI}
		return &contracts.Action{Kind: &contracts.Action_CheckStoreCredential{CheckStoreCredential: &contracts.CheckStoreCredential{Target: storeTargetProto(a.Target), Client: refProto(a.Client), Container: a.Container, Credential: inputs, Reason: a.Reason}}}, true
	}
	return nil, false
}

// dataActionState decodes a store action, or returns nil.
func dataActionState(action *contracts.Action) program.Action {
	switch kind := action.GetKind().(type) {
	case *contracts.Action_StoreBackup:
		a := kind.StoreBackup
		return program.StoreBackup{ID: a.GetId(), Target: storeTargetState(a.GetTarget()), Scope: dataScopeState(a.GetScope()), Reason: a.GetReason()}
	case *contracts.Action_InvalidateDerived:
		a := kind.InvalidateDerived
		return program.InvalidateDerived{Target: storeTargetState(a.GetTarget()), Scope: dataScopeState(a.GetScope()), Reason: a.GetReason()}
	case *contracts.Action_RepairData:
		a := kind.RepairData
		return program.RepairData{Target: storeTargetState(a.GetTarget()), Scope: dataScopeState(a.GetScope()), Statement: a.GetStatement(), MemberPredicate: memberPredicateState(a.GetMemberPredicate()), CopyFrom: optionalScopeState(a.GetCopyFrom()), ExpectMin: int(a.GetExpectMin()), ExpectMax: int(a.GetExpectMax()), Backup: a.GetBackup(), Reason: a.GetReason()}
	case *contracts.Action_RestoreBackup:
		a := kind.RestoreBackup
		return program.RestoreBackup{Target: storeTargetState(a.GetTarget()), Backup: a.GetBackup(), Reason: a.GetReason()}
	case *contracts.Action_RestoreStoreItem:
		a := kind.RestoreStoreItem
		change := storestate.Change{Before: storeItemState(a.GetChange().GetBefore()), After: storeItemState(a.GetChange().GetAfter())}
		return program.RestoreStoreItem{Target: storeTargetState(a.GetTarget()), Change: change, EndSessions: a.GetEndSessions(), Reason: a.GetReason()}
	case *contracts.Action_CheckStoreCredential:
		a := kind.CheckStoreCredential
		c := a.GetCredential()
		inputs := storestate.CredentialInputs{User: c.GetUser(), Password: c.GetPassword(), Database: c.GetDatabase(), URI: c.GetUri()}
		return program.CheckStoreCredential{Target: storeTargetState(a.GetTarget()), Client: objectRefState(a.GetClient()), Container: a.GetContainer(), Credential: inputs, Reason: a.GetReason()}
	}
	return nil
}

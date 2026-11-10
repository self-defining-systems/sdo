package planwire

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

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

// dataActionProto converts a store action; ok is false for any other action.
func dataActionProto(action program.Action) (*contracts.Action, bool) {
	switch a := program.DataAction(action).(type) {
	case program.StoreBackup:
		return &contracts.Action{Kind: &contracts.Action_StoreBackup{StoreBackup: &contracts.StoreBackup{Id: a.ID, Target: storeTargetProto(a.Target), Scope: dataScopeProto(a.Scope), Reason: a.Reason}}}, true
	case program.InvalidateDerived:
		return &contracts.Action{Kind: &contracts.Action_InvalidateDerived{InvalidateDerived: &contracts.InvalidateDerived{Target: storeTargetProto(a.Target), Scope: dataScopeProto(a.Scope), Reason: a.Reason}}}, true
	case program.RepairData:
		return &contracts.Action{Kind: &contracts.Action_RepairData{RepairData: &contracts.RepairData{Target: storeTargetProto(a.Target), Scope: dataScopeProto(a.Scope), Statement: a.Statement, ExpectMin: int32(a.ExpectMin), ExpectMax: int32(a.ExpectMax), Backup: a.Backup, Reason: a.Reason}}}, true
	case program.RestoreBackup:
		return &contracts.Action{Kind: &contracts.Action_RestoreBackup{RestoreBackup: &contracts.RestoreBackup{Target: storeTargetProto(a.Target), Backup: a.Backup, Reason: a.Reason}}}, true
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
		return program.RepairData{Target: storeTargetState(a.GetTarget()), Scope: dataScopeState(a.GetScope()), Statement: a.GetStatement(), ExpectMin: int(a.GetExpectMin()), ExpectMax: int(a.GetExpectMax()), Backup: a.GetBackup(), Reason: a.GetReason()}
	case *contracts.Action_RestoreBackup:
		a := kind.RestoreBackup
		return program.RestoreBackup{Target: storeTargetState(a.GetTarget()), Backup: a.GetBackup(), Reason: a.GetReason()}
	}
	return nil
}

package program

// The generated replay reports opaque scripts honestly: recorded diagnosis and
// origin linkage are accepted without claiming simulated external effects.
const scriptReplayNoteHelper = `
func ReplayNote(programID,caseID string,r replay.Result) sdktest.MitigationReplayResult {
 status:=sdktest.MitigationReplaySkippedUnreplayableEffect
 if r.Status=="replayed" {status=sdktest.MitigationReplayPassed;if r.ReplayBasis=="observed-transition" {status=sdktest.MitigationReplayObservedTransition}}
 effects:=append([]string(nil),r.Effects...)
 effects=append(effects,"replay status: "+r.Status,"open diagnosis: "+string(r.Faulted.Result),"healthy diagnosis: "+string(r.Healthy.Result),"post diagnosis: "+string(r.Post.Result))
 effects=append(effects,r.Errors...)
 return sdktest.MitigationReplayResult{ProgramID:programID,CaseID:caseID,Status:status,Effects:effects}
}
`

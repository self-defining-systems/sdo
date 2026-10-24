package builder

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	programvalidation "sdo.dev/operator/memory/validate/program"
)

// operatorReplayTests makes production executor closed-loop checks mandatory
// for every operator-origin positive case. It is generated alongside (not in
// place of) the normal corpus, generalization, compile, and vet gates.
func operatorReplayTests(root string, programs []programvalidation.Spec) error {
	var body strings.Builder
	for _, p := range programs {
		for _, id := range p.OperatorCases {
			fmt.Fprintf(&body, `t.Run(%q,func(t *testing.T){
   cases,err:=replay.LoadCases(filepath.Join("..","corpus",%q),"");if err!=nil{t.Fatal(err)}
   p,_:=registry.Diagnoser(%q);m,_:=registry.Mitigator(%q)
   for _,c:=range cases{r:=replay.Check(context.Background(),p,m,nil,c);if r.Status!="replayed" {t.Fatalf("operator closed-loop: %%+v",r)}}
  })
`, p.ID+"/"+id, id, p.ID, p.ID)
		}
	}
	if body.Len() == 0 {
		return nil
	}
	source := `package generated
import("context";"path/filepath";"testing";"sdo.dev/operator/runtime/replay")
func TestOperatorClosedLoop(t *testing.T){registry,err:=Programs();if err!=nil{t.Fatal(err)}
` + body.String() + "}\n"
	return os.WriteFile(filepath.Join(root, "generated", "operator_replay_test.go"), []byte(source), 0644)
}

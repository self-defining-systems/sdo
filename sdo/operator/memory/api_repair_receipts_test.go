package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestIncidentAPIRepairReceiptsRetainUnwrappedMutations(t *testing.T) {
	root := t.TempDir()
	const incident = "shell-chain"
	sub, err := PrepareIncidentSession(root, "", incident)
	if err != nil {
		t.Fatal(err)
	}
	store, err := apihistory.Open(filepath.Join(root, sub, "api-history"), incident, apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0)
	refs := []*contracts.ObjectRef{{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}, {ApiVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy.example.test"}}
	record := func(id, verb, group, version, resource, namespace, name string, status int, offset time.Duration, body string) {
		start := apihistory.Event{RequestID: id, Phase: "start", Started: at.Add(offset), Verb: verb, Group: group, Version: version, Resource: resource, Namespace: namespace, Name: name}
		store.Record(start, nil)
		end := start
		end.Phase = "end"
		end.Status = status
		end.Finished = start.Started.Add(time.Second)
		store.Record(end, []byte(body))
	}
	record("patch-web", "patch", "apps", "v1", "deployments", "app", "web", 200, time.Second, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"app","resourceVersion":"2"}}`)
	record("delete-policy", "delete", "admissionregistration.k8s.io", "v1", "validatingwebhookconfigurations", "", "policy.example.test", 200, 3*time.Second, "")
	record("confirm-policy", "get", "admissionregistration.k8s.io", "v1", "validatingwebhookconfigurations", "", "policy.example.test", 404, 5*time.Second, "")
	store.Close()
	for i := 0; i < 2; i++ {
		receipts, err := IncidentAPIRepairReceipts(root, incident, refs, at, at.Add(10*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if len(receipts) != 2 {
			t.Fatalf("lost unwrapped writes: %+v", receipts)
		}
		for j, receipt := range receipts {
			if !receipt.Success || receipt.Resources[0].Name != refs[j].Name || receipt.ActionId == "" || receipt.Reversible {
				t.Fatalf("invalid API receipt: %+v", receipt)
			}
		}
	}
}

func TestIncidentAPIRepairReceiptsRejectUnprovenOrUnrelatedWrites(t *testing.T) {
	for _, name := range []string{"failure", "dry-run", "partial", "missing-start", "wrong-start", "duplicate", "wrong-body", "missing-body", "exec", "read-only", "secret", "out-of-window", "unconfirmed-delete", "stale-generation", "dropped", "wrong-incident"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			const id = "negative"
			sub, err := PrepareIncidentSession(root, "", id)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(root, sub, "api-history")
			store, err := apihistory.Open(dir, id, apihistory.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			at := time.Unix(100, 0)
			start := apihistory.Event{RequestID: "patch", Phase: "start", Started: at.Add(time.Second), Verb: "patch", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app", Name: "web"}
			body := `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"app","resourceVersion":"2"}}`
			switch name {
			case "dry-run":
				start.DryRun = true
			case "partial":
				start.Partial = true
			case "exec":
				start.Subresource = "exec"
				start.Verb = "post"
			case "read-only":
				start.Verb = "get"
			case "secret":
				start.Resource = "secrets"
				start.Group = ""
			case "out-of-window":
				start.Started = at.Add(-time.Second)
			case "unconfirmed-delete":
				start.Verb = "delete"
			}
			if name != "missing-start" {
				store.Record(start, nil)
			}
			end := start
			end.Phase = "end"
			end.Finished = end.Started.Add(time.Second)
			end.Status = 200
			switch name {
			case "failure":
				end.Status = 403
			case "wrong-start":
				end.Name = "other"
			case "wrong-body":
				body = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"other","namespace":"app","resourceVersion":"2"}}`
			case "missing-body":
				body = ""
			}
			store.Record(end, []byte(body))
			if name == "duplicate" {
				store.Record(end, []byte(body))
			}
			if name == "dropped" {
				store.Gap()
			}
			store.Close()
			if name == "stale-generation" {
				if err := apihistory.BeginGeneration(dir); err != nil {
					t.Fatal(err)
				}
			}
			if name == "wrong-incident" {
				data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
				if err != nil {
					t.Fatal(err)
				}
				var manifest apihistory.Manifest
				if err := json.Unmarshal(data, &manifest); err != nil {
					t.Fatal(err)
				}
				manifest.Incident = "other"
				data, _ = json.Marshal(manifest)
				if err := os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			refs := []*contracts.ObjectRef{{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}}
			receipts, _ := IncidentAPIRepairReceipts(root, id, refs, at, at.Add(10*time.Second))
			if len(receipts) != 0 {
				t.Fatalf("unproven write earned receipt: %+v", receipts)
			}
		})
	}
}

func TestIncidentAPIRepairReceiptsPreservePositiveFactsAfterHardStop(t *testing.T) {
	root := t.TempDir()
	const id = "hard-stop"
	sub, err := PrepareIncidentSession(root, "", id)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, sub, "api-history")
	store, err := apihistory.Open(dir, id, apihistory.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(100, 0)
	start := apihistory.Event{RequestID: "patch", Phase: "start", Started: at.Add(time.Second), Verb: "patch", Group: "apps", Version: "v1", Resource: "deployments", Namespace: "app", Name: "web"}
	store.Record(start, nil)
	end := start
	end.Phase = "end"
	end.Status = 200
	end.Finished = at.Add(2 * time.Second)
	store.Record(end, []byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"app","resourceVersion":"2"}}`))
	store.Close()
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest apihistory.Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Open = true
	raw, _ = json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	history, err := apihistory.Read(dir)
	if err != nil || history.Manifest.Complete {
		t.Fatal("hard stop must remain incomplete for replay")
	}
	refs := []*contracts.ObjectRef{{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}}
	receipts, err := IncidentAPIRepairReceipts(root, id, refs, at, at.Add(10*time.Second))
	if err != nil || len(receipts) != 1 {
		t.Fatalf("durable positive pair lost after hard stop: %+v %v", receipts, err)
	}
}

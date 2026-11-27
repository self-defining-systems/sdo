// Package journeyhost is the body of the generated journey-check program.
package journeyhost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"sdo.dev/operator/core"
	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/memoryhost/hostio"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk/traffic"
)

// Options is the input of Run, one JSON document on stdin.
type Options struct {
	Namespace string
	SeedPath  string
	Only      []string
	Alter     bool
	// Soak runs passing scenarios for this long; 0 skips it.
	Soak time.Duration
	// Plan reads the schedule only: no cluster, nothing sent.
	Plan bool
}

// Run judges the memory's probes against the application, reached through port
// forwards, and writes the report as JSON to out.
func Run(ctx context.Context, catalog traffic.Catalog, documents map[string][]byte, applicationMap string, in io.Reader, out, progress io.Writer) error {
	var options Options
	if err := json.NewDecoder(in).Decode(&options); err != nil {
		return err
	}
	workloads, err := prober.ParseWorkloads(documents)
	if err != nil {
		return err
	}
	if options.Plan {
		return json.NewEncoder(out).Encode(journeycheck.Run(ctx, journeycheck.Config{Catalog: catalog, Workloads: workloads, Plan: true}))
	}
	client, config, err := core.NewKubernetesClientAndConfig()
	if err != nil {
		return err
	}
	users := hostio.NewProbeUsers(applicationMap, options.SeedPath, options.Namespace, "", client, kubereads.SPDYExec{Client: client, Config: config})
	forwards := &hostio.Forwards{Namespace: options.Namespace, Client: client, Config: config}
	ssh := &prober.SSHExecer{Signer: users.SSHSigner}
	say := func(line string) { fmt.Fprintln(progress, line) }
	report := journeycheck.Run(ctx, journeycheck.Config{Catalog: catalog, Workloads: workloads, Only: options.Only, Alter: options.Alter,
		Client: prober.NewHTTPClient(), BaseURL: forwards.BaseURL, Passwords: users, Exec: ssh, Progress: say,
		Wait: hostio.Pause})
	if options.Soak > 0 {
		workloads := journeycheck.SoakWorkloads(workloads, report)
		if len(workloads) == 0 {
			say("healthy run skipped: no passing selected scenarios or the schedule exceeds its budget")
		} else {
			say(fmt.Sprintf("running passing scenarios for %s", options.Soak))
			soakCtx, stop := context.WithTimeout(ctx, options.Soak)
			defer stop()
			p, err := prober.New(prober.Config{Namespace: options.Namespace, Catalog: catalog, Workloads: workloads, BaseURL: forwards.BaseURL, Passwords: users, Exec: ssh, Signer: users.SSHSigner})
			if err != nil {
				return err
			}
			p.Run(soakCtx)
			report.Results = append(report.Results, journeycheck.Soaked(p.Windows())...)
		}
	}
	return json.NewEncoder(out).Encode(report)
}

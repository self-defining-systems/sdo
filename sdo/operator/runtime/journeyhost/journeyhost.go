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

// soakPoll is how often the healthy run checks whether it may stop early.
const soakPoll = 5 * time.Second

// Run judges the memory's probes against the application, reached through port
// forwards, and writes the report as JSON to out.
func Run(ctx context.Context, catalog traffic.Catalog, documents map[string][]byte, applicationMap string, in io.Reader, out, progress io.Writer) error {
	var options journeycheck.Options
	if err := json.NewDecoder(in).Decode(&options); err != nil {
		return err
	}
	workloads, err := prober.ParseWorkloads(documents)
	if err != nil {
		return err
	}
	check := journeycheck.Config{
		Catalog: catalog, Workloads: workloads, Plan: options.Plan, DetectionTarget: options.DetectionTarget,
	}
	if options.Plan {
		return json.NewEncoder(out).Encode(journeycheck.Run(ctx, check))
	}
	say := func(line string) {
		if options.Verbose {
			fmt.Fprintln(progress, line)
		}
	}
	client, config, err := core.NewKubernetesClientAndConfig()
	if err != nil {
		return err
	}
	users := hostio.NewProbeUsers(applicationMap, options.SeedPath, options.Namespace, "", client, kubereads.SPDYExec{Client: client, Config: config})
	forwards := &hostio.Forwards{Namespace: options.Namespace, Client: client, Config: config}
	ssh := &prober.SSHExecer{Signer: users.SSHSigner}
	check.Only, check.Alter, check.Once = options.Only, options.Alter, options.Once
	check.Client, check.BaseURL, check.Passwords, check.Exec = prober.NewHTTPClient(), forwards.BaseURL, users, ssh
	check.Progress, check.Wait = say, hostio.Pause
	report := journeycheck.Run(ctx, check)
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
			go func() {
				// Stop early once every scenario is decided (SoakSettled).
				for hostio.Pause(soakCtx, soakPoll) == nil {
					if journeycheck.SoakSettled(p.Windows()) {
						stop()
						return
					}
				}
			}()
			p.Run(soakCtx)
			report.Results = append(report.Results, journeycheck.Soaked(p.Windows())...)
		}
	}
	return json.NewEncoder(out).Encode(report)
}

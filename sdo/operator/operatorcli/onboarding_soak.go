package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/journeycheck"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// OnboardingSoakUsage documents "sdo-operator onboarding soak".
const OnboardingSoakUsage = `usage: sdo-operator onboarding soak --control-namespace NS [--kubeconfig FILE] [--wait 20m]

  Reads the installed controller's onboarding soak: once every opener detector has been evaluated,
  a new install runs the committed probes on their schedule for onboarding.soak (5m) and judges
  them like onboarding journeys --soak, and runs every probe's full checks once beside it, like
  onboarding journeys --alter. Its reds never open an incident. An install soaks once.
  Waits up to --wait for the record. Green: one line, exit 0. Red: prints the one follow-up turn
  for the onboarding session that authored the probes, exit 1. No record within --wait, or an
  infrastructure error: exit 2.`

// soakPoll is how often the command reads the record while it waits.
const soakPoll = 5 * time.Second

func onboardingSoak(ctx context.Context, args []string, stdout, stderr io.Writer) (int, error) {
	flags := flag.NewFlagSet("onboarding soak", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	control := flags.String("control-namespace", "", "operator namespace of the installed controller")
	kubeconfig := flags.String("kubeconfig", "", "kubeconfig file")
	wait := flags.Duration("wait", 20*time.Minute, "how long to wait for the soak to end")
	if err := flags.Parse(args); err != nil {
		return 0, err
	}
	if flags.NArg() != 0 || *control == "" || *wait < 0 {
		return 0, errors.New("soak requires --control-namespace and a non-negative --wait")
	}
	client, err := hostKubernetesClient(*kubeconfig)
	if err != nil {
		return 0, err
	}
	read := func(ctx context.Context) (*journeycheck.SoakRecord, error) {
		configMap, err := client.CoreV1().ConfigMaps(*control).Get(ctx, journeycheck.SoakConfigMap, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		record, err := journeycheck.ParseSoakConfigMap(*control, configMap.Data)
		return &record, err
	}
	reads := int(*wait/soakPoll) + 1
	pause := func(ctx context.Context) error { return processio.SleepUntil(ctx, processio.Now().Add(soakPoll)) }
	code := waitOnboardingSoak(ctx, read, reads, pause, stdout)
	if code == exitInfra {
		fmt.Fprintf(stderr, "no onboarding soak record in %s after %s\n", *control, *wait)
	}
	return code, nil
}

// waitOnboardingSoak reads the record up to reads times, pausing between
// reads, and prints the verdict. A read error is retried like a missing
// record.
func waitOnboardingSoak(ctx context.Context, read func(context.Context) (*journeycheck.SoakRecord, error), reads int, pause func(context.Context) error, stdout io.Writer) int {
	for i := 0; i < reads; i++ {
		if i > 0 && pause(ctx) != nil {
			return exitInfra
		}
		record, err := read(ctx)
		if err != nil || record == nil {
			continue
		}
		if message := journeycheck.SoakFollowUp(*record); message != "" {
			fmt.Fprintln(stdout, message)
			return exitDiagnostics
		}
		fmt.Fprintf(stdout, "green onboarding soak: %d scenarios over %s, none red\n", len(record.Results), record.Duration.Duration())
		return exitClean
	}
	return exitInfra
}

package buildkit

import (
	"context"
	"errors"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"time"
)

const missingSourceLog = `#5 [internal] load build context
#5 DONE 0.1s
#9 [3/6] COPY nginx.vh.default.conf /etc/nginx/conf.d/
#9 ERROR: failed to calculate checksum of ref abc::def: "/nginx.vh.default.conf": not found
------
 > [3/6] COPY nginx.vh.default.conf /etc/nginx/conf.d/:
------
error: failed to solve: failed to compute cache key: failed to calculate checksum of ref abc::def: "/nginx.vh.default.conf": not found
`

const compileLog = `#12 [builder 5/8] RUN make -j4
#12 3.1 In file included from WriteHomeTimelineService.cpp:4
#12 3.2 ClientPool.h:88:5: error: no matching function for call to 'RedisClient::RedisClient(std::string, int, int, json)'
#12 3.3 make[2]: *** [CMakeFiles/WriteHomeTimelineService.dir/build.make:63: x.o] Error 1
#12 ERROR: process "/bin/sh -c make -j4" did not complete successfully: exit code: 2
error: failed to solve: process "/bin/sh -c make -j4" did not complete successfully: exit code: 2
`

const aptLog = `#7 [2/4] RUN apt-get update && apt-get install -y libssl-dev
#7 2.0 Err:1 http://archive.ubuntu.com/ubuntu xenial InRelease  Temporary failure resolving 'archive.ubuntu.com'
#7 2.1 E: Failed to fetch http://archive.ubuntu.com/ubuntu/dists/xenial/InRelease
#7 ERROR: process "/bin/sh -c apt-get update && apt-get install -y libssl-dev" did not complete successfully: exit code: 100
`

func TestClassifyBuildLogByCause(t *testing.T) {
	cases := []struct {
		name, log string
		cause     BuildFailureCause
		step      string
		contains  string
	}{
		{"missing source", missingSourceLog, BuildFailureMissingSource, "[3/6] COPY nginx.vh.default.conf", "/nginx.vh.default.conf"},
		{"compile", compileLog, BuildFailureCompile, "[builder 5/8] RUN make -j4", "ClientPool.h:88:5: error"},
		{"network", aptLog, BuildFailureNetwork, "[2/4] RUN apt-get update", "Temporary failure resolving"},
		{"other", "#3 [1/2] RUN false\n#3 ERROR: process \"/bin/sh -c false\" did not complete successfully: exit code: 1\n", BuildFailureOther, "[1/2] RUN false", "exit code: 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failure := ClassifyBuildLog("reg/app:1", "job", tc.log)
			if failure.Cause != tc.cause || !strings.Contains(failure.Step, tc.step) {
				t.Fatalf("cause=%q step=%q", failure.Cause, failure.Step)
			}
			if message := failure.Error(); !strings.Contains(message, tc.contains) || !strings.Contains(message, string(tc.cause)) {
				t.Fatalf("feedback lacks %q: %s", tc.contains, message)
			}
		})
	}
}

func TestBuildFailureFeedbackIsBoundedForHugeLogs(t *testing.T) {
	huge := strings.Repeat("#4 12.5 "+strings.Repeat("x", 5000)+"\n", 5000) + compileLog
	message := ClassifyBuildLog("reg/app:1", "job", huge).Error()
	if len([]rune(message)) > 4000 {
		t.Fatalf("feedback is %d characters; the prompt budget for correction feedback is 4000", len([]rune(message)))
	}
	if !strings.Contains(message, "ClientPool.h:88:5: error") {
		t.Fatal("the tail must keep the compiler error")
	}
}

func TestFailedBuildJobReturnsTypedFailureWithLog(t *testing.T) {
	root, checkout := proposalCheckout(t)
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"}}}}, nil
	})
	backend := testBackend(root, client, time.Minute)
	backend.ReadJobLogs = func(context.Context, string, string) ([]byte, error) { return []byte(missingSourceLog), nil }
	_, err := backend.buildImage(context.Background(), checkout, testBuild(checkout))
	var failure *BuildFailureError
	if !errors.As(err, &failure) || failure.Cause != BuildFailureMissingSource {
		t.Fatalf("want typed missing-source failure, got %v", err)
	}
	if strings.Contains(err.Error(), "backoff limit") {
		t.Fatalf("the Job condition must not replace the build log: %v", err)
	}
	_ = metav1.ObjectMeta{}
}

// Hand-written BuildKit plain-progress logs. On a cold cache the registry
// cache import fails non-fatally before any build step.
const coldCacheImportLine = `#1 [internal] load build definition from Dockerfile
#1 DONE 0.0s

#2 importing cache manifest from reg.example/app:sdo-buildcache
#2 ERROR: reg.example/app:sdo-buildcache: not found

#3 [internal] load metadata for docker.io/library/ubuntu:20.04
#3 DONE 0.4s
`

func TestClassifyBuildLogIgnoresCacheImportNoise(t *testing.T) {
	cases := []struct {
		name, log, step, message string
		cause                    BuildFailureCause
	}{
		{"cold cache then compile failure", coldCacheImportLine + compileLog, "[builder 5/8] RUN make -j4", "exit code: 2", BuildFailureCompile},
		{"cold cache then missing source", coldCacheImportLine + missingSourceLog, "[3/6] COPY nginx.vh.default.conf", "/nginx.vh.default.conf", BuildFailureMissingSource},
		{"newer buildkit wording", "#4 ERROR: failed to configure registry cache importer: reg.example/app:sdo-buildcache: not found\n" + aptLog, "[2/4] RUN apt-get update", "exit code: 100", BuildFailureNetwork},
		{"cache export error after real failure", compileLog + "#13 exporting cache to registry\n#13 ERROR: error writing layer blob: 500\n", "[builder 5/8] RUN make -j4", "exit code: 2", BuildFailureCompile},
		{"cache import line after the failure", compileLog + "#14 importing cache manifest from reg.example/app:sdo-buildcache\n#14 ERROR: reg.example/app:sdo-buildcache: not found\n", "[builder 5/8] RUN make -j4", "exit code: 2", BuildFailureCompile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failure := ClassifyBuildLog("reg/app:1", "job", tc.log)
			if !strings.Contains(failure.Step, tc.step) || !strings.Contains(failure.Message, tc.message) || failure.Cause != tc.cause {
				t.Fatalf("step=%q message=%q cause=%q", failure.Step, failure.Message, failure.Cause)
			}
			if strings.Contains(failure.Message, "cache") {
				t.Fatalf("headline is cache noise: %q", failure.Message)
			}
		})
	}
}

func TestClassifyBuildLogKeepsCacheErrorWhenItIsTheOnlyFailure(t *testing.T) {
	failure := ClassifyBuildLog("reg/app:1", "job", "#13 exporting cache to registry\n#13 ERROR: error writing layer blob: 500\n")
	if !strings.Contains(failure.Message, "error writing layer blob") {
		t.Fatalf("message=%q", failure.Message)
	}
}

func TestClassifierNamesDockerignoreExclusionInsteadOfMissingSource(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"svc/Dockerfile": "FROM x\n", "svc/nginx.vh.default.conf": "x", "svc/.dockerignore": "*.conf\n"})
	failure := ClassifyBuildLog("reg/app:1", "job", missingSourceLog)
	failure.ExplainExclusions(dir+"/svc", dir+"/svc/Dockerfile")
	if failure.Cause != BuildFailureExcluded || len(failure.Missing) != 0 {
		t.Fatalf("cause=%q missing=%v", failure.Cause, failure.Missing)
	}
	message := failure.Error()
	for _, want := range []string{"nginx.vh.default.conf", "excluded", ".dockerignore", "do not change context_path"} {
		if !strings.Contains(message, want) {
			t.Fatalf("feedback lacks %q: %s", want, message)
		}
	}
	if strings.Contains(message, "do not exist under this build's context_path") {
		t.Fatalf("must not call an existing file missing: %s", message)
	}
}

func TestClassifierKeepsMissingWhenTheFileReallyIsAbsent(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"svc/Dockerfile": "FROM x\n", "svc/.dockerignore": "*.conf\n"})
	failure := ClassifyBuildLog("reg/app:1", "job", missingSourceLog)
	failure.ExplainExclusions(dir+"/svc", dir+"/svc/Dockerfile")
	if failure.Cause != BuildFailureMissingSource || len(failure.Missing) != 1 {
		t.Fatalf("cause=%q missing=%v", failure.Cause, failure.Missing)
	}
}

const buildkitStartupLog = "could not connect to unix:///run/user/1000/buildkit/buildkitd.sock after 10 trials\n========== log ==========\n"

func TestBuildKitStartupFailureIsInfrastructureNotPlan(t *testing.T) {
	failure := ClassifyBuildLog("reg/app:1", "job", buildkitStartupLog)
	if failure.Cause != BuildFailureInfrastructure || !failure.InfrastructureOutcome() {
		t.Fatalf("cause = %q", failure.Cause)
	}
	real := ClassifyBuildLog("reg/app:1", "job", compileLog)
	if real.InfrastructureOutcome() {
		t.Fatal("a compile error is a source fault")
	}
}

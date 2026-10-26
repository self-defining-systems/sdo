package shellpath

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const RunnerName = "sdo-shell-detectors"
const RunnerPort = 8080

func RunnerURL(control string) string {
	return fmt.Sprintf("http://%s.%s.svc:%d/results", RunnerName, control, RunnerPort)
}

// RunInCluster is the long-lived runner entrypoint. Its service account has only
// application reads, with no Secret resource or control-namespace write grants.
func RunInCluster(ctx context.Context) error {
	namespace, root := os.Getenv("SDO_SHELL_NAMESPACE"), os.Getenv("SDO_SHELL_REPOSITORY")
	if namespace == "" || root == "" {
		return fmt.Errorf("shell runner requires application namespace and accepted repository")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	// Kubeconfig uses the projected service account token, without reading a
	// Kubernetes Secret. The filesystem and the mounted repository are read-only.
	kubeconfig := "/tmp/sdo-shell-kubeconfig"
	data := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: app\n  cluster:\n    server: %s\n    certificate-authority: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\ncontexts:\n- name: app\n  context:\n    cluster: app\n    namespace: %s\n    user: app\ncurrent-context: app\nusers:\n- name: app\n  user:\n    tokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token\n", config.Host, namespace)
	if err := os.WriteFile(kubeconfig, []byte(data), 0600); err != nil {
		return err
	}
	events := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var watchers sync.WaitGroup
	startWatch := func(open func(context.Context) (watch.Interface, error)) {
		watchers.Add(1)
		go func() { defer watchers.Done(); watchEvents(runCtx, open, events) }()
	}
	opts := metav1.ListOptions{AllowWatchBookmarks: true, TimeoutSeconds: ptr(int64(60))}
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.CoreV1().Pods(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.CoreV1().Services(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.CoreV1().ConfigMaps(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.CoreV1().Endpoints(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.CoreV1().Events(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.AppsV1().Deployments(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.AppsV1().StatefulSets(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.AppsV1().DaemonSets(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.AppsV1().ReplicaSets(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.BatchV1().Jobs(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.NetworkingV1().NetworkPolicies(namespace).Watch(c, opts)
	})
	startWatch(func(c context.Context) (watch.Interface, error) {
		return client.DiscoveryV1().EndpointSlices(namespace).Watch(c, opts)
	})
	reports := &ReportStore{}
	server := &http.Server{Addr: fmt.Sprintf(":%d", RunnerPort), Handler: reports, ReadHeaderTimeout: 5 * time.Second}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	env := append(os.Environ(), "KUBECONFIG="+kubeconfig, "HOME=/tmp")
	service := &Service{Repository: &GitRepository{Root: root, Scratch: "/tmp"}, Executor: BashExecutor{Env: env}, Reports: reports, Events: events, Tick: ticker.C}
	done := make(chan error, 1)
	go func() { done <- service.Run(runCtx) }()
	var result error
	select {
	case result = <-done:
	case result = <-serverErrors:
	case <-ctx.Done():
		result = ctx.Err()
	}
	cancel()
	_ = server.Close()
	watchers.Wait()
	return result
}
func ptr[T any](v T) *T { return &v }
func watchEvents(ctx context.Context, open func(context.Context) (watch.Interface, error), events chan<- struct{}) {
	for ctx.Err() == nil {
		watcher, err := open(ctx)
		if err == nil {
			closed := false
			for !closed {
				select {
				case <-ctx.Done():
					watcher.Stop()
					return
				case event, ok := <-watcher.ResultChan():
					if !ok || event.Type == watch.Error {
						closed = true
						break
					}
					if event.Type != watch.Bookmark {
						select {
						case events <- struct{}{}:
						default:
						}
					}
				}
			}
			watcher.Stop()
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// AwaitActivation is used after publication. It never acknowledges a commit
// until the runner has loaded that exact accepted revision.
func AwaitActivation(ctx context.Context, reader Reader, commit string) (time.Time, error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		report, err := reader.Read(ctx)
		if err == nil && report.AcceptedCommit == commit && !report.ActivatedAt.IsZero() {
			return report.ActivatedAt, nil
		}
		select {
		case <-ctx.Done():
			return time.Time{}, fmt.Errorf("shell activation %s: %w", commit, ctx.Err())
		case <-ticker.C:
		}
	}
}

// EnsureReadOnlyRunner applies only installer-owned objects. Existing Pods are
// reused only when their complete expected spec matches; image/config changes replace them.
func EnsureReadOnlyRunner(ctx context.Context, client kubernetes.Interface, config RunnerConfig) error {
	objects, err := RunnerResources(config)
	if err != nil {
		return err
	}
	for _, object := range objects {
		switch want := object.(type) {
		case *corev1.ServiceAccount:
			_, err = client.CoreV1().ServiceAccounts(want.Namespace).Create(ctx, want, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				err = nil
			}
		case *corev1.Pod:
			existing, getErr := client.CoreV1().Pods(want.Namespace).Get(ctx, want.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(getErr) {
				_, err = client.CoreV1().Pods(want.Namespace).Create(ctx, want, metav1.CreateOptions{})
			} else if getErr != nil {
				err = getErr
			} else if existing.Annotations["sdo.dev/runner-config"] != want.Annotations["sdo.dev/runner-config"] {
				if existing.Labels["app.kubernetes.io/managed-by"] != "sdo" {
					return fmt.Errorf("shell runner Pod is not installer-owned")
				}
				bounded, cancel := context.WithTimeout(ctx, time.Minute)
				defer cancel()
				policy := metav1.DeletePropagationForeground
				if err = client.CoreV1().Pods(want.Namespace).Delete(bounded, want.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
					return err
				}
				ticker := time.NewTicker(200 * time.Millisecond)
				defer ticker.Stop()
				for {
					_, getErr = client.CoreV1().Pods(want.Namespace).Get(bounded, want.Name, metav1.GetOptions{})
					if apierrors.IsNotFound(getErr) {
						_, err = client.CoreV1().Pods(want.Namespace).Create(bounded, want, metav1.CreateOptions{})
						break
					}
					if getErr != nil {
						return getErr
					}
					select {
					case <-bounded.Done():
						return fmt.Errorf("replace shell runner: %w", bounded.Err())
					case <-ticker.C:
					}
				}
			}
		case *corev1.Service:
			_, err = client.CoreV1().Services(want.Namespace).Create(ctx, want, metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				existing, getErr := client.CoreV1().Services(want.Namespace).Get(ctx, want.Name, metav1.GetOptions{})
				if getErr != nil {
					return getErr
				}
				want.ResourceVersion = existing.ResourceVersion
				want.Spec.ClusterIP = existing.Spec.ClusterIP
				want.Spec.ClusterIPs = existing.Spec.ClusterIPs
				_, err = client.CoreV1().Services(want.Namespace).Update(ctx, want, metav1.UpdateOptions{})
			}
		default:
			err = ensureRBAC(ctx, client, object)
		}
		if err != nil {
			return err
		}
	}
	slog.Info("shell_runner_ready", "namespace", config.ControlNamespace, "image", config.Image)
	return nil
}

// ReportJSON is shared by private outcome/evidence exporters.
func ReportJSON(report Report) ([]byte, error) { return json.Marshal(report) }

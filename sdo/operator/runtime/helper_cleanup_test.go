package runtime

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	batchclient "k8s.io/client-go/kubernetes/typed/batch/v1"
)

type helperCleanupTestClient struct {
	kubernetes.Interface
	batch batchclient.BatchV1Interface
}

func (c helperCleanupTestClient) BatchV1() batchclient.BatchV1Interface { return c.batch }

type helperCleanupTestBatch struct {
	batchclient.BatchV1Interface
	blocked, independent chan struct{}
}

func (c helperCleanupTestBatch) Jobs(namespace string) batchclient.JobInterface {
	return helperCleanupTestJobs{JobInterface: c.BatchV1Interface.Jobs(namespace), namespace: namespace, batch: c}
}

type helperCleanupTestJobs struct {
	batchclient.JobInterface
	namespace string
	batch     helperCleanupTestBatch
}

func (c helperCleanupTestJobs) List(ctx context.Context, opts metav1.ListOptions) (*batchv1.JobList, error) {
	if c.namespace == "application" {
		<-c.batch.blocked
	} else {
		c.batch.independent <- struct{}{}
	}
	return c.JobInterface.List(ctx, opts)
}

func TestHelperCleanupProgressesAcrossNamespaces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := fake.NewSimpleClientset(
			&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "application", Name: "helper", Labels: map[string]string{ResponderHelperLabel: "true"}}},
			&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "control", Name: "helper", Labels: map[string]string{ResponderHelperLabel: "true"}}},
			&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "control", Name: "app-owned"}},
		)
		blocked, independent := make(chan struct{}), make(chan struct{}, 1)
		cleaner := KubernetesHelperCleaner{
			Client:     helperCleanupTestClient{client, helperCleanupTestBatch{client.BatchV1(), blocked, independent}},
			Namespaces: []string{"application", "control", "control", ""},
		}
		done := make(chan struct{})
		var deleted []string
		var err error
		go func() {
			deleted, err = cleaner.CleanupHelpers(context.Background())
			close(done)
		}()
		synctest.Wait()
		started := len(independent) == 1
		close(blocked)
		<-done
		if !started {
			t.Error("cleanup of control namespace waited for application namespace")
		}
		want := []string{"Job/application/helper", "Job/control/helper"}
		if err != nil || !slices.Equal(deleted, want) {
			t.Fatalf("cleanup = %v, %v, want %v", deleted, err, want)
		}
	})
}

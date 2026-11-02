package install

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Every installer-created resource has the same explicit ownership boundary as
// the drift inventory and namespace onboarding. Generated children inherit it
// through their Kubernetes owner references.
func labelInstallObjects[T metav1.Object](namespace string, objects ...T) {
	for _, object := range objects {
		labels := object.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/managed-by"] = "sdo"
		labels[ApplicationNamespaceLabel] = namespace
		object.SetLabels(labels)
	}
}

func labelInstallPlan(c Config, p *Plan) {
	if p.Namespace != nil {
		labelInstallObjects(c.Namespace, p.Namespace)
	}
	labelInstallObjects[metav1.Object](c.Namespace, p.PVC, p.SyncPod, p.Job, &p.Job.Spec.Template.ObjectMeta)
	labelInstallObjects(c.Namespace, p.ServiceAccounts...)
	labelInstallObjects(c.Namespace, p.ClusterRoles...)
	labelInstallObjects(c.Namespace, p.ClusterBindings...)
}

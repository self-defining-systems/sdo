package core

import (
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestExtendedAccessorsAreSortedDefensiveCopies(t *testing.T) {
	s := DetectionSnapshot{
		StatefulSetList:    []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		DaemonSetList:      []appsv1.DaemonSet{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		JobList:            []batchv1.Job{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		CronJobList:        []batchv1.CronJob{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		PVCList:            []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		ServiceAccountList: []corev1.ServiceAccount{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		HPAList:            []autoscalingv2.HorizontalPodAutoscaler{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		PDBList:            []policyv1.PodDisruptionBudget{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		IngressList:        []networkingv1.Ingress{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
		NodeList:           []corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "z", Labels: map[string]string{"state": "original"}}}, {ObjectMeta: metav1.ObjectMeta{Name: "a", Labels: map[string]string{"state": "original"}}}},
	}
	for _, method := range []string{"StatefulSets", "DaemonSets", "Jobs", "CronJobs", "PersistentVolumeClaims", "ServiceAccounts", "HorizontalPodAutoscalers", "PodDisruptionBudgets", "Ingresses", "Nodes"} {
		t.Run(method, func(t *testing.T) {
			accessor := reflect.ValueOf(s).MethodByName(method)
			first := accessor.Call(nil)[0]
			if first.Len() != 2 || first.Index(0).FieldByName("Name").String() != "a" {
				t.Fatal("unsorted")
			}
			first.Index(0).FieldByName("Labels").SetMapIndex(reflect.ValueOf("state"), reflect.ValueOf("mutated"))
			second := accessor.Call(nil)[0]
			if second.Index(0).FieldByName("Labels").MapIndex(reflect.ValueOf("state")).String() != "original" {
				t.Fatal("accessor leaked mutable state")
			}
		})
	}
}

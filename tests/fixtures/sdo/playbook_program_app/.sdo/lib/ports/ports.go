package ports

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func Matches(target intstr.IntOrString, exposed corev1.ContainerPort) bool {
	return target.Type == intstr.Int && exposed.ContainerPort == target.IntVal || target.Type == intstr.String && exposed.Name == target.StrVal
}

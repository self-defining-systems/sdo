package core

import (
	"k8s.io/klog/v2"
	"strings"
)

// client-go v0.30 logs request/response payloads with the process-global klog
// logger, outside the HTTP transport and request context. Install its privacy
// boundary once, before goroutines start. It covers typed, dynamic, discovery,
// List/watch and injected clients equally, at every verbosity. Transport wrappers
// cannot suppress these messages without changing the bytes callers decode.
func init() { klog.SetLogFilter(kubernetesBodyLogFilter{}) }

type kubernetesBodyLogFilter struct{}

const omittedKubernetesBody = "Kubernetes body omitted"

func bodyLogMessage(message string) bool {
	return strings.HasPrefix(message, "Request Body") || strings.HasPrefix(message, "Response Body")
}
func (kubernetesBodyLogFilter) Filter(args []interface{}) []interface{} {
	if len(args) > 0 {
		if prefix, ok := args[0].(string); ok && bodyLogMessage(prefix) {
			return []interface{}{omittedKubernetesBody}
		}
	}
	return args
}
func (kubernetesBodyLogFilter) FilterF(format string, args []interface{}) (string, []interface{}) {
	if bodyLogMessage(format) {
		return omittedKubernetesBody, nil
	}
	if len(args) > 0 {
		if prefix, ok := args[0].(string); ok && bodyLogMessage(prefix) {
			return omittedKubernetesBody, nil
		}
	}
	return format, args
}
func (kubernetesBodyLogFilter) FilterS(message string, values []interface{}) (string, []interface{}) {
	if bodyLogMessage(message) {
		return omittedKubernetesBody, nil
	}
	return message, values
}

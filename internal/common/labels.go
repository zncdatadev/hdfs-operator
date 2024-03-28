package common

import (
	"strings"
)

type RoleLabels struct {
	InstanceName string
	Name         string
}

func (r *RoleLabels) GetLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/Name":       strings.ToLower(r.InstanceName),
		"app.kubernetes.io/component":  r.Name,
		"app.kubernetes.io/managed-by": "alluxio-operator",
	}
}

func GetListenerLabels(listenerClass ListenerClass) map[string]string {
	return map[string]string{
		ListenerAnnotationKey: string(listenerClass),
	}
}

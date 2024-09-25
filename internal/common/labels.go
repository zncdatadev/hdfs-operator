package common

import (
	"strings"

	"github.com/zncdatadev/operator-go/pkg/constants"
)

const (
	LabelCrName    = "app.kubernetes.io/Name"
	LabelComponent = "app.kubernetes.io/component"
	LabelManagedBy = "app.kubernetes.io/managed-by"
)

type RoleLabels struct {
	InstanceName string
	Name         string
}

func (r *RoleLabels) GetLabels() map[string]string {
	res := map[string]string{
		LabelCrName:    strings.ToLower(r.InstanceName),
		LabelComponent: r.Name,
		LabelManagedBy: "hdfs-operator",
	}
	if r.Name != "" {
		res[LabelComponent] = r.Name
	}
	return res
}

func GetListenerLabels(listenerClass constants.ListenerClass) map[string]string {
	return map[string]string{
		constants.AnnotationListenersClass: string(listenerClass),
	}
}

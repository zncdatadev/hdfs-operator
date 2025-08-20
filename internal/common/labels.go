package common

import (
	"github.com/zncdatadev/operator-go/pkg/constants"
)

const (
	LabelCrName    = "app.kubernetes.io/Name"
	LabelComponent = "app.kubernetes.io/component"
	LabelManagedBy = "app.kubernetes.io/managed-by"
)

func GetListenerLabels(listenerClass constants.ListenerClass) map[string]string {
	return map[string]string{
		constants.AnnotationListenersClass: string(listenerClass),
	}
}

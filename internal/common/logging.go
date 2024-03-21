package common

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RoleLoggingDataBuilder interface {
	MakeContainerLogData() map[string]string
}

type LoggingRecociler[T client.Object, G any] struct {
	GeneralResourceStyleReconciler[T, G]
	RoleLoggingDataBuilder RoleLoggingDataBuilder
	role                   Role
	InstanceGetter         InstanceAttributes
	roleConfigName         string
}

// NewLoggingReconciler new logging reconcile
func NewLoggingReconciler[T client.Object](
	scheme *runtime.Scheme,
	instance T,
	client client.Client,
	groupName string,
	mergedLabels map[string]string,
	mergedCfg any,
	logDataBuilder RoleLoggingDataBuilder,
	role Role,
) *LoggingRecociler[T, any] {
	return &LoggingRecociler[T, any]{
		GeneralResourceStyleReconciler: *NewGeneraResourceStyleReconciler(
			scheme,
			instance,
			client,
			groupName,
			mergedLabels,
			mergedCfg,
		),
		RoleLoggingDataBuilder: logDataBuilder,
		role:                   role,
	}
}

// Build log4j config map
func (l *LoggingRecociler[T, G]) Build(_ context.Context) (client.Object, error) {
	cmData := l.RoleLoggingDataBuilder.MakeContainerLogData()
	if len(cmData) == 0 {
		return nil, nil
	}
	obj := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      l.roleConfigName,
			Namespace: l.Instance.GetNamespace(),
			Labels:    l.MergedLabels,
		},
		Data: cmData,
	}
	return obj, nil
}

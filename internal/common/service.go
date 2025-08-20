package common

import (
	"context"

	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	opconstants "github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func NewRoleGroupService(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	listenerClass opconstants.ListenerClass,
	headless bool,
	portsProvider ServicePortProvider,
) reconciler.ResourceReconciler[builder.ServiceBuilder] {
	// Check if portsProvider also implements builder.ServiceBuilder
	if serviceBuilder, ok := portsProvider.(builder.ServiceBuilder); ok {
		// If it's already a ServiceBuilder, use it directly
		return reconciler.NewGenericResourceReconciler(
			client,
			serviceBuilder,
		)
	}

	panic("portsProvider does not implement builder.ServiceBuilder")
}

type ServicePortProvider interface {
	GetServicePorts() []corev1.ContainerPort
}

// HdfsServiceBuilder implements the ServiceBuilder interface for Hdfs services
type HdfsServiceBuilder struct {
	*builder.BaseServiceBuilder
}

func (b *HdfsServiceBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	obj := b.GetObject()
	// set service publish not ready address
	obj.Spec.PublishNotReadyAddresses = true
	return obj, nil
}

// NewHdfsServiceBuilder creates a new HdfsServiceBuilder
func NewHdfsServiceBuilder(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	listenerClass opconstants.ListenerClass,
	headless bool,
	portsProvider ServicePortProvider,
) *HdfsServiceBuilder {

	baseBuilder := builder.NewServiceBuilder(
		client,
		roleGroupInfo.GetFullName(),
		portsProvider.GetServicePorts(),
		func(sbo *builder.ServiceBuilderOptions) {
			sbo.Headless = headless
			sbo.ListenerClass = listenerClass
			sbo.Labels = roleGroupInfo.GetLabels()
			sbo.MatchingLabels = roleGroupInfo.GetLabels()
			sbo.Annotations = roleGroupInfo.GetAnnotations()
		},
	)

	return &HdfsServiceBuilder{
		BaseServiceBuilder: baseBuilder,
	}
}

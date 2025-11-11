package common

import (
	"context"
	"fmt"
	"fmt"
	"strconv"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
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

// NewRoleGroupMetricsService creates a metrics service reconciler using a simple function approach
// This creates a headless service for metrics with Prometheus labels and annotations
func NewRoleGroupMetricsService(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	hdfs *hdfsv1alpha1.HdfsCluster,
) reconciler.Reconciler {
	roleName := roleGroupInfo.GetRoleName()
	role := constant.Role(roleName)
	// Get metrics port
	metricsPort, err := GetNativeMetricsPort(role, hdfs.Spec.ClusterConfig)
	if err != nil {
		// Log the error and return nil to avoid misconfiguration
		fmt.Printf("GetMetricsPort error for role %v: %v. Skipping metrics service creation.\n", roleName, err)
		return nil
	}

	// Create service ports
	servicePorts := []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: metricsPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}

	// Create service name with -metrics suffix
	serviceName := CreateServiceMetricsName(roleGroupInfo)

	// Determine scheme based on TLS configuration
	scheme := "http"
	if IsTlsEnabled(hdfs.Spec.ClusterConfig) {
		scheme = "https"
	}

	// Prepare labels (copy from roleGroupInfo)
	labels := make(map[string]string)
	for k, v := range roleGroupInfo.GetLabels() {
		labels[k] = v
	}
	labels["prometheus.io/scrape"] = "true"

	// Prepare annotations (copy from roleGroupInfo and add Prometheus annotations)
	annotations := make(map[string]string)
	for k, v := range roleGroupInfo.GetAnnotations() {
		annotations[k] = v
	}
	annotations["prometheus.io/scrape"] = "true"
	annotations["prometheus.io/path"] = "/prom"
	annotations["prometheus.io/port"] = strconv.Itoa(int(metricsPort))
	annotations["prometheus.io/scheme"] = scheme

	// Create base service builder
	baseBuilder := builder.NewServiceBuilder(
		client,
		serviceName,
		servicePorts,
		func(sbo *builder.ServiceBuilderOptions) {
			sbo.Headless = true
			sbo.ListenerClass = opconstants.ClusterInternal
			sbo.Labels = labels
			sbo.MatchingLabels = roleGroupInfo.GetLabels() // Use original labels for matching
			sbo.Annotations = annotations
		},
	)

	// Create HdfsServiceBuilder to set PublishNotReadyAddresses
	hdfsServiceBuilder := &HdfsServiceBuilder{
		BaseServiceBuilder: baseBuilder,
	}

	return reconciler.NewGenericResourceReconciler(
		client,
		hdfsServiceBuilder,
	)
}

package name

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/client"
	opconstants "github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
)

// NameNodeServiceBuilder implements ServiceBuilder for NameNode headless service
// It inherits from HdfsServiceBuilder and implements ServicePortProvider
type NameNodeServiceBuilder struct {
	*common.HdfsServiceBuilder
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

// Compile-time check to ensure NameNodeServiceBuilder implements ServicePortProvider
var _ common.ServicePortProvider = &NameNodeServiceBuilder{}

// NewNameNodeServiceBuilder creates a new NameNodeServiceBuilder
func NewNameNodeServiceBuilder(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec,
) *NameNodeServiceBuilder {
	// Create the service builder instance first
	serviceBuilder := &NameNodeServiceBuilder{
		clusterConfig: clusterConfig,
	}

	// Create the underlying HdfsServiceBuilder using the instance as port provider
	serviceBuilder.HdfsServiceBuilder = common.NewHdfsServiceBuilder(
		client,
		roleGroupInfo,
		opconstants.ClusterInternal, // NameNode uses cluster internal listener
		true,                        // headless service
		serviceBuilder,              // Use self as ServicePortProvider
	)
	return serviceBuilder
}

// GetServicePorts implements ServicePortProvider interface
func (b *NameNodeServiceBuilder) GetServicePorts() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.RpcName,
			ContainerPort: hdfsv1alpha1.NameNodeRpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: hdfsv1alpha1.NameNodeMetricPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          "oidc",
			ContainerPort: 4180,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	// Add HTTP/HTTPS port based on TLS configuration
	httpPort := common.HttpPort(b.clusterConfig, hdfsv1alpha1.NameNodeHttpsPort, hdfsv1alpha1.NameNodeHttpPort)
	ports = append(ports, httpPort)

	return ports
}

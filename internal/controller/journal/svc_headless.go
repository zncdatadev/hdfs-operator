package journal

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/client"
	opconstants "github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
)

// JournalNodeServiceBuilder implements ServiceBuilder for JournalNode headless service
// It inherits from HdfsServiceBuilder and implements ServicePortProvider
type JournalNodeServiceBuilder struct {
	*common.HdfsServiceBuilder
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

// Compile-time check to ensure JournalNodeServiceBuilder implements ServicePortProvider
var _ common.ServicePortProvider = &JournalNodeServiceBuilder{}

// NewJournalNodeServiceBuilder creates a new JournalNodeServiceBuilder
func NewJournalNodeServiceBuilder(
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec,
) *JournalNodeServiceBuilder {
	// Create the service builder instance first
	serviceBuilder := &JournalNodeServiceBuilder{
		clusterConfig: clusterConfig,
	}

	// Create the underlying HdfsServiceBuilder using the instance as port provider
	serviceBuilder.HdfsServiceBuilder = common.NewHdfsServiceBuilder(
		client,
		roleGroupInfo,
		opconstants.ClusterInternal, // JournalNode uses cluster internal listener
		true,                        // headless service
		serviceBuilder,              // Use self as ServicePortProvider
	)
	return serviceBuilder
}

// GetServicePorts implements ServicePortProvider interface
func (b *JournalNodeServiceBuilder) GetServicePorts() []corev1.ContainerPort {
	ports := make([]corev1.ContainerPort, 0, 4)
	ports = append(ports, []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.RpcName,
			ContainerPort: hdfsv1alpha1.JournalNodeRpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: hdfsv1alpha1.JournalNodeMetricPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          "oidc",
			ContainerPort: 4180,
			Protocol:      corev1.ProtocolTCP,
		},
	}...)
	// Add HTTP/HTTPS port based on TLS configuration
	httpPort := common.HttpPort(b.clusterConfig, hdfsv1alpha1.JournalNodeHttpsPort, hdfsv1alpha1.JournalNodeHttpPort)
	ports = append(ports, httpPort)

	return ports
}

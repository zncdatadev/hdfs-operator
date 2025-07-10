package data

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
)

// DataNodeServiceBuilder builds Service for DataNode
type DataNodeServiceBuilder struct {
	common.HdfsServiceBuilder
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
}

// ServicePortProvider interface for DataNode Service
var _ common.ServicePortProvider = &DataNodeServiceBuilder{}

// NewDataNodeServiceBuilder creates a new DataNode Service builder
func NewDataNodeServiceBuilder(
	client *client.Client,
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
) *DataNodeServiceBuilder {
	dnBulder := &DataNodeServiceBuilder{}
	dnBulder.HdfsServiceBuilder = *common.NewHdfsServiceBuilder(
		client,
		roleGroupInfo,
		constants.ClusterInternal, // DataNode uses cluster internal listener
		true,                      // not a headless service
		dnBulder,                  // Use self as ServicePortProvider
	)
	dnBulder.instance = instance
	dnBulder.roleGroupInfo = roleGroupInfo
	dnBulder.roleGroupConfig = roleGroupConfig
	return dnBulder
}

// GetServicePorts returns the service ports for DataNode
func (b *DataNodeServiceBuilder) GetServicePorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		{
			Name:          "data",
			ContainerPort: 9866,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          "http",
			ContainerPort: 9864,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          "ipc",
			ContainerPort: 9867,
			Protocol:      corev1.ProtocolTCP,
		},
	}
}

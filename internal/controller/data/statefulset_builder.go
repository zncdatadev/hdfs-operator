package data

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	"github.com/zncdatadev/hdfs-operator/internal/controller/data/container"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opClient "github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Compile-time check to ensure DataNodeStatefulSetBuilder implements StatefulSetComponentBuilder
var _ common.StatefulSetComponentBuilder = (*DataNodeStatefulSetBuilder)(nil)

// DataNodeStatefulSetBuilder inherits from common StatefulSetBuilder and implements datanode-specific logic
type DataNodeStatefulSetBuilder struct {
	*common.StatefulSetBuilder
	// datanode-specific fields
	config        *hdfsv1alpha1.ConfigSpec
	image         *util.Image
	roleGroupInfo *reconciler.RoleGroupInfo
}

// NewDataNodeStatefulSetBuilder creates a new DataNodeStatefulSetBuilder that inherits from common StatefulSetBuilder
func NewDataNodeStatefulSetBuilder(
	ctx context.Context,
	client *opClient.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	image *util.Image,
	replicas *int32,
	config *hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	instance *hdfsv1alpha1.HdfsCluster,
) *DataNodeStatefulSetBuilder {

	dnStsBuilder := &DataNodeStatefulSetBuilder{
		config:        config,
		image:         image,
		roleGroupInfo: roleGroupInfo,
	}
	// Create the common StatefulSetBuilder
	commonBuilder := common.NewStatefulSetBuilder(
		ctx,
		client,
		roleGroupInfo,
		image,
		replicas,
		config.RoleGroupConfigSpec,
		overrides,
		instance,
		constant.DataNode,
		dnStsBuilder,
	)

	dnStsBuilder.StatefulSetBuilder = commonBuilder
	// Set the component to self
	return dnStsBuilder
}

// Build constructs the StatefulSet using the inherited common builder and datanode-specific component
func (b *DataNodeStatefulSetBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	// Use the inherited common builder's Build method, passing self as the component builder
	return b.StatefulSetBuilder.Build(ctx)
}

// StatefulSetComponentBuilder interface implementation

// GetName returns the StatefulSet name
func (b *DataNodeStatefulSetBuilder) GetName() string {
	return b.roleGroupInfo.GetFullName()
}

// GetMainContainers returns the main containers for datanode
func (b *DataNodeStatefulSetBuilder) GetMainContainers() []corev1.Container {
	return []corev1.Container{
		b.makeDataNodeContainer(),
	}
}

// GetInitContainers returns init containers for datanode
func (b *DataNodeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	return []corev1.Container{
		b.makeWaitForNameNodesContainer(),
	}
}

func (b *DataNodeStatefulSetBuilder) GetHttpPort() int32 {
	return common.HttpPort(b.GetInstance().Spec.ClusterConfig, hdfsv1alpha1.DataNodeHttpsPort, hdfsv1alpha1.DataNodeHttpPort).ContainerPort
}

// GetVolumes returns datanode-specific volumes
func (b *DataNodeStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: hdfsv1alpha1.HdfsConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getDataNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.HdfsLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getDataNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.WaitForNamenodesConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getDataNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.WaitForNamenodesLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getDataNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.ListenerVolumeName,
			VolumeSource: corev1.VolumeSource{
				Ephemeral: &corev1.EphemeralVolumeSource{
					VolumeClaimTemplate: b.createListenPvcTemplate(),
				},
			},
		},
	}
}

// GetVolumeClaimTemplates returns PVCs for datanode
func (b *DataNodeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		b.createDataPvcTemplate(),
		// b.createListenPvcTemplate(),
	}
}

// GetServiceAccountName returns the service account name for datanode
func (b *DataNodeStatefulSetBuilder) GetServiceAccountName() string {
	return common.CreateServiceAccountName(b.GetInstance().GetName())
}

// Helper methods for container creation

func (b *DataNodeStatefulSetBuilder) makeDataNodeContainer() corev1.Container {
	dataNode := container.NewDataNodeContainerBuilder(
		b.GetInstance(),
		b.GetRoleGroupInfo(),
		b.config.RoleGroupConfigSpec,
		b.image,
	)
	return *dataNode.Build()
}

func (b *DataNodeStatefulSetBuilder) makeWaitForNameNodesContainer() corev1.Container {
	waitForNameNodes := container.NewWaitForNameNodesContainerBuilder(
		b.GetInstance(),
		b.GetRoleGroupInfo(),
		b.config.RoleGroupConfigSpec,
		b.image,
	)
	return *waitForNameNodes.Build()
}

// TODO: extract this to a common method if needed in other builders(nn,jn,dn)
func (b *DataNodeStatefulSetBuilder) createDataPvcTemplate() corev1.PersistentVolumeClaim {
	// storageSize := b.config.Config.Resources.Storage.Capacity
	storageSize := b.config.Resources.Storage.Capacity
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: hdfsv1alpha1.DataVolumeMountName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  func() *corev1.PersistentVolumeMode { v := corev1.PersistentVolumeFilesystem; return &v }(),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
		},
	}
}

func (b *DataNodeStatefulSetBuilder) createListenPvcTemplate() *corev1.PersistentVolumeClaimTemplate {
	listenerClass := constants.ListenerClass(*b.config.ListenerClass)
	if listenerClass == "" {
		listenerClass = constants.ClusterInternal
	}
	return &corev1.PersistentVolumeClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: common.GetListenerLabels(listenerClass),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: constants.ListenerStorageClassPtr(),
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(common.ListenerPvcStorage),
				},
			},
		},
	}
}

func (b *DataNodeStatefulSetBuilder) getDataNodeConfigMapSource() *corev1.ConfigMapVolumeSource {
	return &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: b.roleGroupInfo.GetFullName(),
		},
	}
}

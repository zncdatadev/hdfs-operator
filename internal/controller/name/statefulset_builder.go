package name

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	"github.com/zncdatadev/hdfs-operator/internal/controller/name/container"
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

// Compile-time check to ensure NamenodeStatefulSetBuilder implements StatefulSetComponentBuilder
var _ common.StatefulSetComponentBuilder = &NamenodeStatefulSetBuilder{}

// NamenodeStatefulSetBuilder inherits from common StatefulSetBuilder and implements namenode-specific logic
type NamenodeStatefulSetBuilder struct {
	*common.StatefulSetBuilder
	// namenode-specific fields
	config          *hdfsv1alpha1.ConfigSpec
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *util.Image
	roleGroupInfo   *reconciler.RoleGroupInfo
}

// NewNamenodeStatefulSetBuilder creates a new NamenodeStatefulSetBuilder that inherits from common StatefulSetBuilder
func NewNamenodeStatefulSetBuilder(
	ctx context.Context,
	client *opClient.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	image *util.Image,
	replicas *int32,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	instance *hdfsv1alpha1.HdfsCluster,
	mergedCfg *hdfsv1alpha1.ConfigSpec,
) *NamenodeStatefulSetBuilder {
	nnStsBuilder := &NamenodeStatefulSetBuilder{
		config:          mergedCfg,
		roleGroupConfig: roleConfig,
		image:           image,
		roleGroupInfo:   roleGroupInfo,
	}
	// Create the common StatefulSetBuilder
	commonBuilder := common.NewStatefulSetBuilder(
		ctx,
		client,
		roleGroupInfo,
		image,
		replicas,
		roleConfig,
		overrides,
		instance,
		constant.NameNode,
		nnStsBuilder,
	)
	nnStsBuilder.StatefulSetBuilder = commonBuilder

	return nnStsBuilder
}

// Build constructs the StatefulSet using the inherited common builder and namenode-specific component
func (b *NamenodeStatefulSetBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	// Use the inherited common builder's Build method, passing self as the component builder
	return b.StatefulSetBuilder.Build(ctx)
}

// StatefulSetComponentBuilder interface implementation

// GetName returns the StatefulSet name
func (b *NamenodeStatefulSetBuilder) GetName() string {
	return b.roleGroupInfo.GetFullName()
}

func (b *NamenodeStatefulSetBuilder) GetHttpPort() int32 {
	return common.HttpPort(b.GetInstance().Spec.ClusterConfig, hdfsv1alpha1.NameNodeHttpsPort, hdfsv1alpha1.NameNodeHttpPort).ContainerPort
}

// GetMainContainers returns the main containers for namenode
func (b *NamenodeStatefulSetBuilder) GetMainContainers() []corev1.Container {
	return []corev1.Container{
		b.makeNameNodeContainer(),
		b.makeZkfcContainer(),
	}
}

// GetInitContainers returns init containers for namenode
func (b *NamenodeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	return []corev1.Container{
		b.makeFormatNameNodeContainer(),
		b.makeFormatZookeeperContainer(),
	}
}

// GetVolumes returns namenode-specific volumes
func (b *NamenodeStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: hdfsv1alpha1.HdfsConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.HdfsLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.FormatNamenodesConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.FormatNamenodesLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.FormatZookeeperConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.FormatZookeeperLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getNameNodeConfigMapSource(),
			},
		},
	}
}

// GetVolumeClaimTemplates returns PVCs for namenode
func (b *NamenodeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		b.createDataPvcTemplate(),
		b.createListenPvcTemplate(),
	}
}

// GetServiceAccountName returns the service account name for namenode
func (b *NamenodeStatefulSetBuilder) GetServiceAccountName() string {
	return common.CreateServiceAccountName(b.GetInstance().GetName())

}

// Helper methods for container creation

func (b *NamenodeStatefulSetBuilder) makeNameNodeContainer() corev1.Container {
	nameNode := container.NewNameNodeContainerBuilder(
		b.GetInstance(),
		b.GetRoleGroupInfo(),
		b.roleGroupConfig,
		b.image,
	)
	return *nameNode.Build()
}

func (b *NamenodeStatefulSetBuilder) makeZkfcContainer() corev1.Container {
	zkfc := container.NewZkfcContainerBuilder(
		b.GetInstance(),
		b.GetRoleGroupInfo(),
		b.roleGroupConfig,
		b.image,
	)
	return *zkfc.Build()
}

func (b *NamenodeStatefulSetBuilder) makeFormatNameNodeContainer() corev1.Container {
	formatNameNode := container.NewFormatNameNodeContainerBuilder(
		b.GetInstance(),
		b.GetRoleGroupInfo(),
		b.roleGroupConfig,
		b.image,
		*b.GetReplicas(),
		b.roleGroupInfo.GetFullName(),
	)
	return *formatNameNode.Build()
}

func (b *NamenodeStatefulSetBuilder) makeFormatZookeeperContainer() corev1.Container {
	formatZookeeper := container.NewFormatZookeeperContainerBuilder(
		b.GetInstance(),
		b.GetRoleGroupInfo(),
		b.roleGroupConfig,
		b.image,
	)
	return *formatZookeeper.Build()
}

func (b *NamenodeStatefulSetBuilder) createDataPvcTemplate() corev1.PersistentVolumeClaim {
	storageSize := resource.MustParse("10Gi") // Default size
	if b.config != nil && b.config.Resources != nil && b.config.Resources.Storage != nil {
		storageSize = b.config.Resources.Storage.Capacity
	}
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

func (b *NamenodeStatefulSetBuilder) createListenPvcTemplate() corev1.PersistentVolumeClaim {
	var listenerClass constants.ListenerClass
	listenerClassSpec := b.config.ListenerClass
	if listenerClassSpec == nil || *listenerClassSpec == "" {
		listenerClass = constants.ClusterInternal
	} else {
		listenerClass = constants.ListenerClass(*listenerClassSpec)
	}
	// Create a PersistentVolumeClaim for the listener
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        hdfsv1alpha1.ListenerVolumeName,
			Annotations: common.GetListenerLabels(listenerClass),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  func() *corev1.PersistentVolumeMode { v := corev1.PersistentVolumeFilesystem; return &v }(),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(common.ListenerPvcStorage),
				},
			},
			StorageClassName: constants.ListenerStorageClassPtr(),
		},
	}
}

func (b *NamenodeStatefulSetBuilder) getNameNodeConfigMapSource() *corev1.ConfigMapVolumeSource {
	return &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: b.roleGroupInfo.GetFullName(),
		},
	}
}

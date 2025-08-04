package journal

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	"github.com/zncdatadev/hdfs-operator/internal/controller/journal/container"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	opClient "github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Compile-time check to ensure JournalnodeStatefulSetBuilder implements StatefulSetComponentBuilder
var _ common.StatefulSetComponentBuilder = (*JournalnodeStatefulSetBuilder)(nil)

// JournalnodeStatefulSetBuilder inherits from common StatefulSetBuilder and implements journalnode-specific logic
type JournalnodeStatefulSetBuilder struct {
	*common.StatefulSetBuilder
	// journalnode-specific fields
	config        *hdfsv1alpha1.ConfigSpec
	image         *util.Image
	roleGroupInfo *reconciler.RoleGroupInfo
}

// NewJournalnodeStatefulSetBuilder creates a new JournalnodeStatefulSetBuilder that inherits from common StatefulSetBuilder
func NewJournalnodeStatefulSetBuilder(
	ctx context.Context,
	client *opClient.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	image *util.Image,
	replicas *int32,
	config *hdfsv1alpha1.ConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	instance *hdfsv1alpha1.HdfsCluster,
) *JournalnodeStatefulSetBuilder {
	jnStsBuiler := &JournalnodeStatefulSetBuilder{
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
		constant.JournalNode,
		jnStsBuiler,
	)
	jnStsBuiler.StatefulSetBuilder = commonBuilder
	// Set the component to self
	return jnStsBuiler

}

// Build constructs the StatefulSet using the inherited common builder and journalnode-specific component
func (b *JournalnodeStatefulSetBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	// Use the inherited common builder's Build method, passing self as the component builder
	return b.StatefulSetBuilder.Build(ctx)
}

// StatefulSetComponentBuilder interface implementation

// GetName returns the StatefulSet name
func (b *JournalnodeStatefulSetBuilder) GetName() string {
	return b.roleGroupInfo.GetFullName()
}

// GetMainContainers returns the main containers for journalnode
func (b *JournalnodeStatefulSetBuilder) GetMainContainers() []corev1.Container {
	return []corev1.Container{
		b.makeJournalNodeContainer(),
	}
}

// GetInitContainers returns init containers for journalnode
func (b *JournalnodeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	return []corev1.Container{}
}

// GetVolumes returns journalnode-specific volumes
func (b *JournalnodeStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: hdfsv1alpha1.HdfsConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getJournalNodeConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.HdfsLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: b.getJournalNodeConfigMapSource(),
			},
		},
	}
}

// GetVolumeClaimTemplates returns PVCs for journalnode
func (b *JournalnodeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		b.createDataPvcTemplate(),
	}
}

// GetSecurityContext returns the security context for journalnode pods
func (b *JournalnodeStatefulSetBuilder) GetSecurityContext() *corev1.PodSecurityContext {
	// For now, return nil as we'll handle security context in a later iteration
	// This follows the pattern from namenode where security context is handled differently
	return nil
}

// GetServiceAccountName returns the service account name for journalnode
func (b *JournalnodeStatefulSetBuilder) GetServiceAccountName() string {
	return common.CreateServiceAccountName(b.GetInstance().GetName())
}

// Helper methods for container creation

func (b *JournalnodeStatefulSetBuilder) makeJournalNodeContainer() corev1.Container {
	// Use the new container builder pattern like namenode
	journalNodeBuilder := container.NewJournalNodeContainerBuilder(
		b.GetInstance(),
		b.roleGroupInfo,
		b.config.RoleGroupConfigSpec,
		b.image,
	)
	return *journalNodeBuilder.Build()
}

func (b *JournalnodeStatefulSetBuilder) createDataPvcTemplate() corev1.PersistentVolumeClaim {
	// Default storage size
	storageSize := resource.MustParse("10Gi")
	// if b.config != nil && b.config.Resources != nil && b.config.Resources.Storage != nil {
	// 	storageSize = b.config.Resources.Storage.Capacity
	// }
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
func (b *JournalnodeStatefulSetBuilder) GetHttpPort() int32 {
	return common.HttpPort(b.GetInstance().Spec.ClusterConfig, hdfsv1alpha1.JournalNodeHttpsPort, hdfsv1alpha1.JournalNodeHttpPort).ContainerPort
}

func (b *JournalnodeStatefulSetBuilder) getJournalNodeConfigMapSource() *corev1.ConfigMapVolumeSource {
	return &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: b.roleGroupInfo.GetFullName(),
		},
	}
}

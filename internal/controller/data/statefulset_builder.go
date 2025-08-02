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
	mergedCfg       *hdfsv1alpha1.RoleGroupSpec
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *util.Image
	roleGroupInfo   *reconciler.RoleGroupInfo
}

// NewDataNodeStatefulSetBuilder creates a new DataNodeStatefulSetBuilder that inherits from common StatefulSetBuilder
func NewDataNodeStatefulSetBuilder(
	ctx context.Context,
	client *opClient.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	image *util.Image,
	replicas *int32,
	roleConfig *commonsv1alpha1.RoleGroupConfigSpec,
	overrides *commonsv1alpha1.OverridesSpec,
	instance *hdfsv1alpha1.HdfsCluster,
	mergedCfg *hdfsv1alpha1.RoleGroupSpec,
) *DataNodeStatefulSetBuilder {
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
		constant.DataNode,
	)

	return &DataNodeStatefulSetBuilder{
		StatefulSetBuilder: commonBuilder,
		mergedCfg:          mergedCfg,
		roleGroupConfig:    roleConfig,
		image:              image,
		roleGroupInfo:      roleGroupInfo,
	}
}

// Build constructs the StatefulSet using the inherited common builder and datanode-specific component
func (b *DataNodeStatefulSetBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	// Use the inherited common builder's Build method, passing self as the component builder
	return b.StatefulSetBuilder.Build(ctx, b)
}

// StatefulSetComponentBuilder interface implementation

// GetName returns the StatefulSet name
func (b *DataNodeStatefulSetBuilder) GetName() string {
	return b.roleGroupInfo.GetFullName()
}

// GetMainContainers returns the main containers for datanode
func (b *DataNodeStatefulSetBuilder) GetMainContainers() []corev1.Container {
	containers := b.buildContainers()
	return containers
}

// GetInitContainers returns init containers for datanode
func (b *DataNodeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	initContainers := b.buildInitContainers()
	return initContainers
}

// GetVolumes returns datanode-specific volumes
func (b *DataNodeStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return b.buildVolumes()
}

// GetVolumeClaimTemplates returns volume claim templates for DataNode
func (b *DataNodeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	return b.buildVolumeClaimTemplates()
}

// GetSecurityContext returns security context for DataNode pods
func (b *DataNodeStatefulSetBuilder) GetSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsUser:    &[]int64{1000}[0],
		RunAsGroup:   &[]int64{1000}[0],
		RunAsNonRoot: &[]bool{true}[0],
		FSGroup:      &[]int64{1000}[0],
	}
}

// GetServiceAccountName returns service account name for DataNode
func (b *DataNodeStatefulSetBuilder) GetServiceAccountName() string {
	return b.instance.Name + "-datanode"
}

// NewDataNodeStatefulSetBuilder creates a new DataNode StatefulSet builder
func NewDataNodeStatefulSetBuilder(
	client *client.Client,
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	vectorAggregatorConfigMapName string,
) *DataNodeStatefulSetBuilder {
	return &DataNodeStatefulSetBuilder{
		client:                        client,
		instance:                      instance,
		roleGroupInfo:                 roleGroupInfo,
		roleGroupConfig:               roleGroupConfig,
		vectorAggregatorConfigMapName: vectorAggregatorConfigMapName,
	}
}

// Build builds the DataNode StatefulSet
func (b *DataNodeStatefulSetBuilder) Build(ctx context.Context) (*appsv1.StatefulSet, error) {
	// Build DataNode containers
	containers := b.buildContainers()

	// Build init containers
	initContainers := b.buildInitContainers()

	// Create StatefulSet
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      b.roleGroupInfo.GetFullName(),
			Namespace: b.instance.Namespace,
			Labels:    b.buildLabels(),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &[]int32{1}[0], // Default to 1 replica
			ServiceName: b.roleGroupInfo.GetFullName(),
			Selector: &metav1.LabelSelector{
				MatchLabels: b.buildSelectorLabels(),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: b.buildLabels(),
				},
				Spec: corev1.PodSpec{
					Containers:     containers,
					InitContainers: initContainers,
					Volumes:        b.buildVolumes(),
				},
			},
			VolumeClaimTemplates: b.buildVolumeClaimTemplates(),
		},
	}

	return sts, nil
}

// buildContainers builds the main containers for DataNode
func (b *DataNodeStatefulSetBuilder) buildContainers() []corev1.Container {
	image := b.instance.Spec.Image

	// Build DataNode container (passing ImageSpec directly for now)
	dataNodeBuilder := container.NewDataNodeContainerBuilder(
		b.instance,
		b.roleGroupInfo,
		b.roleGroupConfig,
		image,
	)
	dataNodeContainer := dataNodeBuilder.Build()

	containers := []corev1.Container{*dataNodeContainer}

	// Add vector container if enabled
	if b.vectorAggregatorConfigMapName != "" {
		vectorContainer := b.buildVectorContainer()
		containers = append(containers, *vectorContainer)
	}

	return containers
}

// buildInitContainers builds init containers for DataNode
func (b *DataNodeStatefulSetBuilder) buildInitContainers() []corev1.Container {
	image := b.instance.Spec.Image

	// Build wait-for-namenodes init container
	waitForNameNodesBuilder := container.NewWaitForNameNodesContainerBuilder(
		b.instance,
		b.roleGroupInfo,
		b.roleGroupConfig,
		image,
	)
	waitForNameNodesContainer := waitForNameNodesBuilder.Build()

	return []corev1.Container{*waitForNameNodesContainer}
}

// buildVolumes builds volumes for DataNode StatefulSet
func (b *DataNodeStatefulSetBuilder) buildVolumes() []corev1.Volume {
	volumes := []corev1.Volume{
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
	}
}

// GetVolumeClaimTemplates returns PVCs for datanode
func (b *DataNodeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		b.createDataPvcTemplate(),
		b.createListenPvcTemplate(),
	}
}

// GetSecurityContext returns the security context for datanode pods
func (b *DataNodeStatefulSetBuilder) GetSecurityContext() *corev1.PodSecurityContext {
	// For now, return nil as we'll handle security context in a later iteration
	// This follows the pattern from namenode where security context is handled differently
	return nil
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
		b.roleGroupConfig,
		b.image,
	)
	return *dataNode.Build()
}

func (b *DataNodeStatefulSetBuilder) makeWaitForNameNodesContainer() corev1.Container {
	waitForNameNodes := container.NewWaitForNameNodesContainerBuilder(
		b.GetInstance(),
		b.GetRoleGroupInfo(),
		b.roleGroupConfig,
		b.GetInstance().Spec.Image,
	)
	return *waitForNameNodes.Build()
}

func (b *DataNodeStatefulSetBuilder) createDataPvcTemplate() corev1.PersistentVolumeClaim {
	storageSize := b.roleGroupConfig.Resources.Storage.Capacity
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

func (b *DataNodeStatefulSetBuilder) createListenPvcTemplate() corev1.PersistentVolumeClaim {
	var listenerClass constants.ListenerClass
	listenerClassSpec := b.mergedCfg.Config.ListenerClass
	if listenerClassSpec == nil || *listenerClassSpec == "" {
		listenerClass = constants.ClusterInternal
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

func (b *DataNodeStatefulSetBuilder) getDataNodeConfigMapSource() *corev1.ConfigMapVolumeSource {
	return &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: b.roleGroupInfo.GetFullName(),
		},
	}
}

// package data

// import (
// 	"context"

// 	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
// 	"github.com/zncdatadev/hdfs-operator/internal/common"
// 	"github.com/zncdatadev/hdfs-operator/internal/controller/data/container"
// 	"github.com/zncdatadev/operator-go/pkg/constants"
// 	"github.com/zncdatadev/operator-go/pkg/util"
// 	appv1 "k8s.io/api/apps/v1"
// 	corev1 "k8s.io/api/core/v1"
// 	"k8s.io/apimachinery/pkg/api/resource"
// 	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/apimachinery/pkg/runtime"
// 	"sigs.k8s.io/controller-runtime/pkg/client"
// )

// type StatefulSetReconciler struct {
// 	common.WorkloadStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.DataNodeRoleGroupSpec]
// }

// func NewStatefulSet(
// 	scheme *runtime.Scheme,
// 	instance *hdfsv1alpha1.HdfsCluster,
// 	client client.Client,
// 	groupName string,
// 	labels map[string]string,
// 	mergedCfg *hdfsv1alpha1.DataNodeRoleGroupSpec,
// 	replicate int32,
// 	image *util.Image,
// ) *StatefulSetReconciler {
// 	return &StatefulSetReconciler{
// 		WorkloadStyleReconciler: *common.NewWorkloadStyleReconciler(
// 			scheme,
// 			instance,
// 			client,
// 			groupName,
// 			labels,
// 			mergedCfg,
// 			replicate,
// 			image,
// 		),
// 	}
// }

// func (s *StatefulSetReconciler) Build(ctx context.Context) (client.Object, error) {
// 	sts := &appv1.StatefulSet{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      createStatefulSetName(s.Instance.GetName(), s.GroupName),
// 			Namespace: s.Instance.GetNamespace(),
// 			Labels:    s.MergedLabels,
// 		},
// 		Spec: appv1.StatefulSetSpec{
// 			ServiceName: createServiceName(s.Instance.GetName(), s.GroupName),
// 			Replicas:    s.getReplicates(),
// 			Selector:    &metav1.LabelSelector{MatchLabels: s.MergedLabels},
// 			Template: corev1.PodTemplateSpec{
// 				ObjectMeta: metav1.ObjectMeta{
// 					Labels: s.MergedLabels,
// 				},
// 				Spec: corev1.PodSpec{

// 					ServiceAccountName: common.CreateServiceAccountName(s.Instance.GetName()),
// 					SecurityContext:    s.MergedCfg.Config.SecurityContext,
// 					Containers: []corev1.Container{
// 						s.makeDataNodeContainer(),
// 					},
// 					InitContainers: []corev1.Container{
// 						s.makeWaitNameNodeContainer(),
// 					},
// 					Volumes: s.makeVolumes(),
// 				},
// 			},
// 			VolumeClaimTemplates: s.makePvcTemplates(),
// 		},
// 	}

// 	isVectorEnabled, err := common.IsVectorEnable(s.MergedCfg.Config.Logging)
// 	if err != nil {
// 		return nil, err
// 	} else if isVectorEnabled {
// 		vectorFactory := common.GetVectorFactory(s.Image)
// 		sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, *vectorFactory.GetContainer())
// 		sts.Spec.Template.Spec.Volumes = append(sts.Spec.Template.Spec.Volumes, vectorFactory.GetVolumes()...)
// 	}
// 	if s.Instance.Spec.ClusterConfig.Authentication != nil && s.Instance.Spec.ClusterConfig.Authentication.AuthenticationClass != "" {
// 		oidcContainer, err := common.MakeOidcContainer(ctx, s.Client, s.Instance, s.getHttpPort(), s.Image)
// 		if err != nil {
// 			return nil, err
// 		}
// 		if oidcContainer != nil {
// 			sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, *oidcContainer)
// 		}
// 	}
// 	return sts, nil
// }

// func (s *StatefulSetReconciler) getHttpPort() int32 {
// 	return common.HttpPort(s.Instance.Spec.ClusterConfig, hdfsv1alpha1.DataNodeHttpsPort, hdfsv1alpha1.DataNodeHttpPort).ContainerPort
// }

// func (s *StatefulSetReconciler) SetAffinity(resource client.Object) {
// 	dep := resource.(*appv1.StatefulSet)
// 	if affinity := s.MergedCfg.Config.Affinity; affinity != nil {
// 		dep.Spec.Template.Spec.Affinity = affinity
// 	} else {
// 		dep.Spec.Template.Spec.Affinity = common.AffinityDefault(common.DataNode, s.Instance.GetName())
// 	}
// }

// func (s *StatefulSetReconciler) CommandOverride(resource client.Object) {
// 	dep := resource.(*appv1.StatefulSet)
// 	containers := dep.Spec.Template.Spec.Containers
// 	if cmdOverride := s.MergedCfg.CliOverrides; cmdOverride != nil {
// 		for i := range containers {
// 			if containers[i].Name == string(container.DataNode) {
// 				containers[i].Command = cmdOverride
// 				break
// 			}
// 		}
// 	}
// }

// func (s *StatefulSetReconciler) EnvOverride(resource client.Object) {
// 	dep := resource.(*appv1.StatefulSet)
// 	containers := dep.Spec.Template.Spec.Containers
// 	if envOverride := s.MergedCfg.EnvOverrides; envOverride != nil {
// 		for i := range containers {
// 			if containers[i].Name == string(container.DataNode) {
// 				envVars := containers[i].Env
// 				common.OverrideEnvVars(&envVars, s.MergedCfg.EnvOverrides)
// 				break
// 			}

// 		}
// 	}
// }

// func (s *StatefulSetReconciler) LogOverride(_ client.Object) {
// 	// do nothing, see name node
// }

// // make name node container
// func (s *StatefulSetReconciler) makeDataNodeContainer() corev1.Container {
// 	dateNode := container.NewDataNodeContainerBuilder(
// 		s.Instance,
// 		*common.ConvertToResourceRequirements(common.GetContainerResource(container.GetRole(), string(container.DataNode))),
// 		s.Image,
// 	)
// 	return dateNode.Build(dateNode)
// }

// // make format name node container
// func (s *StatefulSetReconciler) makeWaitNameNodeContainer() corev1.Container {
// 	initContainer := container.NewWaitNameNodeContainerBuilder(
// 		s.Instance,
// 		*common.ConvertToResourceRequirements(common.GetContainerResource(container.GetRole(), string(container.WaitNameNode))),
// 		s.GroupName,
// 		s.Image,
// 	)
// 	return initContainer.Build(initContainer)
// }

// // make volumes
// func (s *StatefulSetReconciler) makeVolumes() []corev1.Volume {
// 	volumes := common.GetCommonVolumes(s.Instance.Spec.ClusterConfig, s.Instance.GetName(), container.GetRole())
// 	datanodeVolumes := []corev1.Volume{
// 		{
// 			Name: hdfsv1alpha1.HdfsConfigVolumeMountName,
// 			VolumeSource: corev1.VolumeSource{
// 				ConfigMap: s.getConfigMapSource(),
// 			},
// 		},
// 		{
// 			Name: hdfsv1alpha1.HdfsLogVolumeMountName,
// 			VolumeSource: corev1.VolumeSource{
// 				ConfigMap: s.getConfigMapSource(),
// 			},
// 		},
// 		{
// 			Name: hdfsv1alpha1.WaitForNamenodesConfigVolumeMountName,
// 			VolumeSource: corev1.VolumeSource{
// 				ConfigMap: s.getConfigMapSource(),
// 			},
// 		},
// 		{
// 			Name: hdfsv1alpha1.WaitForNamenodesLogVolumeMountName,
// 			VolumeSource: corev1.VolumeSource{
// 				ConfigMap: s.getConfigMapSource(),
// 			},
// 		},
// 		{
// 			Name: hdfsv1alpha1.ListenerVolumeName,
// 			VolumeSource: corev1.VolumeSource{
// 				Ephemeral: &corev1.EphemeralVolumeSource{
// 					VolumeClaimTemplate: s.createListenPvcTemplate(),
// 				},
// 			},
// 		},
// 	}
// 	return append(volumes, datanodeVolumes...)
// }

// func (s *StatefulSetReconciler) makePvcTemplates() []corev1.PersistentVolumeClaim {
// 	return []corev1.PersistentVolumeClaim{
// 		s.createDataPvcTemplate(),
// 	}
// }

// // create data pvc template
// func (s *StatefulSetReconciler) createDataPvcTemplate() corev1.PersistentVolumeClaim {
// 	storageSize := s.MergedCfg.Config.Resources.Storage.Capacity
// 	return corev1.PersistentVolumeClaim{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name: hdfsv1alpha1.DataVolumeMountName,
// 		},
// 		Spec: corev1.PersistentVolumeClaimSpec{
// 			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
// 			VolumeMode:  func() *corev1.PersistentVolumeMode { v := corev1.PersistentVolumeFilesystem; return &v }(),
// 			Resources: corev1.VolumeResourceRequirements{
// 				Requests: corev1.ResourceList{
// 					corev1.ResourceStorage: storageSize,
// 				},
// 			},
// 		},
// 	}
// }

// // create listen pvc template
// func (s *StatefulSetReconciler) createListenPvcTemplate() *corev1.PersistentVolumeClaimTemplate {
// 	listenerClass := constants.ListenerClass(s.MergedCfg.Config.ListenerClass)
// 	if listenerClass == "" {
// 		listenerClass = constants.ClusterInternal
// 	}
// 	return &corev1.PersistentVolumeClaimTemplate{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Annotations: common.GetListenerLabels(listenerClass),
// 		},
// 		Spec: corev1.PersistentVolumeClaimSpec{
// 			StorageClassName: constants.ListenerStorageClassPtr(),
// 			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
// 			Resources: corev1.VolumeResourceRequirements{
// 				Requests: corev1.ResourceList{
// 					corev1.ResourceStorage: resource.MustParse(common.ListenerPvcStorage),
// 				},
// 			},
// 		},
// 	}
// }

// func (s *StatefulSetReconciler) getReplicates() *int32 {
// 	return &s.MergedCfg.Replicas
// }

// func (s *StatefulSetReconciler) getConfigMapSource() *corev1.ConfigMapVolumeSource {
// 	return &corev1.ConfigMapVolumeSource{
// 		LocalObjectReference: corev1.LocalObjectReference{
// 			Name: createConfigName(s.Instance.GetName(), s.GroupName)}}
// }

// func (s *StatefulSetReconciler) GetConditions() *[]metav1.Condition {
// 	return &s.Instance.Status.Conditions
// }

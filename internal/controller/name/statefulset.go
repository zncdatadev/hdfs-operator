package name

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/controller/name/container"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StatefulSetReconciler struct {
	common.WorkloadStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.NameNodeRoleGroupSpec]
}

func (s *StatefulSetReconciler) GetConditions() *[]metav1.Condition {
	return &s.Instance.Status.Conditions
}

// NewStatefulSetController new a StatefulSetReconciler

func NewStatefulSet(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.NameNodeRoleGroupSpec,
	replicate int32,
) *StatefulSetReconciler {
	return &StatefulSetReconciler{
		WorkloadStyleReconciler: *common.NewWorkloadStyleReconciler(
			scheme,
			instance,
			client,
			groupName,
			labels,
			mergedCfg,
			replicate,
		),
	}
}

func (s *StatefulSetReconciler) Build(_ context.Context) (client.Object, error) {
	sts := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      common.CreateNameNodeStatefulSetName(s.Instance.GetName(), s.GroupName),
			Namespace: s.Instance.GetNamespace(),
			Labels:    s.MergedLabels,
		},
		Spec: appv1.StatefulSetSpec{
			ServiceName: createServiceName(s.Instance.GetName(), s.GroupName),
			Replicas:    s.getReplicates(),
			Selector:    &metav1.LabelSelector{MatchLabels: s.MergedLabels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: s.MergedLabels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: common.CreateServiceAccountName(s.Instance.GetName()),
					SecurityContext:    s.MergedCfg.Config.SecurityContext,
					Containers: []corev1.Container{
						s.makeNameNodeContainer(),
						s.makeZkfcContainer(),
					},
					InitContainers: []corev1.Container{
						s.makeFormatNameNodeContainer(),
						s.makeFormatZookeeperContainer(),
					},
					Volumes: s.makeVolumes(),
				},
			},
			VolumeClaimTemplates: s.makePvcTemplates(),
		},
	}

	isVectorEnabled, err := common.IsVectorEnable(s.MergedCfg.Config.Logging)
	if err != nil {
		return nil, err
	} else if isVectorEnabled {
		common.ExtendStatefulSetByVector(nil, sts, createConfigName(s.Instance.GetName(), s.GroupName))
	}

	return sts, nil
}

func (s *StatefulSetReconciler) SetAffinity(resource client.Object) {
	dep := resource.(*appv1.StatefulSet)
	if affinity := s.MergedCfg.Config.Affinity; affinity != nil {
		dep.Spec.Template.Spec.Affinity = affinity
	} else {
		dep.Spec.Template.Spec.Affinity = common.AffinityDefault(common.NameNode, s.Instance.GetName())
	}
}

func (s *StatefulSetReconciler) CommandOverride(resource client.Object) {
	dep := resource.(*appv1.StatefulSet)
	containers := dep.Spec.Template.Spec.Containers
	if cmdOverride := s.MergedCfg.CommandArgsOverrides; cmdOverride != nil {
		for i := range containers {
			if containers[i].Name == string(container.NameNode) {
				containers[i].Command = cmdOverride
				break
			}
		}
	}
}

func (s *StatefulSetReconciler) EnvOverride(resource client.Object) {
	dep := resource.(*appv1.StatefulSet)
	containers := dep.Spec.Template.Spec.Containers
	if envOverride := s.MergedCfg.EnvOverrides; envOverride != nil {
		for i := range containers {
			if containers[i].Name == string(container.NameNode) {
				envVars := containers[i].Env
				common.OverrideEnvVars(&envVars, s.MergedCfg.EnvOverrides)
				break
			}
		}
	}
}

func (s *StatefulSetReconciler) LogOverride(_ client.Object) {
	//because of existing log configuration, it will not use to override
	//see common.OverrideExistLoggingRecociler
}

// make name node container
func (s *StatefulSetReconciler) makeNameNodeContainer() corev1.Container {
	nameNode := container.NewNameNodeContainerBuilder(s.Instance,
		*common.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
	)
	return nameNode.Build(nameNode)
}

// make zkfc container
func (s *StatefulSetReconciler) makeZkfcContainer() corev1.Container {
	zkfc := container.NewZkfcContainerBuilder(
		s.Instance,
		*common.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
	)
	return zkfc.Build(zkfc)
}

// make format name node container
func (s *StatefulSetReconciler) makeFormatNameNodeContainer() corev1.Container {
	formatNameNode := container.NewFormatNameNodeContainerBuilder(
		s.Instance,
		*common.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
		*s.getReplicates(),
		common.CreateNameNodeStatefulSetName(s.Instance.GetName(), s.GroupName),
	)
	return formatNameNode.Build(formatNameNode)
}

// make format zookeeper container
func (s *StatefulSetReconciler) makeFormatZookeeperContainer() corev1.Container {
	formatZookeeper := container.NewFormatZookeeperContainerBuilder(
		s.Instance,
		*common.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
		s.getZookeeperConfigMapName(),
	)
	return formatZookeeper.Build(formatZookeeper)
}

// make volumes
func (s *StatefulSetReconciler) makeVolumes() []corev1.Volume {
	volumes := common.GetCommonVolumes(s.Instance.Spec.ClusterConfigSpec, s.Instance.GetName(), container.GetRole())
	nameNodeVolumes := []corev1.Volume{
		{
			Name: container.NameNodeConfVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.NameNodeLogVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.ZkfcVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.ZkfcLogVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.FormatNameNodeVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.FormatNameNodeLogVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.FormatZookeeperVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.FormatZookeeperLogVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
	}
	return append(volumes, nameNodeVolumes...)
}

func (s *StatefulSetReconciler) makePvcTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		s.createDataPvcTemplate(),
		s.createListenPvcTemplate(),
	}
}

// create data pvc template
func (s *StatefulSetReconciler) createDataPvcTemplate() corev1.PersistentVolumeClaim {
	storageSize := s.MergedCfg.Config.Resources.Storage.Capacity
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: container.DataVolumeName(),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  func() *corev1.PersistentVolumeMode { v := corev1.PersistentVolumeFilesystem; return &v }(),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: *storageSize,
				},
			},
		},
	}
}

// create listen pvc template
func (s *StatefulSetReconciler) createListenPvcTemplate() corev1.PersistentVolumeClaim {
	listenerClass := s.MergedCfg.Config.ListenerClass
	if listenerClass == "" {
		listenerClass = string(common.LoadBalancerClass)
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        container.ListenerVolumeName(),
			Annotations: common.GetListenerLabels(common.ListenerClass(listenerClass)), // important-1!!!!!
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  func() *corev1.PersistentVolumeMode { v := corev1.PersistentVolumeFilesystem; return &v }(),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(common.ListenerPvcStorage),
				},
			},
			StorageClassName: func() *string { v := common.ListenerStorageClass; return &v }(), // important-2!!!!!
		},
	}
}

func (s *StatefulSetReconciler) getReplicates() *int32 {
	return &s.MergedCfg.Replicas
}

func (s *StatefulSetReconciler) getNameNodeConfigMapSource() *corev1.ConfigMapVolumeSource {
	return &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: createConfigName(s.Instance.GetName(), s.GroupName)}}
}

// get zookeeper discovery znode
func (s *StatefulSetReconciler) getZookeeperConfigMapName() string {
	return s.Instance.Spec.ClusterConfigSpec.ZookeeperConfigMapName
}

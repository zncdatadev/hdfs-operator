package name

import (
	"context"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	"github.com/zncdata-labs/hdfs-operator/internal/controller/name/container"
	"github.com/zncdata-labs/hdfs-operator/internal/util"
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

func (s *StatefulSetReconciler) Build(ctx context.Context) (client.Object, error) {
	return &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      createStatefulSetName(s.Instance.GetName(), s.GroupName),
			Namespace: s.Instance.GetNamespace(),
			Labels:    s.MergedLabels,
		},
		Spec: appv1.StatefulSetSpec{
			Replicas: s.getReplicates(),
			Selector: &metav1.LabelSelector{MatchLabels: s.MergedLabels},
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
	}, nil
}
func (s *StatefulSetReconciler) CommandOverride(resource client.Object) {
	//TODO implement me
	//panic("implement me")
}

func (s *StatefulSetReconciler) EnvOverride(resource client.Object) {
	//TODO implement me
	//panic("implement me")
}

func (s *StatefulSetReconciler) LogOverride(resource client.Object) {
	//TODO implement me
	//panic("implement me")
}

// make name node container
func (s *StatefulSetReconciler) makeNameNodeContainer() corev1.Container {
	image := s.getImageSpec()
	nameNode := container.NewNameNodeContainerBuilder(
		util.ImageRepository(image.Repository, image.Tag),
		image.PullPolicy,
		*util.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
		s.getZookeeperDiscoveryZNode(),
	)
	return nameNode.Build(nameNode)
}

// make zkfc container
func (s *StatefulSetReconciler) makeZkfcContainer() corev1.Container {
	image := s.getImageSpec()
	zkfc := container.NewZkfcContainerBuilder(
		util.ImageRepository(image.Repository, image.Tag),
		image.PullPolicy,
		*util.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
		s.getZookeeperDiscoveryZNode(),
	)
	return zkfc.Build(zkfc)
}

// make format name node container
func (s *StatefulSetReconciler) makeFormatNameNodeContainer() corev1.Container {
	image := s.getImageSpec()
	formatNameNode := container.NewFormatNameNodeContainerBuilder(
		util.ImageRepository(image.Repository, image.Tag),
		image.PullPolicy,
		*util.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
		s.getZookeeperDiscoveryZNode(),
	)
	return formatNameNode.Build(formatNameNode)

}

// make format zookeeper container
func (s *StatefulSetReconciler) makeFormatZookeeperContainer() corev1.Container {
	image := s.getImageSpec()
	formatZookeeper := container.NewFormatZookeeperContainerBuilder(
		util.ImageRepository(image.Repository, image.Tag),
		image.PullPolicy,
		*util.ConvertToResourceRequirements(s.MergedCfg.Config.Resources),
		s.getZookeeperDiscoveryZNode(),
	)
	return formatZookeeper.Build(formatZookeeper)
}

// make volumes
func (s *StatefulSetReconciler) makeVolumes() []corev1.Volume {
	limit := resource.MustParse("150Mi")
	return []corev1.Volume{
		{
			Name: container.LogVolumeName(),
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					SizeLimit: &limit,
				},
			},
		},
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
	}
}

func (s *StatefulSetReconciler) makePvcTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		s.createDataPvcTemplate(),
		s.createListenPvcTemplate(),
	}
}

// create data pvc template
func (s *StatefulSetReconciler) createDataPvcTemplate() corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: container.DataVolumeName(),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("2Gi"),
				},
			},
		},
	}
}

// create listen pvc template
func (s *StatefulSetReconciler) createListenPvcTemplate() corev1.PersistentVolumeClaim {
	listenerClass := s.MergedCfg.Config.ListenerClass
	if listenerClass == "" {
		listenerClass = string(common.ClusterIp)
	}

	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        container.ListenerVolumeName(),
			Annotations: common.GetListenerLabels(common.ListenerClass(listenerClass)), // important-1!!!!!
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
			StorageClassName: func() *string { v := common.ListenerStorageClass; return &v }(), // important-2!!!!!
		},
	}
}

func (s *StatefulSetReconciler) getReplicates() *int32 {
	return &s.MergedCfg.Replicas
}

// get image spec
func (s *StatefulSetReconciler) getImageSpec() *hdfsv1alpha1.ImageSpec {
	return s.Instance.Spec.Image
}

func (s *StatefulSetReconciler) getNameNodeConfigMapSource() *corev1.ConfigMapVolumeSource {
	return &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: createConfigName(s.Instance.GetName(), s.GroupName)}}
}

// get zookeeper discovery znode
func (s *StatefulSetReconciler) getZookeeperDiscoveryZNode() string {
	return s.Instance.Spec.ClusterConfigSpec.ZookeeperDiscoveryZNode
}

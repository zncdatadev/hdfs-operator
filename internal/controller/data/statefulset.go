package data

import (
	"context"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	"github.com/zncdata-labs/hdfs-operator/internal/controller/data/container"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StatefulSetReconciler struct {
	common.WorkloadStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.RoleGroupSpec]
}

// NewStatefulSetController new a StatefulSetReconciler

func NewStatefulSet(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.RoleGroupSpec,
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
					SecurityContext: s.MergedCfg.Config.SecurityContext,
					Containers: []corev1.Container{
						s.makeDataNodeContainer(),
					},
					InitContainers: []corev1.Container{
						s.makeWaitNameNodeContainer(),
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
func (s *StatefulSetReconciler) makeDataNodeContainer() corev1.Container {
	panic("implement me")
}

// make format name node container
func (s *StatefulSetReconciler) makeWaitNameNodeContainer() corev1.Container {
	panic("implement me")
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
			Name: container.DataNodeConfVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.DataNodeLogVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.WaitNameNodeConfigVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.WaitNameNodeLogVolumeName(),
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getNameNodeConfigMapSource(),
			},
		},
		{
			Name: container.ListenerVolumeName(),
			VolumeSource: corev1.VolumeSource{
				Ephemeral: &corev1.EphemeralVolumeSource{
					VolumeClaimTemplate: s.createListenPvcTemplate(),
				},
			},
		},
	}
}

func (s *StatefulSetReconciler) makePvcTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		s.createDataPvcTemplate(),
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
func (s *StatefulSetReconciler) createListenPvcTemplate() *corev1.PersistentVolumeClaimTemplate {
	listenerClass := s.MergedCfg.Config.ListenerClass
	if listenerClass == "" {
		listenerClass = string(common.NodePort)
	}
	return &corev1.PersistentVolumeClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: common.GetListenerLabels(common.ListenerClass(listenerClass)), // important-1!!!!
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: func() *string { c := common.ListenerStorageClass; return &c }(), // important-2!!!!
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
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

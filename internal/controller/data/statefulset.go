package data

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/controller/data/container"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/util"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StatefulSetReconciler struct {
	common.WorkloadStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.DataNodeRoleGroupSpec]
}

func NewStatefulSet(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.DataNodeRoleGroupSpec,
	replicate int32,
	image *util.Image,
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
			image,
		),
	}
}

func (s *StatefulSetReconciler) Build(ctx context.Context) (client.Object, error) {
	sts := &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      createStatefulSetName(s.Instance.GetName(), s.GroupName),
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
	}

	isVectorEnabled, err := common.IsVectorEnable(s.MergedCfg.Config.Logging)
	if err != nil {
		return nil, err
	} else if isVectorEnabled {
		common.ExtendStatefulSetByVector(nil, sts, s.Image, createConfigName(s.Instance.GetName(), s.GroupName))
	}
	if s.Instance.Spec.ClusterConfig.Authentication != nil && s.Instance.Spec.ClusterConfig.Authentication.AuthenticationClass != "" {
		oidcContainer, err := common.MakeOidcContainer(ctx, s.Client, s.Instance, s.getHttpPort(), s.Image)
		if err != nil {
			return nil, err
		}
		if oidcContainer != nil {
			sts.Spec.Template.Spec.Containers = append(sts.Spec.Template.Spec.Containers, *oidcContainer)
		}
	}
	return sts, nil
}

func (s *StatefulSetReconciler) getHttpPort() int32 {
	return common.HttpPort(s.Instance.Spec.ClusterConfig, hdfsv1alpha1.DataNodeHttpsPort, hdfsv1alpha1.DataNodeHttpPort).ContainerPort
}

func (s *StatefulSetReconciler) SetAffinity(resource client.Object) {
	dep := resource.(*appv1.StatefulSet)
	if affinity := s.MergedCfg.Config.Affinity; affinity != nil {
		dep.Spec.Template.Spec.Affinity = affinity
	} else {
		dep.Spec.Template.Spec.Affinity = common.AffinityDefault(common.DataNode, s.Instance.GetName())
	}
}

func (s *StatefulSetReconciler) CommandOverride(resource client.Object) {
	dep := resource.(*appv1.StatefulSet)
	containers := dep.Spec.Template.Spec.Containers
	if cmdOverride := s.MergedCfg.CliOverrides; cmdOverride != nil {
		for i := range containers {
			if containers[i].Name == string(container.DataNode) {
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
			if containers[i].Name == string(container.DataNode) {
				envVars := containers[i].Env
				common.OverrideEnvVars(&envVars, s.MergedCfg.EnvOverrides)
				break
			}

		}
	}
}

func (s *StatefulSetReconciler) LogOverride(_ client.Object) {
	// do nothing, see name node
}

// make name node container
func (s *StatefulSetReconciler) makeDataNodeContainer() corev1.Container {
	dateNode := container.NewDataNodeContainerBuilder(
		s.Instance,
		*common.ConvertToResourceRequirements(common.GetContainerResource(container.GetRole(), string(container.DataNode))),
		s.Image,
	)
	return dateNode.Build(dateNode)
}

// make format name node container
func (s *StatefulSetReconciler) makeWaitNameNodeContainer() corev1.Container {
	initContainer := container.NewWaitNameNodeContainerBuilder(
		s.Instance,
		*common.ConvertToResourceRequirements(common.GetContainerResource(container.GetRole(), string(container.WaitNameNode))),
		s.GroupName,
		s.Image,
	)
	return initContainer.Build(initContainer)
}

// make volumes
func (s *StatefulSetReconciler) makeVolumes() []corev1.Volume {
	volumes := common.GetCommonVolumes(s.Instance.Spec.ClusterConfig, s.Instance.GetName(), container.GetRole())
	datanodeVolumes := []corev1.Volume{
		{
			Name: hdfsv1alpha1.HdfsConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.HdfsLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.WaitForNamenodesConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.WaitForNamenodesLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: s.getConfigMapSource(),
			},
		},
		{
			Name: hdfsv1alpha1.ListenerVolumeName,
			VolumeSource: corev1.VolumeSource{
				Ephemeral: &corev1.EphemeralVolumeSource{
					VolumeClaimTemplate: s.createListenPvcTemplate(),
				},
			},
		},
	}
	return append(volumes, datanodeVolumes...)
}

func (s *StatefulSetReconciler) makePvcTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		s.createDataPvcTemplate(),
	}
}

// create data pvc template
func (s *StatefulSetReconciler) createDataPvcTemplate() corev1.PersistentVolumeClaim {
	storageSize := s.MergedCfg.Config.Resources.Storage.Capacity
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

// create listen pvc template
func (s *StatefulSetReconciler) createListenPvcTemplate() *corev1.PersistentVolumeClaimTemplate {
	listenerClass := constants.ListenerClass(s.MergedCfg.Config.ListenerClass)
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

func (s *StatefulSetReconciler) getReplicates() *int32 {
	return &s.MergedCfg.Replicas
}

func (s *StatefulSetReconciler) getConfigMapSource() *corev1.ConfigMapVolumeSource {
	return &corev1.ConfigMapVolumeSource{
		LocalObjectReference: corev1.LocalObjectReference{
			Name: createConfigName(s.Instance.GetName(), s.GroupName)}}
}

func (s *StatefulSetReconciler) GetConditions() *[]metav1.Condition {
	return &s.Instance.Status.Conditions
}

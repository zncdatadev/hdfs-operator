package journal

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type StatefulSetReconciler struct {
	common.WorkloadStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.JournalNodeRoleGroupSpec]
}

func NewStatefulSet(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.JournalNodeRoleGroupSpec,
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
						s.makeJournalNodeContainer(),
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
		img := hdfsv1alpha1.TransformImage(s.Instance.Spec.Image)
		common.ExtendStatefulSetByVector(nil, sts, img, createConfigName(s.Instance.GetName(), s.GroupName))
	}
	if s.Instance.Spec.ClusterConfig.Authentication != nil && s.Instance.Spec.ClusterConfig.Authentication.AuthenticationClass != "" {
		oidcContainer, err := common.MakeOidcContainer(ctx, s.Client, s.Instance, s.getHttpPort())
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
	return common.HttpPort(s.Instance.Spec.ClusterConfig, hdfsv1alpha1.JournalNodeHttpsPort, hdfsv1alpha1.JournalNodeHttpPort).ContainerPort
}

func (s *StatefulSetReconciler) SetAffinity(resource client.Object) {
	dep := resource.(*appv1.StatefulSet)
	if affinity := s.MergedCfg.Config.Affinity; affinity != nil {
		dep.Spec.Template.Spec.Affinity = affinity
	} else {
		dep.Spec.Template.Spec.Affinity = common.AffinityDefault(common.JournalNode, s.Instance.GetName())
	}
}

func (s *StatefulSetReconciler) CommandOverride(resource client.Object) {
	dep := resource.(*appv1.StatefulSet)
	containers := dep.Spec.Template.Spec.Containers
	if cmdOverride := s.MergedCfg.CliOverrides; cmdOverride != nil {
		for i := range containers {
			if containers[i].Name == string(ContainerJournalNode) {
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
			if containers[i].Name == string(ContainerJournalNode) {
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
func (s *StatefulSetReconciler) makeJournalNodeContainer() corev1.Container {
	journalNode := NewJournalNodeContainerBuilder(
		s.Instance,
		*common.ConvertToResourceRequirements(common.GetContainerResource(GetRole(), string(ContainerJournalNode))),
	)
	return journalNode.Build(journalNode)
}

// make volumes
func (s *StatefulSetReconciler) makeVolumes() []corev1.Volume {
	volumes := common.GetCommonVolumes(s.Instance.Spec.ClusterConfig, s.Instance.GetName(), GetRole())
	journalNodeVolumes := []corev1.Volume{
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
	}
	return append(volumes, journalNodeVolumes...)
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
			Name: hdfsv1alpha1.DataVolumeMountName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeMode:  func() *corev1.PersistentVolumeMode { v := corev1.PersistentVolumeFilesystem; return &v }(),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: s.MergedCfg.Config.Resources.Storage.Capacity,
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

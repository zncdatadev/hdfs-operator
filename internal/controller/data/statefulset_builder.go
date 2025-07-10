package data

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/controller/data/container"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DataNodeStatefulSetBuilder builds StatefulSet for DataNode
type DataNodeStatefulSetBuilder struct {
	client                        *client.Client
	instance                      *hdfsv1alpha1.HdfsCluster
	roleGroupInfo                 *reconciler.RoleGroupInfo
	roleGroupConfig               *commonsv1alpha1.RoleGroupConfigSpec
	vectorAggregatorConfigMapName string
}

// StatefulSetComponentBuilder interface for DataNode StatefulSet
var _ common.StatefulSetComponentBuilder = &DataNodeStatefulSetBuilder{}

// GetName returns the StatefulSet name
func (b *DataNodeStatefulSetBuilder) GetName() string {
	return b.roleGroupInfo.GetFullName()
}

// GetMainContainers returns the main containers for DataNode
func (b *DataNodeStatefulSetBuilder) GetMainContainers() []corev1.Container {
	containers, _ := b.buildContainers()
	return containers
}

// GetInitContainers returns init containers for DataNode
func (b *DataNodeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	initContainers, _ := b.buildInitContainers()
	return initContainers
}

// GetVolumes returns volumes for DataNode
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
	containers, err := b.buildContainers()
	if err != nil {
		return nil, err
	}

	// Build init containers
	initContainers, err := b.buildInitContainers()
	if err != nil {
		return nil, err
	}

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
func (b *DataNodeStatefulSetBuilder) buildContainers() ([]corev1.Container, error) {
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

	return containers, nil
}

// buildInitContainers builds init containers for DataNode
func (b *DataNodeStatefulSetBuilder) buildInitContainers() ([]corev1.Container, error) {
	image := b.instance.Spec.Image

	// Build wait-for-namenodes init container
	waitForNameNodesBuilder := container.NewWaitForNameNodesContainerBuilder(
		b.instance,
		b.roleGroupInfo,
		b.roleGroupConfig,
		image,
	)
	waitForNameNodesContainer := waitForNameNodesBuilder.Build()

	return []corev1.Container{*waitForNameNodesContainer}, nil
}

// buildVolumes builds volumes for DataNode StatefulSet
func (b *DataNodeStatefulSetBuilder) buildVolumes() []corev1.Volume {
	volumes := []corev1.Volume{
		{
			Name: hdfsv1alpha1.HdfsConfigVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: b.roleGroupInfo.GetFullName(),
					},
				},
			},
		},
		{
			Name: hdfsv1alpha1.HdfsLogVolumeMountName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	// Add vector volume if enabled
	if b.vectorAggregatorConfigMapName != "" {
		vectorVolume := corev1.Volume{
			Name: "vector-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: b.vectorAggregatorConfigMapName,
					},
				},
			},
		}
		volumes = append(volumes, vectorVolume)
	}

	return volumes
}

// buildVolumeClaimTemplates builds volume claim templates for DataNode StatefulSet
func (b *DataNodeStatefulSetBuilder) buildVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "datanode-data",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		},
	}
}

// buildVectorContainer builds vector container for logging
func (b *DataNodeStatefulSetBuilder) buildVectorContainer() *corev1.Container {
	return &corev1.Container{
		Name:  "vector",
		Image: "timberio/vector:0.21.0-alpine",
		Args:  []string{"--config", "/etc/vector/vector.yaml"},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "vector-config",
				MountPath: "/etc/vector",
			},
			{
				Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
				MountPath: "/stackable/log",
			},
		},
	}
}

// buildLabels builds labels for DataNode resources
func (b *DataNodeStatefulSetBuilder) buildLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "hdfs",
		"app.kubernetes.io/instance":  b.instance.Name,
		"app.kubernetes.io/component": "datanode",
		"app.kubernetes.io/part-of":   "hdfs-cluster",
	}
}

// buildSelectorLabels builds selector labels for DataNode StatefulSet
func (b *DataNodeStatefulSetBuilder) buildSelectorLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      "hdfs",
		"app.kubernetes.io/instance":  b.instance.Name,
		"app.kubernetes.io/component": "datanode",
	}
}

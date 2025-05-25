package name

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NamenodeStatefulSetBuilder implements the StatefulSetComponentBuilder interface for namenode
type NamenodeStatefulSetBuilder struct {
	scheme      *runtime.Scheme
	client      client.Client
	instance    *hdfsv1alpha1.HdfsCluster
	groupName   string
	labels      map[string]string
	image       *util.Image
	serviceName string
	ctx         context.Context
}

// NewNamenodeStatefulSetBuilder creates a new NamenodeStatefulSetBuilder
func NewNamenodeStatefulSetBuilder(
	ctx context.Context,
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	image *util.Image,
	serviceName string,
) *NamenodeStatefulSetBuilder {
	return &NamenodeStatefulSetBuilder{
		scheme:      scheme,
		client:      client,
		instance:    instance,
		groupName:   groupName,
		labels:      labels,
		image:       image,
		serviceName: serviceName,
		ctx:         ctx,
	}
}

// GetName returns the StatefulSet name
func (b *NamenodeStatefulSetBuilder) GetName() string {
	return b.groupName
}

// GetMainContainers returns the main containers for the namenode
func (b *NamenodeStatefulSetBuilder) GetMainContainers() []corev1.Container {
	// Main container configuration specific to namenode
	container := corev1.Container{
		Name:            "namenode",
		Image:           b.image.String(),
		ImagePullPolicy: b.image.GetPullPolicy(),
		Env: []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.name",
					},
				},
			},
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
			{
				Name: "HOST_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.hostIP",
					},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.namespace",
					},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "rpc",
				ContainerPort: 8020,
				Protocol:      corev1.ProtocolTCP,
			},
			{
				Name:          "http",
				ContainerPort: 9870,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "hdfs-data",
				MountPath: "/hadoop/dfs/name",
			},
			{
				Name:      "hdfs-config",
				MountPath: "/etc/hadoop",
			},
		},
	}

	return []corev1.Container{container}
}

// GetInitContainers returns init containers required by namenode
func (b *NamenodeStatefulSetBuilder) GetInitContainers() []corev1.Container {
	return []corev1.Container{}
}

// GetVolumes returns namenode-specific volumes
func (b *NamenodeStatefulSetBuilder) GetVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "hdfs-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: b.GetName() + "-config",
					},
				},
			},
		},
	}
}

// GetVolumeClaimTemplates returns PVCs for namenode
func (b *NamenodeStatefulSetBuilder) GetVolumeClaimTemplates() []corev1.PersistentVolumeClaim {
	storageSize := "100Gi" // Default storage size
	storageClass := ""     // Default storage class
	
	// Get storage configuration from rolegroup config
	if b.instance.Spec.NameNode != nil {
		if roleGroups := b.instance.Spec.NameNode.RoleGroups; roleGroups != nil {
			for _, roleGroup := range roleGroups {
				if roleGroup.Config != nil {
					if roleGroup.Config.StorageSize != "" {
						storageSize = roleGroup.Config.StorageSize
					}
					if roleGroup.Config.StorageClass != "" {
						storageClass = roleGroup.Config.StorageClass
					}
				}
			}
		}
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "hdfs-data",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(storageSize),
				},
			},
		},
	}
	
	// Set storage class if specified
	if storageClass != "" {
		pvc.Spec.StorageClassName = &storageClass
	}

	return []corev1.PersistentVolumeClaim{pvc}
}

// GetSecurityContext returns the security context for namenode pods
func (b *NamenodeStatefulSetBuilder) GetSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		FSGroup: func() *int64 { i := int64(1000); return &i }(),
	}
}

// GetServiceAccountName returns the service account name for namenode
func (b *NamenodeStatefulSetBuilder) GetServiceAccountName() string {
	return b.instance.GetName() + "-namenode"
}

// BuildStatefulSet builds the namenode StatefulSet
func (b *NamenodeStatefulSetBuilder) BuildStatefulSet(ctx context.Context) (client.Object, error) {
	// Get replicas from the first available rolegroup
	replicas := int32(1) // default replicas
	if b.instance.Spec.NameNode != nil && b.instance.Spec.NameNode.RoleGroups != nil {
		for _, roleGroup := range b.instance.Spec.NameNode.RoleGroups {
			if roleGroup.Replicas > 0 {
				replicas = roleGroup.Replicas
				break
			}
		}
	}

	builder := common.NewStatefulSetBuilder(
		ctx,
		b.scheme,
		b.instance,
		b.client,
		b.groupName,
		b.labels,
		common.RoleType("namenode"), // Convert Role to RoleType
		&replicas,
		b.image,
		b.serviceName,
	)

	return builder.Build(ctx, b)
}

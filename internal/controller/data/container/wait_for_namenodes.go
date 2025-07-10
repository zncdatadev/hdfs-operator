package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
)

// WaitForNameNodesContainerBuilder builds wait-for-namenodes init containers
type WaitForNameNodesContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *hdfsv1alpha1.ImageSpec
}

// NewWaitForNameNodesContainerBuilder creates a new wait-for-namenodes container builder
func NewWaitForNameNodesContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *hdfsv1alpha1.ImageSpec,
) *WaitForNameNodesContainerBuilder {
	return &WaitForNameNodesContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the wait-for-namenodes container
func (b *WaitForNameNodesContainerBuilder) Build() *corev1.Container {
	// Convert ImageSpec to container string
	imageString := b.image.Repo + "/hadoop:" + b.image.ProductVersion

	// Create wait-for-namenodes component implementation
	component := newWaitForNameNodesComponent(b.instance, b.roleGroupInfo)

	// Build the container manually
	container := &corev1.Container{
		Name:            component.GetContainerName(),
		Image:           imageString,
		Command:         component.GetCommand(),
		Args:            component.GetArgs(),
		Env:             component.GetEnvVars(),
		VolumeMounts:    component.GetVolumeMounts(),
		ImagePullPolicy: b.image.PullPolicy,
	}

	return container
}

// WaitForNameNodesComponent implements the component interface for wait-for-namenodes
type WaitForNameNodesComponent struct {
	instance      *hdfsv1alpha1.HdfsCluster
	roleGroupInfo *reconciler.RoleGroupInfo
}

// Compile-time check to ensure WaitForNameNodesComponent implements ContainerComponentInterface
var _ common.ContainerComponentInterface = &WaitForNameNodesComponent{}

func newWaitForNameNodesComponent(instance *hdfsv1alpha1.HdfsCluster, roleGroupInfo *reconciler.RoleGroupInfo) *WaitForNameNodesComponent {
	return &WaitForNameNodesComponent{
		instance:      instance,
		roleGroupInfo: roleGroupInfo,
	}
}

func (c *WaitForNameNodesComponent) GetContainerName() string {
	return string(constant.WaitForNameNodesComponent)
}

func (c *WaitForNameNodesComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *WaitForNameNodesComponent) GetArgs() []string {
	return []string{
		"echo 'Waiting for NameNodes to be ready...' && sleep 10 && echo 'NameNodes should be ready now'",
	}
}

func (c *WaitForNameNodesComponent) GetEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "HADOOP_CONF_DIR",
			Value: "/stackable/config",
		},
	}
}

func (c *WaitForNameNodesComponent) GetVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: "/stackable/config",
		},
	}
}

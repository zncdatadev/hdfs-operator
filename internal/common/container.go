package common

import (
	"strings"

	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	opgoutil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

// Core interface that all container components must implement
type ContainerComponentInterface interface {
	GetContainerName() string
	GetCommand() []string
	GetArgs() []string
	GetEnvVars() []corev1.EnvVar
	GetVolumeMounts() []corev1.VolumeMount
}

// Optional interfaces that components can implement as needed
type ContainerPortsProvider interface {
	GetPorts() []corev1.ContainerPort
}

type ContainerHealthCheckProvider interface {
	GetLivenessProbe() *corev1.Probe
	GetReadinessProbe() *corev1.Probe
}

type ContainerSecretProvider interface {
	GetSecretEnvFrom() string
}

// HdfsContainerBuilder represents the new HDFS container builder
type HdfsContainerBuilder struct {
	*builder.Container
	ZookeeperConfigMapName string
	RoleGroupInfo          *reconciler.RoleGroupInfo
	RoleGroupConfig        *commonsv1alpha1.RoleGroupConfigSpec

	secretEnvfrom string
	envs          []corev1.EnvVar
	ports         []corev1.ContainerPort
	readiness     *corev1.Probe
	liveness      *corev1.Probe
	volumeMounts  []corev1.VolumeMount
	args          []string
	argsScript    string
}

func NewHdfsContainerBuilder(
	container constant.ContainerComponent,
	image *opgoutil.Image,
	zookeeperConfigMapName string,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
) *HdfsContainerBuilder {
	b := &HdfsContainerBuilder{
		Container:              builder.NewContainer(string(container), image),
		ZookeeperConfigMapName: zookeeperConfigMapName,
		RoleGroupInfo:          roleGroupInfo,
		RoleGroupConfig:        roleGroupConfig,
	}
	b.volumeMounts = b.commonVolumeMounts()

	return b
}

// BuildWithComponent builds container with specific component interface
func (c *HdfsContainerBuilder) BuildWithComponent(component ContainerComponentInterface) *corev1.Container {
	if component != nil {
		// Set required properties
		c.envs = component.GetEnvVars()
		c.volumeMounts = append(c.volumeMounts, component.GetVolumeMounts()...)
		c.args = component.GetArgs()
		c.argsScript = strings.Join(c.args, "\n")

		// Set optional properties using type assertions
		if portProvider, ok := component.(ContainerPortsProvider); ok {
			c.ports = portProvider.GetPorts()
		}

		if healthProvider, ok := component.(ContainerHealthCheckProvider); ok {
			c.liveness = healthProvider.GetLivenessProbe()
			c.readiness = healthProvider.GetReadinessProbe()
		}

		if secretProvider, ok := component.(ContainerSecretProvider); ok {
			c.secretEnvfrom = secretProvider.GetSecretEnvFrom()
		}
	}

	c.SetLivenessProbe(c.liveness).
		SetReadinessProbe(c.readiness).
		AddEnvVars(c.envs).
		// AddEnvFromConfigMap(RoleGroupEnvsConfigMapName(c.RoleGroupInfo.GetClusterName())).
		AddPorts(c.ports).
		SetCommand([]string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}).
		AddVolumeMounts(c.volumeMounts).
		SetArgs([]string{c.argsScript})

	if c.secretEnvfrom != "" {
		c.AddEnvFromSecret(c.secretEnvfrom)
	}

	if c.RoleGroupConfig != nil {
		c.SetResources(c.RoleGroupConfig.Resources)
	}

	return c.Build()
}

// commonVolumeMounts returns the common volume mounts for all containers
func (c *HdfsContainerBuilder) commonVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      constant.ListenerVolumeName,
			MountPath: constant.KubedoopListenerDir,
		},
		{
			Name:      constant.KubedoopLogVolumeMountName,
			MountPath: constant.KubedoopLogDirMount,
		},
		// Add other common volume mounts as needed
	}
}

// RoleGroupEnvsConfigMapName generates the role group environment config map name
// func RoleGroupEnvsConfigMapName(clusterName string) string {
// 	return clusterName + "-env"
// }

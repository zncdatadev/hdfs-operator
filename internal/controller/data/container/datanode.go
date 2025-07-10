package container

import (
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// DataNodeContainerBuilder builds datanode containers
type DataNodeContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *hdfsv1alpha1.ImageSpec
}

// NewDataNodeContainerBuilder creates a new datanode container builder
func NewDataNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *hdfsv1alpha1.ImageSpec,
) *DataNodeContainerBuilder {
	return &DataNodeContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the datanode container
func (b *DataNodeContainerBuilder) Build() *corev1.Container {
	// Convert ImageSpec to container string
	imageString := b.image.Repo + "/hadoop:" + b.image.ProductVersion

	// Create datanode component implementation
	component := newDataNodeComponent(b.instance, b.roleGroupInfo)

	// Build the container manually since we don't have the right Image type
	container := &corev1.Container{
		Name:            component.GetContainerName(),
		Image:           imageString,
		Command:         component.GetCommand(),
		Args:            component.GetArgs(),
		Env:             component.GetEnvVars(),
		VolumeMounts:    component.GetVolumeMounts(),
		Ports:           component.GetPorts(),
		LivenessProbe:   component.GetLivenessProbe(),
		ReadinessProbe:  component.GetReadinessProbe(),
		StartupProbe:    component.GetStartupProbe(),
		Resources:       *component.GetResources(),
		SecurityContext: component.GetSecurityContext(),
		ImagePullPolicy: b.image.PullPolicy,
	}

	return container
}

// DataNodeComponent implements the component interface for DataNode
type DataNodeComponent struct {
	instance      *hdfsv1alpha1.HdfsCluster
	roleGroupInfo *reconciler.RoleGroupInfo
}

// Compile-time check to ensure DataNodeComponent implements ContainerComponentInterface
var _ common.ContainerComponentInterface = &DataNodeComponent{}

func newDataNodeComponent(instance *hdfsv1alpha1.HdfsCluster, roleGroupInfo *reconciler.RoleGroupInfo) *DataNodeComponent {
	return &DataNodeComponent{
		instance:      instance,
		roleGroupInfo: roleGroupInfo,
	}
}

func (c *DataNodeComponent) GetContainerName() string {
	return string(constant.DataNodeComponent)
}

func (c *DataNodeComponent) GetCommand() []string {
	return []string{"/stackable/hadoop/bin/hdfs"}
}

func (c *DataNodeComponent) GetArgs() []string {
	return []string{"datanode"}
}

func (c *DataNodeComponent) GetEnvVars() []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "HADOOP_CONF_DIR",
			Value: "/stackable/config",
		},
		{
			Name:  "HADOOP_LOG_DIR",
			Value: "/stackable/log",
		},
		{
			Name:  "HADOOP_USER_NAME",
			Value: "hdfs",
		},
		{
			Name:  "HDFS_DATANODE_USER",
			Value: "hdfs",
		},
	}
}

func (c *DataNodeComponent) GetVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: "/stackable/config",
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: "/stackable/log",
		},
		{
			Name:      "datanode-data",
			MountPath: "/stackable/data/datanode",
		},
	}
}

func (c *DataNodeComponent) GetPorts() []corev1.ContainerPort {
	return []corev1.ContainerPort{
		{
			Name:          "data",
			ContainerPort: 9866,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          "http",
			ContainerPort: 9864,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          "ipc",
			ContainerPort: 9867,
			Protocol:      corev1.ProtocolTCP,
		},
	}
}

func (c *DataNodeComponent) GetLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/",
				Port: intstr.FromString("http"),
			},
		},
		InitialDelaySeconds: 30,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    3,
	}
}

func (c *DataNodeComponent) GetReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/",
				Port: intstr.FromString("http"),
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       5,
		TimeoutSeconds:      3,
		FailureThreshold:    3,
	}
}

func (c *DataNodeComponent) GetStartupProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/",
				Port: intstr.FromString("http"),
			},
		},
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		FailureThreshold:    30,
	}
}

func (c *DataNodeComponent) GetResources() *corev1.ResourceRequirements {
	return &corev1.ResourceRequirements{}
}

func (c *DataNodeComponent) GetSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsUser:    &[]int64{1000}[0],
		RunAsGroup:   &[]int64{1000}[0],
		RunAsNonRoot: &[]bool{true}[0],
	}
}

// GetJvmOpts returns JVM options for DataNode
func (c *DataNodeComponent) GetJvmOpts() string {
	opts := []string{
		"-Xmx1g",
		"-Xms1g",
		"-XX:+UseG1GC",
		"-XX:MaxGCPauseMillis=20",
		"-XX:InitiatingHeapOccupancyPercent=35",
		"-XX:+ExplicitGCInvokesConcurrent",
		"-Djava.awt.headless=true",
		"-Djava.net.preferIPv4Stack=true",
		"-Dhadoop.log.dir=/stackable/log",
		"-Dhadoop.log.file=hadoop.log",
		"-Dhadoop.home.dir=/stackable/hadoop",
		"-Dhadoop.id.str=hdfs",
		"-Dhadoop.root.logger=INFO,RFA",
		"-Dhadoop.policy.file=hadoop-policy.xml",
	}
	return strings.Join(opts, " ")
}

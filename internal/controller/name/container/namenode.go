package container

import (
	"path"
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	oputil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// NameNodeContainerBuilder builds namenode containers
type NameNodeContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *oputil.Image
}

// NewNameNodeContainerBuilder creates a new namenode container builder
func NewNameNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
) *NameNodeContainerBuilder {
	return &NameNodeContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the namenode container
func (b *NameNodeContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		constant.NameNodeComponent,
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create namenode component and build container
	component := newNameNodeComponent(b.instance.Name, b.instance.Spec.ClusterConfig)

	return builder.BuildWithComponent(component)
}

// nameNodeComponent implements ContainerComponentInterface for NameNode
type nameNodeComponent struct {
	clusterName   string
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

// Ensure nameNodeComponent implements all required interfaces
var _ common.ContainerComponentInterface = &nameNodeComponent{}
var _ common.ContainerPortsProvider = &nameNodeComponent{}
var _ common.ContainerHealthCheckProvider = &nameNodeComponent{}

func newNameNodeComponent(clusterName string, clusterConfig *hdfsv1alpha1.ClusterConfigSpec) *nameNodeComponent {
	return &nameNodeComponent{
		clusterName:   clusterName,
		clusterConfig: clusterConfig,
	}
}

func (c *nameNodeComponent) GetContainerName() string {
	return constant.NameNodeContainer
}

func (c *nameNodeComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *nameNodeComponent) GetArgs() []string {
	args := []string{
		`mkdir -p /kubedoop/config/namenode
cp /kubedoop/mount/config/namenode/*.xml /kubedoop/config/namenode
cp /kubedoop/mount/config/namenode/namenode.log4j.properties /kubedoop/config/namenode/log4j.properties`,
	}

	// Add Kerberos configuration if enabled
	if common.IsKerberosEnabled(c.clusterConfig) {
		args = append(args, `{{ if .kerberosEnabled}}
{{- .kerberosEnv}}
{{- end}}`)
	}

	// Add common bash functions and namenode startup
	args = append(args,
		oputil.CommonBashTrapFunctions,
		oputil.RemoveVectorShutdownFileCommand(),
		oputil.InvokePrepareSignalHandlers,
		oputil.ExportPodAddress(),
		"/kubedoop/hadoop/bin/hdfs namenode &",
		oputil.InvokeWaitForTermination,
		oputil.CreateVectorShutdownFileCommand(),
	)

	// Process template and return
	tmpl := strings.Join(args, "\n")
	krbData := common.CreateExportKrbRealmEnvData(c.clusterConfig)
	return common.ParseTemplate(tmpl, krbData)
}

func (c *nameNodeComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.clusterConfig, constant.NameNodeComponent, ptr.To(constant.NameNode))
}

func (c *nameNodeComponent) GetVolumeMounts() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(c.clusterConfig)
	nameNodeMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: path.Join(constants.KubedoopConfigDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: path.Join(constants.KubedoopLogDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.ListenerVolumeName,
			MountPath: constants.KubedoopListenerDir,
		},
		{
			Name:      hdfsv1alpha1.DataVolumeMountName,
			MountPath: constants.KubedoopDataDir,
		},
	}
	return append(mounts, nameNodeMounts...)
}

// ContainerPortsProvider interface implementation
func (c *nameNodeComponent) GetPorts() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.RpcName,
			ContainerPort: hdfsv1alpha1.NameNodeRpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: hdfsv1alpha1.NameNodeMetricPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	return append(ports, common.HttpPort(c.clusterConfig, hdfsv1alpha1.NameNodeHttpsPort, hdfsv1alpha1.NameNodeHttpPort))
}

// ContainerHealthCheckProvider interface implementation
func (c *nameNodeComponent) GetLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    5,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: common.TlsHttpGetAction(c.clusterConfig, "dfshealth.html"),
		},
	}
}

func (c *nameNodeComponent) GetReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    3,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(hdfsv1alpha1.RpcName)},
		},
	}
}

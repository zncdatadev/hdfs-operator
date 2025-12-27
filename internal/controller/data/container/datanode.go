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

// DataNodeContainerBuilder builds datanode containers
type DataNodeContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *oputil.Image
}

// NewDataNodeContainerBuilder creates a new datanode container builder
func NewDataNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
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
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		constant.DataNodeComponent,
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create datanode component and build container
	component := newDataNodeComponent(b.instance.Name, b.instance.Spec.ClusterConfig)

	return builder.BuildWithComponent(component)
}

// DataNodeComponent implements the component interface for DataNode
type DataNodeComponent struct {
	clusterName   string
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

// Compile-time check to ensure DataNodeComponent implements ContainerComponentInterface
var _ common.ContainerComponentInterface = &DataNodeComponent{}
var _ common.ContainerPortsProvider = &DataNodeComponent{}
var _ common.ContainerHealthCheckProvider = &DataNodeComponent{}

func newDataNodeComponent(clusterName string, clusterConfig *hdfsv1alpha1.ClusterConfigSpec) *DataNodeComponent {
	return &DataNodeComponent{
		clusterName:   clusterName,
		clusterConfig: clusterConfig,
	}
}

func (c *DataNodeComponent) GetContainerName() string {
	return constant.DataNodeContainer
}

func (c *DataNodeComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *DataNodeComponent) GetArgs() []string {
	var args []string
	args = append(args, `mkdir -p /kubedoop/config/datanode
cp /kubedoop/mount/config/datanode/*.xml /kubedoop/config/datanode
cp /kubedoop/mount/config/datanode/datanode.log4j.properties /kubedoop/config/datanode/log4j.properties`)
	if common.IsKerberosEnabled(c.clusterConfig) {
		args = append(args, `{{ if .kerberosEnabled}}
{{- .kerberosEnv}}
{{- end}}`)
	}

	// Add common bash functions and datanode startup
	args = append(args,
		oputil.CommonBashTrapFunctions,
		oputil.RemoveVectorShutdownFileCommand(),
		oputil.InvokePrepareSignalHandlers,
		oputil.ExportPodAddress(),
		"/kubedoop/hadoop/bin/hdfs datanode &",
		oputil.InvokeWaitForTermination,
		oputil.CreateVectorShutdownFileCommand(),
	)

	tmpl := strings.Join(args, "\n")
	krbData := common.CreateExportKrbRealmEnvData(c.clusterConfig)
	return common.ParseTemplate(tmpl, krbData)
}

func (c *DataNodeComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.clusterConfig, constant.DataNodeComponent, ptr.To(constant.DataNode))
}

func (c *DataNodeComponent) GetVolumeMounts() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(c.clusterConfig)
	datanodeMounts := []corev1.VolumeMount{
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
			MountPath: path.Join(hdfsv1alpha1.DataNodeRootDataDirPrefix, hdfsv1alpha1.DataVolumeMountName), // !!! the last "data" is pvc name
		},
	}
	return append(mounts, datanodeMounts...)
}

func (c *DataNodeComponent) GetPorts() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: hdfsv1alpha1.DataNodeMetricPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.DataName,
			ContainerPort: hdfsv1alpha1.DataNodeDataPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.IpcName,
			ContainerPort: hdfsv1alpha1.DataNodeIpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	return append(ports, common.HttpPort(c.clusterConfig, hdfsv1alpha1.DataNodeHttpsPort, hdfsv1alpha1.DataNodeHttpPort))
}

func (c *DataNodeComponent) GetLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    5,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: common.TlsHttpGetAction(c.clusterConfig, "/datanode.html"),
		},
	}
}

func (c *DataNodeComponent) GetReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    3,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(hdfsv1alpha1.IpcName)},
		},
	}
}

func (c *DataNodeComponent) GetStartupProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    30,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		TimeoutSeconds:      5,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: common.TlsHttpGetAction(c.clusterConfig, "/datanode.html"),
		},
	}
}

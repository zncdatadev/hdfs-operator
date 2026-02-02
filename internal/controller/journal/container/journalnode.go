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

// JournalNodeContainerBuilder builds journalnode containers
type JournalNodeContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *oputil.Image
}

// NewJournalNodeContainerBuilder creates a new journalnode container builder
func NewJournalNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
) *JournalNodeContainerBuilder {
	return &JournalNodeContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the journalnode container
func (b *JournalNodeContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		constant.JournalNodeComponent,
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create journalnode component and build container
	component := newJournalNodeComponent(b.instance.Name, b.instance.Spec.ClusterConfig)

	return builder.BuildWithComponent(component)
}

// journalNodeComponent implements ContainerComponentInterface for JournalNode
type journalNodeComponent struct {
	clusterName   string
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

// Ensure journalNodeComponent implements all required interfaces
var _ common.ContainerComponentInterface = &journalNodeComponent{}
var _ common.ContainerPortsProvider = &journalNodeComponent{}
var _ common.ContainerHealthCheckProvider = &journalNodeComponent{}

func newJournalNodeComponent(clusterName string, clusterConfig *hdfsv1alpha1.ClusterConfigSpec) *journalNodeComponent {
	return &journalNodeComponent{
		clusterName:   clusterName,
		clusterConfig: clusterConfig,
	}
}

func (c *journalNodeComponent) GetContainerName() string {
	return constant.JournalNodeContainer
}

func (c *journalNodeComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *journalNodeComponent) GetArgs() []string {
	args := []string{
		`mkdir -p /kubedoop/config/journalnode
cp /kubedoop/mount/config/journalnode/*.xml /kubedoop/config/journalnode
cp /kubedoop/mount/config/journalnode/journalnode.log4j.properties /kubedoop/config/journalnode/log4j.properties`,
	}

	// Add Kerberos configuration if enabled
	if common.IsKerberosEnabled(c.clusterConfig) {
		args = append(args, `{{ if .kerberosEnabled}}
{{- .kerberosEnv}}
{{- end}}`)
	}

	// Add common bash functions and journalnode startup
	args = append(args,
		oputil.CommonBashTrapFunctions,
		oputil.RemoveVectorShutdownFileCommand(),
		oputil.InvokePrepareSignalHandlers,
		oputil.ExportPodAddress(),
		"/kubedoop/hadoop/bin/hdfs journalnode &",
		oputil.InvokeWaitForTermination,
		oputil.CreateVectorShutdownFileCommand(),
	)

	// Process template and return
	tmpl := strings.Join(args, "\n")
	krbData := common.CreateExportKrbRealmEnvData(c.clusterConfig)
	return common.ParseTemplate(tmpl, krbData)
}

// GetEnvVars returns environment variables for journalnode
func (c *journalNodeComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.clusterConfig, constant.JournalNodeComponent, ptr.To(constant.JournalNode))
}

// GetVolumeMounts returns volume mounts for journalnode
func (c *journalNodeComponent) GetVolumeMounts() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(c.clusterConfig)
	journalNodeMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: path.Join(constants.KubedoopConfigDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: path.Join(constants.KubedoopLogDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.DataVolumeMountName,
			MountPath: constants.KubedoopDataDir,
		},
	}
	return append(mounts, journalNodeMounts...)
}

// ContainerPortsProvider interface implementation
func (c *journalNodeComponent) GetPorts() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.RpcName,
			ContainerPort: hdfsv1alpha1.JournalNodeRpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: hdfsv1alpha1.JournalNodeMetricPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	return append(ports, common.HttpPort(c.clusterConfig, hdfsv1alpha1.JournalNodeHttpsPort, hdfsv1alpha1.JournalNodeHttpPort))
}

// ContainerHealthCheckProvider interface implementation
func (c *journalNodeComponent) GetLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    5,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: common.TlsHttpGetAction(c.clusterConfig, "/journalnode.html"),
		},
	}
}

func (c *journalNodeComponent) GetReadinessProbe() *corev1.Probe {
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

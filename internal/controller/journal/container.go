package journal

import (
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type ContainerBuilder struct {
	common.ContainerBuilder
	zookeeperConfigMapName string
	clusterConfig          *hdfsv1alpha1.ClusterConfigSpec
}

func NewJournalNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
) *ContainerBuilder {
	imageSpec := instance.Spec.Image
	image := hdfsv1alpha1.TransformImage(imageSpec)
	clusterConfig := instance.Spec.ClusterConfig
	return &ContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image.String(), image.GetPullPolicy(), resource),
		zookeeperConfigMapName: clusterConfig.ZookeeperConfigMapName,
		clusterConfig:          clusterConfig,
	}
}

func (d *ContainerBuilder) ContainerName() string {
	return string(ContainerJournalNode)
}

func (d *ContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}

func (d *ContainerBuilder) ContainerEnv() []corev1.EnvVar {
	envs := common.GetCommonContainerEnv(d.clusterConfig, ContainerJournalNode)
	return envs
}

func (d *ContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(d.clusterConfig)
	jnMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: constants.KubedoopConfigDirMount + "/" + d.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: constants.KubedoopLogDirMount + "/" + d.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.DataVolumeMountName,
			MountPath: constants.KubedoopDataDir, // note:do not use  hdfsv1alpha1.JournalNodeRootDataDir
		},
	}
	return append(mounts, jnMounts...)
}

func (d *ContainerBuilder) LivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    5,
		InitialDelaySeconds: 10,
		PeriodSeconds:       60,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: common.TlsHttpGetAction(d.clusterConfig, "/journalnode.html"),
		},
	}
}

func (d *ContainerBuilder) ReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    3,
		InitialDelaySeconds: 10,
		PeriodSeconds:       60,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(hdfsv1alpha1.RpcName)},
		},
	}
}

// ContainerPorts  make container ports of data node
func (d *ContainerBuilder) ContainerPorts() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: hdfsv1alpha1.JournalNodeMetricPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.RpcName,
			ContainerPort: hdfsv1alpha1.JournalNodeRpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	return append(ports, common.HttpPort(d.clusterConfig, hdfsv1alpha1.JournalNodeHttpsPort, hdfsv1alpha1.JournalNodeHttpPort))
}

func (d *ContainerBuilder) CommandArgs() []string {
	var args []string
	args = append(args, `mkdir -p /kubedoop/config/journalnode
cp /kubedoop/mount/config/journalnode/*.xml /kubedoop/config/journalnode
cp /kubedoop/mount/config/journalnode/journalnode.log4j.properties /kubedoop/config/journalnode/log4j.properties`)
	if common.IsKerberosEnabled(d.clusterConfig) {
		args = append(args, `{{ if .kerberosEnabled}}
{{- .kerberosEnv}}
{{- end}}`)
	}
	args = append(args, util.CommonBashTrapFunctions)
	args = append(args, util.RemoveVectorShutdownFileCommand())
	args = append(args, util.InvokePrepareSignalHandlers)
	args = append(args, util.ExportPodAddress())
	args = append(args, "/kubedoop/hadoop/bin/hdfs journalnode &")
	args = append(args, util.InvokeWaitForTermination)
	args = append(args, util.CreateVectorShutdownFileCommand())

	tmpl := strings.Join(args, "\n")
	krbData := common.CreateExportKrbRealmEnvData(d.clusterConfig)
	return common.ParseTemplate(tmpl, krbData)
}

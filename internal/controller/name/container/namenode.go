package container

import (
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type NameNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperConfigMapName string
	clusterConfig          *hdfsv1alpha1.ClusterConfigSpec
}

func NewNameNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
	image *util.Image,
) *NameNodeContainerBuilder {
	zookeeperConfigMapName := instance.Spec.ClusterConfig.ZookeeperConfigMapName
	return &NameNodeContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image.String(), image.GetPullPolicy(), resource),
		zookeeperConfigMapName: zookeeperConfigMapName,
		clusterConfig:          instance.Spec.ClusterConfig,
	}
}

func (n *NameNodeContainerBuilder) ContainerName() string {
	return string(NameNode)
}

func (n *NameNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}

func (n *NameNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	envs := common.GetCommonContainerEnv(n.clusterConfig, NameNode)
	return envs
}

func (n *NameNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(n.clusterConfig)
	nnMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: constants.KubedoopConfigDirMount + "/" + n.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: constants.KubedoopLogDirMount + "/" + n.ContainerName(),
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
	return append(mounts, nnMounts...)
}

func (n *NameNodeContainerBuilder) LivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    5,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: common.TlsHttpGetAction(n.clusterConfig, "dfshealth.html"),
		},
	}
}

func (n *NameNodeContainerBuilder) ReadinessProbe() *corev1.Probe {
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

// ContainerPorts  make container ports of name node
func (n *NameNodeContainerBuilder) ContainerPorts() []corev1.ContainerPort {
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
	return append(ports, common.HttpPort(n.clusterConfig, hdfsv1alpha1.NameNodeHttpsPort, hdfsv1alpha1.NameNodeHttpPort))
}

func (n *NameNodeContainerBuilder) CommandArgs() []string {
	var args []string
	args = append(args, `mkdir -p /kubedoop/config/namenode
cp /kubedoop/mount/config/namenode/*.xml /kubedoop/config/namenode
cp /kubedoop/mount/config/namenode/namenode.log4j.properties /kubedoop/config/namenode/log4j.properties`)

	// args = append(args, "while true; do sleep 1; done")

	if common.IsKerberosEnabled(n.clusterConfig) {
		args = append(args, `{{ if .kerberosEnabled}}
{{- .kerberosEnv}}
{{- end}}`)
	}
	args = append(args, util.CommonBashTrapFunctions)
	args = append(args, util.RemoveVectorShutdownFileCommand())
	args = append(args, util.InvokePrepareSignalHandlers)
	args = append(args, util.ExportPodAddress())
	args = append(args, "/kubedoop/hadoop/bin/hdfs namenode &")
	args = append(args, util.InvokeWaitForTermination)
	args = append(args, util.CreateVectorShutdownFileCommand())

	tmpl := strings.Join(args, "\n")
	krbData := common.CreateExportKrbRealmEnvData(n.clusterConfig)
	return common.ParseTemplate(tmpl, krbData)
}

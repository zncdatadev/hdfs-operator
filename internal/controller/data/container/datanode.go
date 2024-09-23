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

type DataNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperConfigMapName string
	clusterConfig          *hdfsv1alpha1.ClusterConfigSpec
}

func NewDataNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
) *DataNodeContainerBuilder {
	image := hdfsv1alpha1.TransformImage(instance.Spec.Image)
	imagePullPolicy := image.GetPullPolicy()
	clusterConfig := instance.Spec.ClusterConfigSpec
	return &DataNodeContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image.String(), imagePullPolicy, resource),
		zookeeperConfigMapName: clusterConfig.ZookeeperConfigMapName,
		clusterConfig:          clusterConfig,
	}
}

func (d *DataNodeContainerBuilder) ContainerName() string {
	return string(DataNode)
}

func (d *DataNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}

func (d *DataNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	envs := common.GetCommonContainerEnv(d.clusterConfig, DataNode)
	return envs
}

func (d *DataNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(d.clusterConfig)
	datanodeMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: constants.KubedoopConfigDirMount + "/" + d.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: constants.KubedoopLogDirMount + "/" + d.ContainerName(),
		},
		{
			Name:      hdfsv1alpha1.ListenerVolumeName,
			MountPath: constants.KubedoopListenerDir,
		},
		{
			Name:      hdfsv1alpha1.DataVolumeMountName,
			MountPath: hdfsv1alpha1.DataNodeRootDataDirPrefix + hdfsv1alpha1.DataVolumeMountName, // !!! the last "data" is pvc name
		},
	}
	return append(mounts, datanodeMounts...)
}

func (d *DataNodeContainerBuilder) LivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		FailureThreshold:    5,
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      1,
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: common.TlsHttpGetAction(d.clusterConfig, "/datanode.html"),
		},
	}
}

func (d *DataNodeContainerBuilder) ReadinessProbe() *corev1.Probe {
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

// ContainerPorts  make container ports of data node
func (d *DataNodeContainerBuilder) ContainerPorts() []corev1.ContainerPort {
	ports := []corev1.ContainerPort{
		{
			Name:          hdfsv1alpha1.MetricName,
			ContainerPort: hdfsv1alpha1.DataNodeMetricPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name: hdfsv1alpha1.DataName,
			// 20000
			ContainerPort: hdfsv1alpha1.DataNodeDataPort,
			Protocol:      corev1.ProtocolTCP,
		},
		{
			Name:          hdfsv1alpha1.IpcName,
			ContainerPort: hdfsv1alpha1.DataNodeIpcPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
	return append(ports, common.HttpPort(d.clusterConfig, hdfsv1alpha1.DataNodeHttpsPort, hdfsv1alpha1.DataNodeHttpPort))
}
func (d *DataNodeContainerBuilder) CommandArgs() []string {
	var args []string
	args = append(args, `mkdir -p /kubedoop/config/datanode
cp /kubedoop/mount/config/datanode/*.xml /kubedoop/config/datanode
cp /kubedoop/mount/config/datanode/datanode.log4j.properties /kubedoop/config/datanode/log4j.properties`)
	if common.IsKerberosEnabled(d.clusterConfig) {
		args = append(args, `{{ if .kerberosEnabled}}
{{- .kerberosEnv}}
{{- end}}`)
	}
	args = append(args, util.CommonBashTrapFunctions)
	args = append(args, util.RemoveVectorShutdownFileCommand())
	args = append(args, util.InvokePrepareSignalHandlers)
	args = append(args, util.ExportPodAddress())
	args = append(args, "/kubedoop/hadoop/bin/hdfs datanode &")
	args = append(args, util.InvokeWaitForTermination)
	args = append(args, util.CreateVectorShutdownFileCommand())

	tmpl := strings.Join(args, "\n")
	krbData := common.CreateExportKrbRealmEnvData(d.clusterConfig)
	return common.ParseTemplate(tmpl, krbData)
}

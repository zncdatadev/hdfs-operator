package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type DataNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
	clusterConfig           *hdfsv1alpha1.ClusterConfigSpec
}

func NewDataNodeContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	resource corev1.ResourceRequirements,
) *DataNodeContainerBuilder {
	imageSpec := instance.Spec.Image
	image := util.ImageRepository(imageSpec.Repository, imageSpec.Tag)
	imagePullPolicy := imageSpec.PullPolicy
	clusterConfig := instance.Spec.ClusterConfigSpec
	return &DataNodeContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: clusterConfig.ZookeeperDiscoveryZNode,
		clusterConfig:           clusterConfig,
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
			Name:      DataNodeConfVolumeName(),
			MountPath: "/stackable/mount/config/datanode",
		},
		{
			Name:      DataNodeLogVolumeName(),
			MountPath: "/stackable/mount/log/datanode",
		},
		{
			Name:      ListenerVolumeName(),
			MountPath: "/stackable/listener",
		},
		{
			Name:      DataVolumeName(),
			MountPath: "/stackable/data/data", // !!! the last "data" is pvc name
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
			HTTPGet: &corev1.HTTPGetAction{
				Path:   "/datanode.html",
				Port:   intstr.FromString(hdfsv1alpha1.HttpName),
				Scheme: common.WebUiPortProbe(d.clusterConfig),
			},
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

	tmpl := `mkdir -p /stackable/config/datanode
cp /stackable/mount/config/datanode/*.xml /stackable/config/datanode
cp /stackable/mount/config/datanode/datanode.log4j.properties /stackable/config/datanode/log4j.properties
\

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}
{{- end }}

prepare_signal_handlers()
{
    unset term_child_pid
    unset term_kill_needed
    trap 'handle_term_signal' TERM
}

handle_term_signal()
{
    if [ "${term_child_pid}" ]; then
        kill -TERM "${term_child_pid}" 2>/dev/null
    else
        term_kill_needed="yes"
    fi
}

wait_for_termination()
{
    set +e
    term_child_pid=$1
    if [[ -v term_kill_needed ]]; then
        kill -TERM "${term_child_pid}" 2>/dev/null
    fi
    wait ${term_child_pid} 2>/dev/null
    trap - TERM
    wait ${term_child_pid} 2>/dev/null
    set -e
}

rm -f /stackable/log/_vector/shutdown
prepare_signal_handlers
if [[ -d /stackable/listener ]]; then
  export POD_ADDRESS=$(cat /stackable/listener/default-address/address)
  for i in /stackable/listener/default-address/ports/*; do
      export $(basename $i | tr a-z A-Z)_PORT="$(cat $i)"
  done
fi
/stackable/hadoop/bin/hdfs datanode &
wait_for_termination $!
mkdir -p /stackable/log/_vector && touch /stackable/log/_vector/shutdown
`
	return common.ParseKerberosScript(tmpl, common.CreateExportKrbRealmEnvData(d.clusterConfig))
}

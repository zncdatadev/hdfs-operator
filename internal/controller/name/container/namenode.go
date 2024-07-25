package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/util"
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
	resource corev1.ResourceRequirements) *NameNodeContainerBuilder {
	imageSpec := instance.Spec.Image
	image := util.ImageRepository(imageSpec.Repository, imageSpec.Tag)
	imagePullPolicy := imageSpec.PullPolicy
	zookeeperConfigMapName := instance.Spec.ClusterConfigSpec.ZookeeperConfigMapName
	return &NameNodeContainerBuilder{
		ContainerBuilder:       *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperConfigMapName: zookeeperConfigMapName,
		clusterConfig:          instance.Spec.ClusterConfigSpec,
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
			Name:      NameNodeConfVolumeName(),
			MountPath: "/stackable/mount/config/namenode",
		},
		{
			Name:      NameNodeLogVolumeName(),
			MountPath: "/stackable/mount/log/namenode",
		},
		{
			Name:      ListenerVolumeName(),
			MountPath: "/stackable/listener",
		},
		{
			Name:      DataVolumeName(),
			MountPath: "/stackable/data",
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
	return common.ParseKerberosScript(`mkdir -p /stackable/config/namenode
cp /stackable/mount/config/namenode/*.xml /stackable/config/namenode
cp /stackable/mount/config/namenode/namenode.log4j.properties /stackable/config/namenode/log4j.properties
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
/stackable/hadoop/bin/hdfs namenode &
wait_for_termination $!
mkdir -p /stackable/log/_vector && touch /stackable/log/_vector/shutdown
`, common.CreateExportKrbRealmEnvData(n.clusterConfig))
}

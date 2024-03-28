package container

import (
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type DataNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
}

func NewDataNodeContainerBuilder(
	image string,
	imagePullPolicy corev1.PullPolicy,
	resource corev1.ResourceRequirements,
	zookeeperDiscoveryZNode string,
) *DataNodeContainerBuilder {
	return &DataNodeContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
	}
}

func (d *DataNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}

func (d *DataNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	envs := common.GetCommonContainerEnv(d.zookeeperDiscoveryZNode, DataNode)
	envs = append(envs, corev1.EnvVar{
		Name:  "HDFS_DATANODE_OPTS",
		Value: "-Djava.security.properties=/znclabs/config/datanode/security.properties -Xmx419430k",
	})
	return envs
}

func (d *DataNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      LogVolumeName(),
			MountPath: "/znclabs/log",
		},
		{
			Name:      DataNodeConfVolumeName(),
			MountPath: "/znclabs/mount/config/datanode",
		},
		{
			Name:      DataNodeLogVolumeName(),
			MountPath: "/znclabs/mount/log/datanode",
		},
		{
			Name:      ListenerVolumeName(),
			MountPath: "/znclabs/listener",
		},
		{
			Name:      DataVolumeName(),
			MountPath: "/znclabs/data/data",
		},
	}
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
				Scheme: corev1.URISchemeHTTP,
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
	return []corev1.ContainerPort{
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
		{
			Name:          hdfsv1alpha1.HttpName,
			ContainerPort: hdfsv1alpha1.DataNodeHttpPort,
			Protocol:      corev1.ProtocolTCP,
		},
	}
}

func (d *DataNodeContainerBuilder) CommandArgs() []string {
	return []string{
		`mkdir -p /znclabs/config/datanode
cp /znclabs/mount/config/datanode/*.xml /znclabs/config/datanode
cp /znclabs/mount/config/datanode/datanode.log4j.properties /znclabs/config/datanode/log4j.properties
\

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

rm -f /znclabs/log/_vector/shutdown
prepare_signal_handlers
if [[ -d /znclabs/listener ]]; then
  export POD_ADDRESS=$(cat /znclabs/listener/default-address/address)
  for i in /znclabs/listener/default-address/ports/*; do
	  export $(basename $i | tr a-z A-Z)_PORT="$(cat $i)"
  done
fi
/stackable/hadoop/bin/hdfs datanode &
wait_for_termination $!
mkdir -p /znclabs/log/_vector && touch /znclabs/log/_vector/shutdown
`,
	}
}

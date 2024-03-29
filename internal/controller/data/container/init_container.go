package container

import (
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
)

type WaitNameNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
}

func NewWaitNameNodeContainerBuilder(
	image string,
	imagePullPolicy corev1.PullPolicy,
	resource corev1.ResourceRequirements,
	zookeeperDiscoveryZNode string,
) *WaitNameNodeContainerBuilder {
	return &WaitNameNodeContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
	}
}

func (z *WaitNameNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(z.zookeeperDiscoveryZNode, WaitNameNode)
}

func (z *WaitNameNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      LogVolumeName(),
			MountPath: "/znclabs/log",
		},
		{
			Name:      WaitNameNodeConfigVolumeName(),
			MountPath: "/znclabs/mount/config/wait-for-namenodes",
		},
		{
			Name:      WaitNameNodeLogVolumeName(),
			MountPath: "/znclabs/mount/log/wait-for-namenodes",
		},
	}
}

func (z *WaitNameNodeContainerBuilder) ContainerName() string {
	return string(WaitNameNode)
}

func (z *WaitNameNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (z *WaitNameNodeContainerBuilder) CommandArgs() []string {
	return []string{`mkdir -p /stackable/config/wait-for-namenodes
cp /stackable/mount/config/wait-for-namenodes/*.xml /stackable/config/wait-for-namenodes
cp /stackable/mount/config/wait-for-namenodes/wait-for-namenodes.log4j.properties /stackable/config/wait-for-namenodes/log4j.properties
echo "Waiting for namenodes to get ready:"
n=0
while [ ${n} -lt 12 ]; do
	ALL_NODES_READY=true
	for namenode_id in simple-hdfs-namenode-default-0 simple-hdfs-namenode-default-1 simple-hdfs-namenode-default-2; do
		echo -n "Checking pod $namenode_id... "
		SERVICE_STATE=$(/stackable/hadoop/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)
		if [ "$SERVICE_STATE" = "active" ] || [ "$SERVICE_STATE" = "standby" ]; then
			echo "$SERVICE_STATE"
		else
			echo "not ready"
			ALL_NODES_READY=false
		fi
	done
	if [ "$ALL_NODES_READY" == "true" ]; then
		echo "All namenodes ready!"
		break
	fi
	echo ""
	n=$((n + 1))
	sleep 5
donee
`}
}

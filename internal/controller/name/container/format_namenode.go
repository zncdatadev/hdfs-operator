package container

import (
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
)

// FormatNameNodeContainerBuilder container builder
type FormatNameNodeContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
}

func NewFormatNameNodeContainerBuilder(
	image string,
	imagePullPolicy corev1.PullPolicy,
	resource corev1.ResourceRequirements,
	zookeeperDiscoveryZNode string,
) *FormatNameNodeContainerBuilder {
	return &FormatNameNodeContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
	}
}

func (f *FormatNameNodeContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(f.zookeeperDiscoveryZNode, FormatNameNode)
}

func (f *FormatNameNodeContainerBuilder) VolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      LogVolumeName(),
			MountPath: "/znclabs/log",
		},
		{
			Name:      FormatNameNodeVolumeName(),
			MountPath: "/znclabs/mount/config/format-namenodes",
		},
		{
			Name:      FormatNameNodeLogVolumeName(),
			MountPath: "/znclabs/mount/log/format-namenodes",
		},
		{
			Name:      DataVolumeName(),
			MountPath: "/znclabs/data",
		},
	}
}

func (f *FormatNameNodeContainerBuilder) ContainerName() string {
	return string(FormatNameNode)
}

func (f *FormatNameNodeContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (f *FormatNameNodeContainerBuilder) CommandArgs() []string {
	return []string{`mkdir -p /znclabs/config/format-namenodes
cp /znclabs/mount/config/format-namenodes/*.xml /znclabs/config/format-namenodes
cp /znclabs/mount/config/format-namenodes/format-namenodes.log4j.properties /znclabs/config/format-namenodes/log4j.properties
echo "Start formatting namenode $POD_NAME. Checking for active namenodes:"
for namenode_id in simple-hdfs-namenode-default-0 simple-hdfs-namenode-default-1 simple-hdfs-namenode-default-2
do
	echo -n "Checking pod $namenode_id... "
	SERVICE_STATE=$(/stackable/hadoop/bin/hdfs haadmin -getServiceState $namenode_id | tail -n1 || true)
	if [ "$SERVICE_STATE" == "active" ]
	then
		ACTIVE_NAMENODE=$namenode_id
	  	echo "active"
	  	break
	fi
	echo ""
done

if [ ! -f "/znclabs/data/namenode/current/VERSION" ]
then
	if [ -z ${ACTIVE_NAMENODE+x} ]
	then
	  echo "Create pod $POD_NAME as active namenode."
	  /stackable/hadoop/bin/hdfs namenode -format -noninteractive
	else
	  echo "Create pod $POD_NAME as standby namenode."
	  /stackable/hadoop/bin/hdfs namenode -bootstrapStandby -nonInteractive
	fi
else
	cat "/znclabs/data/namenode/current/VERSION"
	echo "Pod $POD_NAME already formatted. Skipping..."
  fi
`}
}

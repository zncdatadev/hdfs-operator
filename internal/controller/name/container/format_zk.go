package container

import (
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
)

// FormatZookeeperContainerBuilder container builder
type FormatZookeeperContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
}

func NewFormatZookeeperContainerBuilder(
	image string,
	imagePullPolicy corev1.PullPolicy,
	resource corev1.ResourceRequirements,
	zookeeperDiscoveryZNode string,
) *FormatZookeeperContainerBuilder {
	return &FormatZookeeperContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
	}
}

func (z *FormatZookeeperContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(z.zookeeperDiscoveryZNode, FormatZookeeper)
}

func (z *FormatZookeeperContainerBuilder) VolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      LogVolumeName(),
			MountPath: "/znclabs/log",
		},
		{
			Name:      FormatZookeeperVolumeName(),
			MountPath: "/znclabs/mount/config/format-zookeeper",
		},
		{
			Name:      FormatZookeeperLogVolumeName(),
			MountPath: "/znclabs/mount/log/format-zookeeper",
		},
	}
}

func (z *FormatZookeeperContainerBuilder) ContainerName() string {
	return string(FormatZookeeper)
}

func (z *FormatZookeeperContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}
func (z *FormatZookeeperContainerBuilder) CommandArgs() []string {
	return []string{`mkdir -p /znclabs/config/format-zookeeper
cp /znclabs/mount/config/format-zookeeper/*.xml /znclabs/config/format-zookeeper
cp /znclabs/mount/config/format-zookeeper/format-zookeeper.log4j.properties /znclabs/config/format-zookeeper/log4j.properties
echo "Attempt to format ZooKeeper..."
if [[ "0" -eq "$(echo $POD_NAME | sed -e 's/.*-//')" ]] ; then
	set +e
	/stackable/hadoop/bin/hdfs zkfc -formatZK -nonInteractive
	EXITCODE=$?
	set -e
	if [[ $EXITCODE -eq 0 ]]; then
		echo "Successfully formatted"
	elif [[ $EXITCODE -eq 2 ]]; then
		echo "ZNode already existed, did nothing"
	else
		echo "Zookeeper format failed with exit code $EXITCODE"
		exit $EXITCODE
	fi

else
	echo "ZooKeeper already formatted!"
fi
`}
}

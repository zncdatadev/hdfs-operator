package container

import (
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
)

// zkfc container builder
type ZkfcContainerBuilder struct {
	common.ContainerBuilder
	zookeeperDiscoveryZNode string
}

func NewZkfcContainerBuilder(
	image string,
	imagePullPolicy corev1.PullPolicy,
	resource corev1.ResourceRequirements,
	zookeeperDiscoveryZNode string,
) *ZkfcContainerBuilder {
	return &ZkfcContainerBuilder{
		ContainerBuilder:        *common.NewContainerBuilder(image, imagePullPolicy, resource),
		zookeeperDiscoveryZNode: zookeeperDiscoveryZNode,
	}
}

func (z *ZkfcContainerBuilder) ContainerName() string {
	return string(Zkfc)
}

// CommandArgs zookeeper fail-over controller command args
func (z *ZkfcContainerBuilder) CommandArgs() []string {
	return []string{`mkdir -p /znclabs/config/zkfc
cp /znclabs/mount/config/zkfc/*.xml /znclabs/config/zkfc
cp /znclabs/mount/config/zkfc/zkfc.log4j.properties /znclabs/config/zkfc/log4j.properties
/stackable/hadoop/bin/hdfs zkfc
`,
	}
}

func (z *ZkfcContainerBuilder) ContainerEnv() []corev1.EnvVar {
	return common.GetCommonContainerEnv(z.zookeeperDiscoveryZNode, Zkfc)
}

func (z *ZkfcContainerBuilder) VolumeMount() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      LogVolumeName(),
			MountPath: "/znclabs/log",
		},
		{
			Name:      ZkfcVolumeName(),
			MountPath: "/znclabs/mount/config/zkfc",
		},
		{
			Name:      ZkfcLogVolumeName(),
			MountPath: "/znclabs/mount/log/zkfc",
		},
	}
}

func (z *ZkfcContainerBuilder) Command() []string {
	return common.GetCommonCommand()
}

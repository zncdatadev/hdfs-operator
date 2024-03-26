package container

import (
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	corev1 "k8s.io/api/core/v1"
)

// Component ContainerComponent name node container component
// contains: zkfc, namenode, format-namenode, format-zookeeper
type Component string

const (
	Zkfc            Component = "zkfc"
	NameNode        Component = "namenode"
	FormatNameNode  Component = "format-namenodes"
	FormatZookeeper Component = "format-zookeeper"
)

func LogVolumeName() string {
	return "log"
}

func NameNodeVolumeName() string {
	return "name-node"
}

func NameNodeLogVolumeName() string {
	return "name-node-log"
}

func ZkfcVolumeName() string {
	return "zkfc"
}

func ZkfcLogVolumeName() string {
	return "zkfc-log"
}

func FormatNameNodeVolumeName() string {
	return "format-name-node"
}

func FormatNameNodeLogVolumeName() string {
	return "format-name-node-log"
}

func FormatZookeeperVolumeName() string {
	return "format-zookeeper"
}

func FormatZookeeperLogVolumeName() string {
	return "format-zookeeper-log"
}

func DataVolumeName() string {
	return "data"
}

func ListenerVolumeName() string {
	return "listener"
}

func commonContainerEnv(zkDiscoveryZNode string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  "HADOOP_CONF_DIR",
			Value: "/znclabs/config/namenode",
		},
		{
			Name:  "HADOOP_HOME",
			Value: "/stackable/hadoop", // todo rename
		},
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "ZOOKEEPER",
			ValueFrom: &corev1.EnvVarSource{
				ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: zkDiscoveryZNode,
					},
					Key: common.ZookeeperHdfsDiscoveryKey,
				},
			},
		},
	}
}

func commonCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

package container

import (
	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	oputil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

// WaitForZookeeperContainerBuilder builds wait-for-zookeeper init containers for journalnode
type WaitForZookeeperContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *oputil.Image
}

// NewWaitForZookeeperContainerBuilder creates a new wait-for-zookeeper container builder
func NewWaitForZookeeperContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
) *WaitForZookeeperContainerBuilder {
	return &WaitForZookeeperContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the wait-for-zookeeper container
func (b *WaitForZookeeperContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		"wait-for-zookeeper",
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create wait-for-zookeeper component and build container
	component := &waitForZookeeperComponent{
		clusterConfig: b.instance.Spec.ClusterConfig,
	}

	return builder.BuildWithComponent(component)
}

// waitForZookeeperComponent implements ContainerComponentInterface for wait-for-zookeeper
type waitForZookeeperComponent struct {
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

func (c *waitForZookeeperComponent) GetContainerName() string {
	return "wait-for-zookeeper"
}

func (c *waitForZookeeperComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *waitForZookeeperComponent) GetArgs() []string {
	args := `echo "Waiting for Zookeeper to be ready..."

# Extract Zookeeper connection string from cluster config
# This is a simplified check - in production you might want more robust health checks
for i in {1..60}; do
  echo "Attempt $i: Checking Zookeeper availability..."

  # Try to connect to Zookeeper and list znodes
  if echo "ls /" | zkCli.sh 2>/dev/null | grep -q "WatchedEvent"; then
    echo "Zookeeper is ready!"
    exit 0
  fi

  echo "Zookeeper not ready yet, waiting 5 seconds..."
  sleep 5
done

echo "Timeout waiting for Zookeeper to be ready"
exit 1`

	return []string{args}
}

// GetEnvVars returns environment variables for wait-for-zookeeper
func (c *waitForZookeeperComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.clusterConfig, constant.JournalNodeComponent)
}

// GetVolumeMounts returns volume mounts for wait-for-zookeeper
func (c *waitForZookeeperComponent) GetVolumeMounts() []corev1.VolumeMount {
	return []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: constants.KubedoopConfigDirMount + "/wait-for-zookeeper",
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: constants.KubedoopLogDirMount + "/wait-for-zookeeper",
		},
	}
}

package container

import (
	"path"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	oputil "github.com/zncdatadev/operator-go/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

// FormatZookeeperContainerBuilder builds format zookeeper containers
type FormatZookeeperContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *oputil.Image
}

// NewFormatZookeeperContainerBuilder creates a new format zookeeper container builder
func NewFormatZookeeperContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
) *FormatZookeeperContainerBuilder {
	return &FormatZookeeperContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the format zookeeper container
func (b *FormatZookeeperContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		constant.FormatZookeeperComponent,
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create format zookeeper component and build container
	component := &formatZookeeperComponent{
		clusterConfig: b.instance.Spec.ClusterConfig,
	}

	return builder.BuildWithComponent(component)
}

// formatZookeeperComponent implements ContainerComponentInterface for FormatZookeeper
type formatZookeeperComponent struct {
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

// Ensure formatZookeeperComponent implements the required interfaces
var _ common.ContainerComponentInterface = &formatZookeeperComponent{}

func (c *formatZookeeperComponent) GetContainerName() string {
	return string(constant.FormatZookeeperComponent)
}

func (c *formatZookeeperComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *formatZookeeperComponent) GetArgs() []string {
	tmpl := `mkdir -p /kubedoop/config/format-zookeeper
cp /kubedoop/mount/config/format-zookeeper/*.xml /kubedoop/config/format-zookeeper
cp /kubedoop/mount/config/format-zookeeper/format-zookeeper.log4j.properties /kubedoop/config/format-zookeeper/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}
{{- end }}

echo "Attempt to format ZooKeeper..."
if [[ "0" -eq "$(echo $POD_NAME | sed -e 's/.*-//')" ]] ; then
    set +e
    /kubedoop/hadoop/bin/hdfs zkfc -formatZK -nonInteractive
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
`
	return common.ParseTemplate(tmpl, common.CreateExportKrbRealmEnvData(c.clusterConfig))
}

func (c *formatZookeeperComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.clusterConfig, constant.FormatZookeeperComponent, nil)
}

func (c *formatZookeeperComponent) GetVolumeMounts() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(c.clusterConfig)
	formatZookeeperMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.FormatZookeeperConfigVolumeMountName,
			MountPath: path.Join(constants.KubedoopConfigDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.FormatZookeeperLogVolumeMountName,
			MountPath: path.Join(constants.KubedoopLogDirMount, c.GetContainerName()),
		},
	}
	return append(mounts, formatZookeeperMounts...)
}

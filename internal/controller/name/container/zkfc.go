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

// ZkfcContainerBuilder builds zkfc containers
type ZkfcContainerBuilder struct {
	instance        *hdfsv1alpha1.HdfsCluster
	roleGroupInfo   *reconciler.RoleGroupInfo
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec
	image           *oputil.Image
}

// NewZkfcContainerBuilder creates a new zkfc container builder
func NewZkfcContainerBuilder(
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	roleGroupConfig *commonsv1alpha1.RoleGroupConfigSpec,
	image *oputil.Image,
) *ZkfcContainerBuilder {
	return &ZkfcContainerBuilder{
		instance:        instance,
		roleGroupInfo:   roleGroupInfo,
		roleGroupConfig: roleGroupConfig,
		image:           image,
	}
}

// Build builds the zkfc container
func (b *ZkfcContainerBuilder) Build() *corev1.Container {
	// Create the common container builder
	builder := common.NewHdfsContainerBuilder(
		constant.ZkfcComponent,
		b.image,
		b.instance.Spec.ClusterConfig.ZookeeperConfigMapName,
		b.roleGroupInfo,
		b.roleGroupConfig,
	)

	// Create zkfc component and build container
	component := &zkfcComponent{
		clusterConfig: b.instance.Spec.ClusterConfig,
	}

	return builder.BuildWithComponent(component)
}

// zkfcComponent implements ContainerComponentInterface for Zkfc
type zkfcComponent struct {
	clusterConfig *hdfsv1alpha1.ClusterConfigSpec
}

var _ common.ContainerComponentInterface = &zkfcComponent{}

func (c *zkfcComponent) GetContainerName() string {
	return string(constant.ZkfcComponent)
}

func (c *zkfcComponent) GetCommand() []string {
	return []string{"/bin/bash", "-x", "-euo", "pipefail", "-c"}
}

func (c *zkfcComponent) GetArgs() []string {
	tmpl := `mkdir -p /kubedoop/config/zkfc
cp /kubedoop/mount/config/zkfc/*.xml /kubedoop/config/zkfc
cp /kubedoop/mount/config/zkfc/zkfc.log4j.properties /kubedoop/config/zkfc/log4j.properties

{{ if .kerberosEnabled }}
{{- .kerberosEnv }}
{{- end }}

/kubedoop/hadoop/bin/hdfs zkfc
`
	return common.ParseTemplate(tmpl, common.CreateExportKrbRealmEnvData(c.clusterConfig))
}

func (c *zkfcComponent) GetEnvVars() []corev1.EnvVar {
	return common.GetCommonContainerEnv(c.clusterConfig, constant.ZkfcComponent)
}

func (c *zkfcComponent) GetVolumeMounts() []corev1.VolumeMount {
	mounts := common.GetCommonVolumeMounts(c.clusterConfig)
	zkfcMounts := []corev1.VolumeMount{
		{
			Name:      hdfsv1alpha1.HdfsConfigVolumeMountName,
			MountPath: path.Join(constants.KubedoopConfigDirMount, c.GetContainerName()),
		},
		{
			Name:      hdfsv1alpha1.HdfsLogVolumeMountName,
			MountPath: path.Join(constants.KubedoopLogDirMount, c.GetContainerName()),
		},
	}
	return append(mounts, zkfcMounts...)
}

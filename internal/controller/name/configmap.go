package name

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Compile-time check to ensure NamenodeConfigMapBuilder implements ConfigMapComponentBuilder
var _ common.ConfigMapComponentBuilder = &NamenodeConfigMapBuilder{}

// NamenodeConfigMapBuilder implements namenode-specific ConfigMap logic
type NamenodeConfigMapBuilder struct {
	*common.ConfigMapBuilder
	instance             *hdfsv1alpha1.HdfsCluster
	groupName            string
	replicas             *int32
	configSpec           hdfsv1alpha1.ConfigSpec
	overrides            *commonsv1alpha1.OverridesSpec
	clusterComponentInfo *common.ClusterComponentsInfo
}

// NewNamenodeConfigMapBuilder creates a new NamenodeConfigMapBuilder
func NewNamenodeConfigMapBuilder(
	ctx context.Context,
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	replicas *int32,
	overrides *commonsv1alpha1.OverridesSpec,
	configSpec *hdfsv1alpha1.ConfigSpec,
	instance *hdfsv1alpha1.HdfsCluster,
	clusterComponentInfo *common.ClusterComponentsInfo,
) builder.ConfigBuilder {
	configMapBuilder := &NamenodeConfigMapBuilder{
		instance:             instance,
		groupName:            roleGroupInfo.GetGroupName(),
		replicas:             replicas,
		configSpec:           *configSpec,
		overrides:            overrides,
		clusterComponentInfo: clusterComponentInfo,
	}

	nnbuilder := common.NewConfigMapBuilder(
		ctx,
		client,
		constant.NameNode,
		roleGroupInfo,
		overrides,
		configSpec,
		instance,
		configMapBuilder, // self as component
	)

	return nnbuilder
}

// Build constructs the ConfigMap using the inherited common builder
func (b *NamenodeConfigMapBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	return b.ConfigMapBuilder.Build(ctx)
}

// ConfigMapComponentBuilder interface implementation

// BuildConfig returns namenode-specific configuration content
func (b *NamenodeConfigMapBuilder) BuildConfig() (map[string]string, error) {
	data := map[string]string{
		hdfsv1alpha1.CoreSiteFileName:     b.makeCoreSiteData(),
		hdfsv1alpha1.HdfsSiteFileName:     b.makeHdfsSiteData(),
		hdfsv1alpha1.HadoopPolicyFileName: common.MakeHadoopPolicyData(),
		hdfsv1alpha1.SecurityFileName:     common.MakeSecurityPropertiesData(),
		hdfsv1alpha1.SslClientFileName:    common.MakeSslClientData(b.instance.Spec.ClusterConfig),
		hdfsv1alpha1.SslServerFileName:    common.MakeSslServerData(b.instance.Spec.ClusterConfig),
		// log4j
		common.CreateComponentLog4jPropertiesName(constant.NameNodeComponent):        common.MakeLog4jPropertiesData(constant.NameNodeComponent),
		common.CreateComponentLog4jPropertiesName(constant.ZkfcComponent):            common.MakeLog4jPropertiesData(constant.ZkfcComponent),
		common.CreateComponentLog4jPropertiesName(constant.FormatNameNodeComponent):  common.MakeLog4jPropertiesData(constant.FormatNameNodeComponent),
		common.CreateComponentLog4jPropertiesName(constant.FormatZookeeperComponent): common.MakeLog4jPropertiesData(constant.FormatZookeeperComponent),
	}

	return data, nil
}

// GetConfigOverrides returns namenode-specific configuration overrides
func (b *NamenodeConfigMapBuilder) GetConfigOverrides() map[string]map[string]string {
	if b.overrides != nil {
		return b.overrides.ConfigOverrides
	}
	return nil
}

// Helper methods for configuration generation

// make core-site.xml data
func (b *NamenodeConfigMapBuilder) makeCoreSiteData() string {
	generator := &common.CoreSiteXmlGenerator{InstanceName: b.instance.GetName()}
	return generator.EnableKerberos(b.instance.Spec.ClusterConfig, b.instance.Namespace).HaZookeeperQuorum().Generate()
}

// make hdfs-site.xml data
func (b *NamenodeConfigMapBuilder) makeHdfsSiteData() string {
	clusterSpec := b.instance.Spec.ClusterConfig
	// Create ClusterComponentsInfo for the updated generator
	generator := common.NewNameNodeHdfsSiteXmlGenerator(b.instance.GetName(), b.groupName,
		*b.replicas, b.instance.Namespace, b.instance.Spec.ClusterConfig, clusterSpec.ClusterDomain,
		clusterSpec.DfsReplication, b.clusterComponentInfo)
	return generator.EnablerKerberos(clusterSpec).EnableHttps().Generate()
}

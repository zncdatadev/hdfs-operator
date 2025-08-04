package journal

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

// =================================================================
// NEW REFACTORED CODE - Following namenode pattern
// =================================================================

// Compile-time check to ensure JournalnodeConfigMapBuilder implements ConfigMapComponentBuilder
var _ common.ConfigMapComponentBuilder = (*JournalnodeConfigMapBuilder)(nil)

// JournalnodeConfigMapBuilder implements journalnode-specific ConfigMap logic
type JournalnodeConfigMapBuilder struct {
	*common.ConfigMapBuilder
	instance             *hdfsv1alpha1.HdfsCluster
	groupName            string
	mergedCfg            *hdfsv1alpha1.ConfigSpec
	override             *commonsv1alpha1.OverridesSpec
	clusterComponentInfo *common.ClusterComponentsInfo
}

// NewJournalnodeConfigMapBuilder creates a new JournalnodeConfigMapBuilder
func NewJournalnodeConfigMapBuilder(
	ctx context.Context,
	client *client.Client,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	roleConfig *hdfsv1alpha1.ConfigSpec,
	instance *hdfsv1alpha1.HdfsCluster,
	clusterComponentInfo *common.ClusterComponentsInfo,
) builder.ConfigBuilder {
	configMapBuilder := &JournalnodeConfigMapBuilder{
		instance:             instance,
		groupName:            roleGroupInfo.GetGroupName(),
		override:             overrides,
		mergedCfg:            roleConfig,
		clusterComponentInfo: clusterComponentInfo,
	}

	jnbuilder := common.NewConfigMapBuilder(
		ctx,
		client,
		constant.JournalNode,
		roleGroupInfo,
		overrides,
		roleConfig,
		instance,
		configMapBuilder, // self as component
	)

	return jnbuilder
}

// Build builds the ConfigMap
func (b *JournalnodeConfigMapBuilder) Build(ctx context.Context) (ctrlclient.Object, error) {
	return b.ConfigMapBuilder.Build(ctx)
}

// BuildConfig builds the configuration data for the journalnode ConfigMap
// This implements the ConfigMapComponentBuilder interface
func (b *JournalnodeConfigMapBuilder) BuildConfig() (map[string]string, error) {
	data := map[string]string{
		hdfsv1alpha1.CoreSiteFileName:     b.makeCoreSiteData(),
		hdfsv1alpha1.HdfsSiteFileName:     b.makeHdfsSiteData(),
		hdfsv1alpha1.HadoopPolicyFileName: common.MakeHadoopPolicyData(),
		hdfsv1alpha1.SecurityFileName:     common.MakeSecurityPropertiesData(),
		hdfsv1alpha1.SslClientFileName:    common.MakeSslClientData(b.instance.Spec.ClusterConfig),
		hdfsv1alpha1.SslServerFileName:    common.MakeSslServerData(b.instance.Spec.ClusterConfig),
		// log4j for journalnode
		common.CreateComponentLog4jPropertiesName(constant.JournalNodeComponent): common.MakeLog4jPropertiesData(constant.JournalNodeComponent),
	}

	return data, nil
}

// GetConfigOverrides returns journalnode-specific configuration overrides
func (b *JournalnodeConfigMapBuilder) GetConfigOverrides() map[string]map[string]string {
	if b.override != nil {
		return b.override.ConfigOverrides
	}
	return nil
}

// makeCoreSiteData generates core-site.xml data for journalnode
func (b *JournalnodeConfigMapBuilder) makeCoreSiteData() string {
	generator := &common.CoreSiteXmlGenerator{InstanceName: b.instance.GetName()}
	return generator.EnableKerberos(b.instance.Spec.ClusterConfig, b.instance.Namespace).HaZookeeperQuorum().Generate()
}

// makeHdfsSiteData generates hdfs-site.xml data for journalnode
func (b *JournalnodeConfigMapBuilder) makeHdfsSiteData() string {
	clusterSpec := b.instance.Spec.ClusterConfig
	// Create ClusterComponentsInfo for the updated generator
	generator := common.NewNameNodeHdfsSiteXmlGenerator(
		b.instance.GetName(), b.groupName, b.getNameNodeReplicas(), b.instance.Namespace,
		b.instance.Spec.ClusterConfig, clusterSpec.ClusterDomain, clusterSpec.DfsReplication, b.clusterComponentInfo)
	return generator.EnablerKerberos(clusterSpec).EnableHttps().Generate()
}

// getNameNodeReplicas gets the number of NameNode replicas
func (b *JournalnodeConfigMapBuilder) getNameNodeReplicas() int32 {
	return b.clusterComponentInfo.NameNode[b.groupName].Replicas
}

package data

import (
	"context"
	"fmt"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/constants"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// DataNodeConfigMapBuilder builds ConfigMap for DataNode
type DataNodeConfigMapBuilder struct {
	*common.ConfigMapBuilder
	instance             *hdfsv1alpha1.HdfsCluster
	roleGroupInfo        *reconciler.RoleGroupInfo
	configSpec           *hdfsv1alpha1.ConfigSpec
	clusterComponentInfo *common.ClusterComponentsInfo
}

// ConfigMapComponentBuilder interface for DataNode ConfigMap
var _ common.ConfigMapComponentBuilder = &DataNodeConfigMapBuilder{}

// NewDataNodeConfigMapBuilder creates a new DataNode ConfigMap builder
func NewDataNodeConfigMapBuilder(
	ctx context.Context,
	client *client.Client,
	instance *hdfsv1alpha1.HdfsCluster,
	roleGroupInfo *reconciler.RoleGroupInfo,
	overrides *commonsv1alpha1.OverridesSpec,
	config *hdfsv1alpha1.ConfigSpec,
	clusterComponentInfo *common.ClusterComponentsInfo,
	vectorConfigName string,
) builder.ConfigBuilder {
	dnBuilder := &DataNodeConfigMapBuilder{
		instance:             instance,
		roleGroupInfo:        roleGroupInfo,
		configSpec:           config,
		clusterComponentInfo: clusterComponentInfo,
	}
	dnBuilder.ConfigMapBuilder = common.NewConfigMapBuilder(
		ctx,
		client,
		constant.DataNode,
		roleGroupInfo,
		overrides,
		config,
		instance,
		dnBuilder, // Pass itself as the component
	)
	return dnBuilder
}

// BuildConfig builds the DataNode configuration
func (b *DataNodeConfigMapBuilder) BuildConfig() (map[string]string, error) {
	// Create configuration map with basic HDFS settings
	data := map[string]string{
		hdfsv1alpha1.CoreSiteFileName:     b.makeCoreSiteData(),
		hdfsv1alpha1.HdfsSiteFileName:     b.makeHdfsSiteData(),
		hdfsv1alpha1.HadoopPolicyFileName: common.MakeHadoopPolicyData(),
		hdfsv1alpha1.SecurityFileName:     common.MakeSecurityPropertiesData(),
		hdfsv1alpha1.SslClientFileName:    common.MakeSslClientData(b.instance.Spec.ClusterConfig),
		hdfsv1alpha1.SslServerFileName:    common.MakeSslServerData(b.instance.Spec.ClusterConfig),
		// log4j
		common.CreateComponentLog4jPropertiesName(constant.DataNodeComponent):         common.MakeLog4jPropertiesData(constant.DataNodeComponent),
		common.CreateComponentLog4jPropertiesName(constant.WaitForNameNodesComponent): common.MakeLog4jPropertiesData(constant.WaitForNameNodesComponent),
	}
	return data, nil
}

// GetConfigOverrides returns any configuration overrides specific to DataNode
func (b *DataNodeConfigMapBuilder) GetConfigOverrides() map[string]map[string]string {
	return map[string]map[string]string{}
}

// make core-site.xml data
func (b *DataNodeConfigMapBuilder) makeCoreSiteData() string {
	generator := &common.CoreSiteXmlGenerator{InstanceName: b.instance.GetName()}
	return generator.EnableKerberos(b.instance.Spec.ClusterConfig, b.instance.Namespace).HaZookeeperQuorum().Generate()
}

// make hdfs-site.xml data
func (b *DataNodeConfigMapBuilder) makeHdfsSiteData() string {
	clusterSpec := b.instance.Spec.ClusterConfig
	generator := common.NewDataNodeHdfsSiteXmlGenerator(
		b.instance,
		b.roleGroupInfo.GetGroupName(),
		b.dataNodeConfig(),
		b.clusterComponentInfo,
	)
	return generator.EnablerKerberos(clusterSpec).EnableHttps().Generate()
}

func (c *DataNodeConfigMapBuilder) dataNodeConfig() map[string]string {
	dataDir := fmt.Sprintf("[DISK]%s/%s/%s", constants.KubedoopDataDir, hdfsv1alpha1.DataVolumeMountName, string(constant.DataNode))
	return map[string]string{
		"dfs.datanode.data.dir": dataDir,
	}
}

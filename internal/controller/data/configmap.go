package data

import (
	"context"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/common"
	"github.com/zncdatadev/hdfs-operator/internal/constant"
	commonsv1alpha1 "github.com/zncdatadev/operator-go/pkg/apis/commons/v1alpha1"
	"github.com/zncdatadev/operator-go/pkg/builder"
	"github.com/zncdatadev/operator-go/pkg/client"
	"github.com/zncdatadev/operator-go/pkg/reconciler"
)

// DataNodeConfigMapBuilder builds ConfigMap for DataNode
type DataNodeConfigMapBuilder struct {
	*common.ConfigMapBuilder
	instance             *hdfsv1alpha1.HdfsCluster
	roleGroupInfo        *reconciler.RoleGroupInfo
	roleGroupConfig      *hdfsv1alpha1.ConfigSpec
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
		roleGroupConfig:      config,
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
	config := map[string]string{
		"core-site.xml": `<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://namenode:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/stackable/data/tmp</value>
  </property>
</configuration>`,
		"hdfs-site.xml": `<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>3</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/stackable/data/datanode</value>
  </property>
  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:9866</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:9864</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>0.0.0.0:9867</value>
  </property>
</configuration>`,
	}

	return config, nil
}

// GetConfigOverrides returns any configuration overrides specific to DataNode
func (b *DataNodeConfigMapBuilder) GetConfigOverrides() map[string]map[string]string {
	return map[string]map[string]string{}
}

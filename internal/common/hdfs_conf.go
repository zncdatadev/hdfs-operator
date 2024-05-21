package common

import (
	"fmt"
	"strconv"
	"strings"

	hdfsv1alpha1 "github.com/zncdatadev/hdfs-operator/api/v1alpha1"
	"github.com/zncdatadev/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
)

const coreSiteTemplate = `<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://%s/</value>
  </property>
  <property>
    <name>ha.zookeeper.quorum</name>
    <value>${env.ZOOKEEPER}</value>
  </property>
</configuration> 
`

type CoreSiteXmlGenerator struct {
	InstanceName string
}

func (c *CoreSiteXmlGenerator) Generate() string {
	return fmt.Sprintf(coreSiteTemplate, c.InstanceName)
}

type NameNodeHdfsSiteXmlGenerator struct {
	NameNodeReplicas int32
	InstanceName     string
	GroupName        string
	NameSpace        string
	ClusterDomain    string
	hdfsReplication  int32
}

// NewNameNodeHdfsSiteXmlGenerator new a NameNodeHdfsSiteXmlGenerator
func NewNameNodeHdfsSiteXmlGenerator(
	instanceName string,
	groupName string,
	nameNodeReplicas int32,
	nameSpace string,
	clusterDomain string,
	hdfsReplication int32) *NameNodeHdfsSiteXmlGenerator {
	return &NameNodeHdfsSiteXmlGenerator{
		NameNodeReplicas: nameNodeReplicas,
		InstanceName:     instanceName,
		GroupName:        groupName,
		NameSpace:        nameSpace,
		ClusterDomain:    clusterDomain,
		hdfsReplication:  hdfsReplication,
	}
}

// make hdfs-site.xml data

func (c *NameNodeHdfsSiteXmlGenerator) Generate() string {
	var properties []util.XmlNameValuePair
	properties = append(properties, c.makeServiceId()...)
	properties = append(properties, c.makeHdfsReplication())
	properties = append(properties, c.makeNameNodeHosts())
	properties = append(properties, c.makeNameNodeHttp()...)
	properties = append(properties, c.makeNameNodeRpc()...)
	properties = append(properties, c.makeNameNodeNameDir()...)
	properties = append(properties, c.makeJournalNodeDataDir())
	return util.Append(hdfsSiteTemplate, properties)
}

func (c *NameNodeHdfsSiteXmlGenerator) makeServiceId() []util.XmlNameValuePair {
	return []util.XmlNameValuePair{
		{
			Name:  "dfs.nameservices",
			Value: c.InstanceName,
		},
		//<property>
		//<name>dfs.client.failover.proxy.provider.simple-hdfs</name>
		//<value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
		//</property>
		{
			Name:  "dfs.client.failover.proxy.provider." + c.InstanceName,
			Value: "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
		},
	}
}

// make hdfs replication number
func (c *NameNodeHdfsSiteXmlGenerator) makeHdfsReplication() util.XmlNameValuePair {
	return util.XmlNameValuePair{
		Name:  "dfs.replication",
		Value: strconv.Itoa(int(c.hdfsReplication)),
	}
}

// make journal node dir data
// if journal node is multiple, just add more data, separated by ";"
//
//	<property>
//		<name>dfs.namenode.shared.edits.dir</name>
//		<value>qjournal://node1.example.com:8485;node2.example.com:8485;node3.example.com:8485/mycluster</value>
//	</property>
func (c *NameNodeHdfsSiteXmlGenerator) makeJournalNodeDataDir() util.XmlNameValuePair {
	journalStatefulSetName := CreateJournalNodeStatefulSetName(c.InstanceName, c.GroupName)
	JournalSvcName := CreateJournalNodeServiceName(c.InstanceName, c.GroupName)
	journalUrls := CreateNetworksByReplicates(c.getJournalNodeReplicates(), journalStatefulSetName, JournalSvcName, c.NameSpace, c.ClusterDomain, 8485)
	journalConnection := CreateJournalUrl(journalUrls, c.InstanceName)
	return util.XmlNameValuePair{
		Name:  "dfs.namenode.shared.edits.dir",
		Value: journalConnection,
	}
}

// get journal node replicates
func (c *NameNodeHdfsSiteXmlGenerator) getJournalNodeReplicates() int32 {
	cfg := GetMergedRoleGroupCfg(JournalNode, c.InstanceName, c.GroupName)
	journalCfg := cfg.(*hdfsv1alpha1.JournalNodeRoleGroupSpec)
	return journalCfg.Replicas
}

// make name node hosts data
// if multiple name nodes, just add more data, separated by ","
// like below:
//
//	<property>
//		<name>dfs.ha.namenodes.simple-hdfs</name>
//		<value>simple-hdfs-namenode-default-0,simple-hdfs-namenode-default-1,simple-hdfs-namenode-default-2</value>
//	</property>
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeHosts() util.XmlNameValuePair {
	nameNodeStatefulSetName := CreateNameNodeStatefulSetName(c.InstanceName, c.GroupName)
	pods := CreatePodNamesByReplicas(c.NameNodeReplicas, nameNodeStatefulSetName)
	podNames := strings.Join(pods, ",")
	return util.XmlNameValuePair{
		Name:  "dfs.ha.namenodes." + c.InstanceName,
		Value: podNames,
	}
}

// make name node http address
// if multiple name nodes, should config multiple http address
// like below:
//
//	<property>
//		<name>dfs.namenode.http-address.simple-hdfs.simple-hdfs-namenode-default-0</name>
//		<value>simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:9870</value>
//	</property>
//	<property>
//		<name>dfs.namenode.http-address.simple-hdfs.simple-hdfs-namenode-default-1</name>
//		<value>simple-hdfs-namenode-default-1.simple-hdfs-namenode-default.default.svc.cluster.local:9870</value>
//	</property>
//	<property>
//		<name>dfs.namenode.http-address.simple-hdfs.simple-hdfs-namenode-default-2</name>
//		<value>simple-hdfs-namenode-default-2.simple-hdfs-namenode-default.default.svc.cluster.local:9870</value>
//	</property>
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeHttp() []util.XmlNameValuePair {
	statefulSetName := CreateNameNodeStatefulSetName(c.InstanceName, c.GroupName)
	svc := CreateNameNodeServiceName(c.InstanceName, c.GroupName)
	dnsDomain := CreateDnsDomain(svc, c.NameSpace, c.ClusterDomain, hdfsv1alpha1.NameNodeHttpPort)
	keyTemplate := fmt.Sprintf("dfs.namenode.http-address.%s.%s-%%d", c.InstanceName, statefulSetName)
	valueTemplate := fmt.Sprintf("%s-%%d.%s", statefulSetName, dnsDomain)
	return CreateXmlContentByReplicas(c.NameNodeReplicas, keyTemplate, valueTemplate)
}

// make name node rpc address
// if multiple name nodes, should config multiple rpc address
// like below:
//
//	<property>
//		<name>dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-0</name>
//		<value>simple-hdfs-namenode-default-0.simple-hdfs-namenode-default.default.svc.cluster.local:9868</value>
//	</property>
//	<property>
//		<name>dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-1</name>
//		<value>simple-hdfs-namenode-default-1.simple-hdfs-namenode-default.default.svc.cluster.local:9868</value>
//	</property>
//	<property>
//		<name>dfs.namenode.rpc-address.simple-hdfs.simple-hdfs-namenode-default-2</name>
//		<value>simple-hdfs-namenode-default-2.simple-hdfs-namenode-default.default.svc.cluster.local:9868</value>
//	</property>
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeRpc() []util.XmlNameValuePair {
	statefulSetName := CreateNameNodeStatefulSetName(c.InstanceName, c.GroupName)
	svc := CreateNameNodeServiceName(c.InstanceName, c.GroupName)
	dnsDomain := CreateDnsDomain(svc, c.NameSpace, c.ClusterDomain, hdfsv1alpha1.NameNodeRpcPort)
	keyTemplate := fmt.Sprintf("dfs.namenode.rpc-address.%s.%s-%%d", c.InstanceName, statefulSetName)
	valueTemplate := fmt.Sprintf("%s-%%d.%s", statefulSetName, dnsDomain)
	return CreateXmlContentByReplicas(c.NameNodeReplicas, keyTemplate, valueTemplate)
}

// make name node name dir
// if multiple name nodes, should config multiple name dir
// like below:
// <!-- name node name dir -->
//
//	<property>
//		<name>dfs.namenode.name.dir.simple-hdfs.simple-hdfs-namenode-default-0</name>
//		<value>/zncdata/data/namenode</value>
//	</property>
//	<property>
//		<name>dfs.namenode.name.dir.simple-hdfs.simple-hdfs-namenode-default-1</name>
//		<value>/zncdata/data/namenode</value>
//	</property>
//	<property>
//		<name>dfs.namenode.name.dir.simple-hdfs.simple-hdfs-namenode-default-2</name>
//		<value>/zncdata/data/namenode</value>
//	</property>
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeNameDir() []util.XmlNameValuePair {
	statefulSetName := CreateNameNodeStatefulSetName(c.InstanceName, c.GroupName)
	keyTemplate := fmt.Sprintf("dfs.namenode.name.dir.%s.%s-%%d", c.InstanceName, statefulSetName)
	valueTemplate := "/stackable/data/namenode"
	return CreateXmlContentByReplicas(c.NameNodeReplicas, keyTemplate, valueTemplate)
}

const hdfsSiteTemplate = `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.datanode.registered.hostname</name>
    <value>${env.POD_ADDRESS}</value>
  </property>
  <property>
    <name>dfs.datanode.registered.http.port</name>
    <value>${env.HTTP_PORT}</value>
  </property>
  <property>
    <name>dfs.datanode.registered.ipc.port</name>
    <value>${env.IPC_PORT}</value>
  </property>
  <property>
    <name>dfs.datanode.registered.port</name>
    <value>${env.DATA_PORT}</value>
  </property>
  <property>
    <name>dfs.ha.automatic-failover.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.ha.fencing.methods</name>
    <value>shell(/bin/true)</value>
  </property>
  <property>
    <name>dfs.ha.namenode.id</name>
    <value>${env.POD_NAME}</value>
  </property>
  <property>
    <name>dfs.journalnode.edits.dir</name>
    <value>/stackable/data/journalnode</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.unsafe.allow-address-override</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/stackable/data/namenode</value>
  </property>
</configuration>
`

// MakeHadoopPolicyData make hadoop-policy.xml data
func MakeHadoopPolicyData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// MakeSecurityPropertiesData make security.properties data
func MakeSecurityPropertiesData() string {
	return `networkaddress.cache.negative.ttl=0
networkaddress.cache.ttl=30`
}

// MakeSslClientData make ssl-client.xml data
func MakeSslClientData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// MakeSslServerData make ssl-server.xml data
func MakeSslServerData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// make log4j.properties data
const log4jProperties = `log4j.rootLogger=INFO, CONSOLE, FILE

log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=DEBUG
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n

log4j.appender.FILE=org.apache.log4j.RollingFileAppender
log4j.appender.FILE.Threshold=INFO
log4j.appender.FILE.MaxFileSize=5MB
log4j.appender.FILE.MaxBackupIndex=1
log4j.appender.FILE.layout=org.apache.log4j.PatternLayout
log4j.appender.FILE.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n
`
const fileLocationTemplate = `log4j.appender.FILE.File=/stackable/log/%s/%s.log`

func MakeLog4jPropertiesData(containerComponent ContainerComponent) string {
	fileLocation := fmt.Sprintf(fileLocationTemplate, containerComponent, containerComponent)
	return log4jProperties + "\n" + fileLocation
}

func CreateComponentLog4jPropertiesName(component ContainerComponent) string {
	return fmt.Sprintf("%s.log4j.properties", string(component))
}

// OverrideConfigurations override configurations
// override the content of the configMap
func OverrideConfigurations(cm *corev1.ConfigMap, overrides *hdfsv1alpha1.ConfigOverridesSpec) {
	if overrides == nil {
		return
	}
	// core-site.xml
	if override := overrides.CoreSite; override != nil {
		origin := cm.Data[hdfsv1alpha1.CoreSiteFileName]
		cm.Data[hdfsv1alpha1.CoreSiteFileName] = util.AppendXmlContent(origin, override)
	}
	// hdfs-site.xml
	if override := overrides.HdfsSite; override != nil {
		origin := cm.Data[hdfsv1alpha1.HdfsSiteFileName]
		cm.Data[hdfsv1alpha1.HdfsSiteFileName] = util.AppendXmlContent(origin, override)
	}
	// hadoop-policy.xml
	if override := overrides.HadoopPolicy; override != nil {
		origin := cm.Data[hdfsv1alpha1.HadoopPolicyFileName]
		cm.Data[hdfsv1alpha1.HadoopPolicyFileName] = util.AppendXmlContent(origin, override)
	}
	// security.properties
	if override := overrides.Security; override != nil {
		origin := cm.Data[hdfsv1alpha1.SecurityFileName]
		overrideContent := util.MakePropertiesFileContent(override)
		cm.Data[hdfsv1alpha1.SecurityFileName] = util.OverrideConfigFileContent(origin, overrideContent)
	}
	// ssl-client.xml
	if override := overrides.SslClient; override != nil {
		origin := cm.Data[hdfsv1alpha1.SslClientFileName]
		cm.Data[hdfsv1alpha1.SslClientFileName] = util.AppendXmlContent(origin, override)
	}
	// ssl-server.xml
	if override := overrides.SslServer; override != nil {
		origin := cm.Data[hdfsv1alpha1.SslServerFileName]
		cm.Data[hdfsv1alpha1.SslServerFileName] = util.AppendXmlContent(origin, override)
	}
}

type DataNodeHdfsSiteXmlGenerator struct {
	NameNodeHdfsSiteXmlGenerator
	DataNodeConfig map[string]string
}

// NewDataNodeHdfsSiteXmlGenerator new a DataNodeHdfsSiteXmlGenerator
func NewDataNodeHdfsSiteXmlGenerator(
	instanceName string,
	groupName string,
	nameNodeReplicas int32,
	nameSpace string,
	clusterDomain string,
	hdfsReplication int32,
	dataNodeConfig map[string]string) *DataNodeHdfsSiteXmlGenerator {
	return &DataNodeHdfsSiteXmlGenerator{
		NameNodeHdfsSiteXmlGenerator: *NewNameNodeHdfsSiteXmlGenerator(
			instanceName,
			groupName,
			nameNodeReplicas,
			nameSpace,
			clusterDomain,
			hdfsReplication),
		DataNodeConfig: dataNodeConfig,
	}
}

// Generate make hdfs-site.xml data
func (c *DataNodeHdfsSiteXmlGenerator) Generate() string {
	nameNodeSiteXml := c.NameNodeHdfsSiteXmlGenerator.Generate()
	return util.AppendXmlContent(nameNodeSiteXml, c.DataNodeConfig)
}

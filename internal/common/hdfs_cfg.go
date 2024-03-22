package common

import (
	"fmt"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"strings"
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
	NameNodeReplicas    int32
	JournalNodeReplicas int32
	InstanceName        string
	GroupName           string
	NameSpace           string
	ClusterDomain       string
	hdfsReplication     int32
}

// make hdfs-site.xml data

func (c *NameNodeHdfsSiteXmlGenerator) Generate() string {
	return fmt.Sprintf(
		hdfsSiteTemplate,
		c.makeServiceId(),
		c.makeHdfsReplication(),
		c.makeNameNodeHosts(),
		c.makeNameNodeHttp(),
		c.makeNameNodeRpc(),
		c.makeNameNodeNameDir(),
	)
}

// make service id
const serviceIdTemplate = `
  <property>
    <name>dfs.nameservices</name>
    <value>%s</value>
  </property>
`

func (c *NameNodeHdfsSiteXmlGenerator) makeServiceId() string {
	return fmt.Sprintf(serviceIdTemplate, c.InstanceName)
}

// make hdfs replication number
const hdfsReplicationTemplate = `
  <property>
	<name>dfs.replication</name>
	<value>%d</value>
  </property>
`

func (c *NameNodeHdfsSiteXmlGenerator) makeHdfsReplication() string {
	return fmt.Sprintf(hdfsReplicationTemplate, c.hdfsReplication)
}

// make journal node dir data

const JournalNodeTemplate = `
  <property>
    <name>dfs.namenode.shared.edits.dir</name>
    <value>%s</value>
  </property>
`

// if journal node is multiple, just add more data, separated by ";"
//
//	<property>
//		<name>dfs.namenode.shared.edits.dir</name>
//		<value>qjournal://node1.example.com:8485;node2.example.com:8485;node3.example.com:8485/mycluster</value>
//	</property>
func (c *NameNodeHdfsSiteXmlGenerator) makeJournalNodeDataDir() string {
	journalStatefulSetName := CreateJournalNodeStatefulSetName(c.InstanceName, c.GroupName)
	JournalSvcName := CreateJournalNodeServiceName(c.InstanceName, c.GroupName)
	journalUrls := CreateNetworksByReplicates(c.JournalNodeReplicas, journalStatefulSetName, JournalSvcName, c.NameSpace, c.ClusterDomain, 8485)
	journalConnection := CreateJournalUrl(journalUrls, c.InstanceName)
	return fmt.Sprintf(JournalNodeTemplate, journalConnection)
}

// make name nodes
const nameNodeHostsTemplate = `
  <property>
    <name>dfs.ha.namenodes.simple-hdfs</name>
    <value>%s</value>
  </property>
`

// make name node hosts data
// if multiple name nodes, just add more data, separated by ","
// like below:
//
//	<property>
//		<name>dfs.ha.namenodes.simple-hdfs</name>
//		<value>simple-hdfs-namenode-default-0,simple-hdfs-namenode-default-1,simple-hdfs-namenode-default-2</value>
//	</property>
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeHosts() string {
	nameNodeStatefulSetName := CreateNameNodeStatefulSetName(c.InstanceName, c.GroupName)
	pods := CreatePodNamesByReplicas(c.NameNodeReplicas, nameNodeStatefulSetName)
	podNames := strings.Join(pods, ",")
	return fmt.Sprintf(nameNodeHostsTemplate, podNames)
}

// make name node http address

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
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeHttp() string {
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
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeRpc() string {
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
func (c *NameNodeHdfsSiteXmlGenerator) makeNameNodeNameDir() string {
	statefulSetName := CreateNameNodeStatefulSetName(c.InstanceName, c.GroupName)
	keyTemplate := fmt.Sprintf("dfs.namenode.name.dir.%s.%s-%%d", c.InstanceName, statefulSetName)
	valueTemplate := fmt.Sprintf("/zncdata/data/namenode")
	return CreateXmlContentByReplicas(c.NameNodeReplicas, keyTemplate, valueTemplate)
}

const hdfsSiteTemplate = `<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.client.failover.proxy.provider.simple-hdfs</name>
    <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
  </property>
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
    <value>/zncdata/data/journalnode</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.unsafe.allow-address-override</name>
    <value>true</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/zncdata/data/namenode</value>
  </property>

  <!-- name service id -->
  %s

  <!-- hdfs replication number -->
  %s

  <!-- name node hosts -->
  %s

  <!-- name node http address -->
  %s

  <!-- name node rpc address -->
  %s	

  <!-- name node name dir -->
  %s

</configuration>
`

package name

import (
	"context"
	"fmt"
	hdfsv1alpha1 "github.com/zncdata-labs/hdfs-operator/api/v1alpha1"
	"github.com/zncdata-labs/hdfs-operator/internal/common"
	"github.com/zncdata-labs/hdfs-operator/internal/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ConfigMapReconciler struct {
	common.ConfigurationStyleReconciler[*hdfsv1alpha1.HdfsCluster, *hdfsv1alpha1.RoleGroupSpec]
}

// NewConfigMap new a ConfigMapReconciler
func NewConfigMap(
	scheme *runtime.Scheme,
	instance *hdfsv1alpha1.HdfsCluster,
	client client.Client,
	groupName string,
	labels map[string]string,
	mergedCfg *hdfsv1alpha1.RoleGroupSpec,
) *ConfigMapReconciler {
	return &ConfigMapReconciler{
		ConfigurationStyleReconciler: *common.NewConfigurationStyleReconciler(
			scheme,
			instance,
			client,
			groupName,
			labels,
			mergedCfg,
		),
	}
}
func (c *ConfigMapReconciler) ConfigurationOverride(resource client.Object) {
	cm := resource.(*corev1.ConfigMap)
	overrides := c.MergedCfg.ConfigOverrides
	// core-site.xml
	if override := overrides.CoreSite; override != nil {
		origin := cm.Data[hdfsv1alpha1.CoreSiteFileName]
		cm.Data[hdfsv1alpha1.CoreSiteFileName] = util.OverrideXmlFileContent(origin, override)
	}
	// hdfs-site.xml
	if override := overrides.HdfsSite; override != nil {
		origin := cm.Data[hdfsv1alpha1.HdfsSiteFileName]
		cm.Data[hdfsv1alpha1.HdfsSiteFileName] = util.OverrideXmlFileContent(origin, override)
	}
	// hadoop-policy.xml
	if override := overrides.HadoopPolicy; override != nil {
		origin := cm.Data[hdfsv1alpha1.HadoopPolicyFileName]
		cm.Data[hdfsv1alpha1.HadoopPolicyFileName] = util.OverrideXmlFileContent(origin, override)
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
		cm.Data[hdfsv1alpha1.SslClientFileName] = util.OverrideXmlFileContent(origin, override)
	}
	// ssl-server.xml
	if override := overrides.SslServer; override != nil {
		origin := cm.Data[hdfsv1alpha1.SslServerFileName]
		cm.Data[hdfsv1alpha1.SslServerFileName] = util.OverrideXmlFileContent(origin, override)
	}
	// only name node log4j,other component log4j not override, I think it is not necessary
	if override := overrides.Log4j; override != nil {
		origin := cm.Data[createComponentLog4jPropertiesName(NameNode)]
		overrideContent := util.MakePropertiesFileContent(override)
		cm.Data[createComponentLog4jPropertiesName(NameNode)] = util.OverrideConfigFileContent(origin, overrideContent)
	}
}

func (c *ConfigMapReconciler) Build(ctx context.Context) (client.Object, error) {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      createConfigName(c.Instance.GetName(), c.GroupName),
			Namespace: c.Instance.GetNamespace(),
			Labels:    c.MergedLabels,
		},
		Data: map[string]string{
			hdfsv1alpha1.CoreSiteFileName:     c.makeCoreSiteData(),
			hdfsv1alpha1.HdfsSiteFileName:     c.makeHdfsSiteData(),
			hdfsv1alpha1.HadoopPolicyFileName: c.makeHadoopPolicyData(),
			hdfsv1alpha1.SecurityFileName:     c.makeSecurityPropertiesData(),
			hdfsv1alpha1.SslClientFileName:    c.makeSslClientData(),
			hdfsv1alpha1.SslServerFileName:    c.makeSslServerData(),
			//log4j
			createComponentLog4jPropertiesName(NameNode):        c.makeLog4jPropertiesData(NameNode),
			createComponentLog4jPropertiesName(Zkfc):            c.makeLog4jPropertiesData(Zkfc),
			createComponentLog4jPropertiesName(FormatNameNode):  c.makeLog4jPropertiesData(FormatNameNode),
			createComponentLog4jPropertiesName(FormatZookeeper): c.makeLog4jPropertiesData(FormatZookeeper),
		},
	}, nil
}

// make core-site.xml data
func (c *ConfigMapReconciler) makeCoreSiteData() string {
	generator := &common.CoreSiteXmlGenerator{InstanceName: c.Instance.GetName()}
	return generator.Generate()
}

// make hdfs-site.xml data
func (c *ConfigMapReconciler) makeHdfsSiteData() string {
	generator := &common.NameNodeHdfsSiteXmlGenerator{
		InstanceName: c.Instance.GetName(),
		GroupName:    c.GroupName,
	}
	return generator.Generate()
}

// make hadoop-policy.xml data
func (c *ConfigMapReconciler) makeHadoopPolicyData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// make security.properties data
func (c *ConfigMapReconciler) makeSecurityPropertiesData() string {
	return `networkaddress.cache.negative.ttl=0
networkaddress.cache.ttl=30`
}

// make ssl-client.xml data
func (c *ConfigMapReconciler) makeSslClientData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// make ssl-server.xml data
func (c *ConfigMapReconciler) makeSslServerData() string {
	return `<?xml version="1.0"?>
<configuration>
</configuration>`
}

// make log4j.properties data
const log4jProperties = `log4j.rootLogger=INFO, CONSOLE, FILE

log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender
log4j.appender.CONSOLE.Threshold=INFO
log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout
log4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n

log4j.appender.FILE=org.apache.log4j.RollingFileAppender
log4j.appender.FILE.Threshold=INFO
log4j.appender.FILE.MaxFileSize=5MB
log4j.appender.FILE.MaxBackupIndex=1
log4j.appender.FILE.layout=org.apache.log4j.PatternLayout
log4j.appender.FILE.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n
`
const fileLocationTemplate = `log4j.appender.FILE.File=/zncdata/log/%s/%s.log`

func (c *ConfigMapReconciler) makeLog4jPropertiesData(containerComponent ContainerComponent) string {
	fileLocation := fmt.Sprintf(fileLocationTemplate, string(containerComponent), string(containerComponent))
	return log4jProperties + "\n" + fileLocation
}

func createComponentLog4jPropertiesName(component ContainerComponent) string {
	return fmt.Sprintf("%s.log4j.properties", string(component))
}

// ContainerComponent name node container component
// contains: zkfc, namenode, format-namenode, format-zookeeper
type ContainerComponent string

const (
	Zkfc            ContainerComponent = "zkfc"
	NameNode        ContainerComponent = "namenode"
	FormatNameNode  ContainerComponent = "format-namenodes"
	FormatZookeeper ContainerComponent = "format-zookeeper"
)
